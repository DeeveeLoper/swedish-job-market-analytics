"""
FIXED: JobTech API Data Loader with CORRECT occupation field codes

Key fix: Updated occupation field codes based on API testing
"""

import dlt
import requests
import json
import time
from pathlib import Path
import os
import duckdb
from datetime import datetime
from typing import Set, Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ✅ CORRECTED occupation field codes (based on API testing)
OCCUPATION_FIELDS = {
    "apaJ_2ja_LuF": "Data_IT",                          # ✅ CORRECTED: Was apa1_2ja_LuF
    "bh3H_Y3h_5eD": "Chefer_och_verksamhetsledare",    # ✅ CORRECTED: Was bhFF_Y3h_SeD  
    "ScKy_FHB_7wT": "Hotell_restaurang_storhushall",   # ✅ CORRECTED: Was ScKy_FHG_7wT
    
    # TODO: Find correct codes for remaining fields via API discovery
    # For now, we'll start with these 3 confirmed working codes
    # and discover the rest programmatically
}

# Database configuration  
CURRENT_DIR = Path(__file__).parent
DATA_DIR = CURRENT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = str(DATA_DIR / "swedish_job_market_dev.duckdb")
SCHEMA_NAME = "staging"
TABLE_NAME = "job_ads"

def discover_all_occupation_fields():
    """
    Discover all available occupation field codes from the API
    """
    logger.info("🔍 Discovering all occupation field codes from API...")
    
    url = "https://jobsearch.api.jobtechdev.se/search"
    headers = {"accept": "application/json"}
    
    discovered_fields = {}
    seen_concept_ids = set()
    
    # Fetch multiple pages to find all occupation fields
    for offset in range(0, 2000, 100):  # Check first 2000 jobs
        params = {"limit": 100, "offset": offset}
        
        try:
            response = requests.get(url, headers=headers, params=params)
            data = response.json()
            hits = data.get("hits", [])
            
            if not hits:
                break
                
            for hit in hits:
                occ_field = hit.get("occupation_field", {})
                if occ_field:
                    concept_id = occ_field.get("concept_id")
                    label = occ_field.get("label")
                    
                    if concept_id and concept_id not in seen_concept_ids:
                        seen_concept_ids.add(concept_id)
                        # Convert label to safe python identifier
                        safe_label = label.replace("ä", "a").replace("ö", "o").replace("å", "a")
                        safe_label = safe_label.replace("/", "_").replace(", ", "_")
                        safe_label = safe_label.replace(" ", "_").replace("-", "_")
                        discovered_fields[concept_id] = safe_label
            
            # Small delay between requests
            time.sleep(0.1)
            
        except Exception as e:
            logger.warning(f"Error at offset {offset}: {e}")
            continue
    
    logger.info(f"🎯 Discovered {len(discovered_fields)} occupation fields!")
    
    # Log discovered fields
    for concept_id, label in sorted(discovered_fields.items()):
        logger.info(f"  {concept_id}: {label}")
    
    return discovered_fields

def init_database(db_path: str):
    """Initialize database"""
    try:
        con = duckdb.connect(db_path)
        con.execute("SET default_collation = 'C'")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
        con.close()
        logger.info(f"Database initialized: {db_path}")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

class JobTechAPIClient:
    """API client with rate limiting"""
    
    def __init__(self, base_url: str = "https://jobsearch.api.jobtechdev.se"):
        self.base_url = base_url
        self.search_url = f"{base_url}/search"
        self.session = requests.Session()
        self.session.headers.update({"accept": "application/json"})
        self.last_request_time = 0
        self.min_interval = 0.2
        
    def _rate_limit(self):
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_interval:
            time.sleep(self.min_interval - time_since_last)
        self.last_request_time = time.time()
    
    def get_job_ads(self, params: Dict) -> Dict:
        self._rate_limit()
        try:
            response = self.session.get(self.search_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"API request failed: {e}")
            raise

def get_existing_ids(db_path: str = DB_PATH) -> Set[str]:
    """Fetch existing job ad IDs"""
    try:
        con = duckdb.connect(db_path)
        tables_query = f"""
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = '{SCHEMA_NAME}' AND table_name = '{TABLE_NAME}'
        """
        existing_tables = con.execute(tables_query).fetchall()
        
        if not existing_tables:
            logger.info("Starting fresh - no existing data")
            con.close()
            return set()
            
        result = con.execute(f"SELECT DISTINCT id FROM {SCHEMA_NAME}.{TABLE_NAME}").fetchall()
        con.close()
        
        existing_ids = {str(row[0]) for row in result if row[0] is not None}
        logger.info(f"Found {len(existing_ids)} existing job ads")
        return existing_ids
        
    except Exception as e:
        logger.warning(f"Could not fetch existing IDs: {e}")
        return set()

@dlt.resource(write_disposition="append")
def jobsearch_resource(
    api_client: JobTechAPIClient,
    params: Dict,
    existing_ids: Set[str],
    occupation_field_name: str
):
    """DLT resource to fetch job ads"""
    limit = params.get("limit", 100)
    offset = 0
    new_ads_count = 0
    
    logger.info(f"Starting extraction for: {occupation_field_name}")
    
    while True:
        page_params = dict(params, offset=offset, limit=limit)
        
        try:
            data = api_client.get_job_ads(page_params)
        except Exception as e:
            logger.error(f"Failed at offset {offset}: {e}")
            break
            
        hits = data.get("hits", [])
        total_available = data.get("total", {}).get("value", 0)
        
        if offset == 0:
            logger.info(f"  📊 {total_available:,} total job ads available")
        
        if not hits:
            break
            
        # Process ads
        for ad in hits:
            ad_id = ad.get("id")
            if not ad_id or str(ad_id) in existing_ids:
                continue
                
            # Add metadata
            ad["_extracted_at"] = datetime.now().isoformat()
            ad["_occupation_field_code"] = params.get("occupation-field")
            ad["_occupation_field_name"] = occupation_field_name
            
            yield ad
            new_ads_count += 1
            
        # Progress logging
        if offset % 500 == 0 and offset > 0:
            logger.info(f"    Progress: {offset:,} processed, {new_ads_count} new ads")
            
        # Stop conditions
        if len(hits) < limit or offset >= 1900:
            break
            
        offset += limit
        
    logger.info(f"✅ {occupation_field_name}: {new_ads_count:,} new ads extracted")

def run_corrected_pipeline():
    """Run pipeline with corrected occupation codes"""
    
    logger.info("🚀 RUNNING CORRECTED PIPELINE")
    logger.info(f"Database: {DB_PATH}")
    
    # Option 1: Discover all occupation fields first
    logger.info("\n" + "="*60)
    logger.info("STEP 1: Discovering all occupation field codes")
    logger.info("="*60)
    
    try:
        all_occupation_fields = discover_all_occupation_fields()
        
        # Use discovered fields, or fall back to our known working ones
        if len(all_occupation_fields) > 10:
            occupation_fields_to_use = all_occupation_fields
            logger.info(f"✅ Using {len(occupation_fields_to_use)} discovered occupation fields")
        else:
            occupation_fields_to_use = OCCUPATION_FIELDS
            logger.info(f"⚠️ Using {len(occupation_fields_to_use)} fallback occupation fields")
            
    except Exception as e:
        logger.error(f"Discovery failed: {e}")
        occupation_fields_to_use = OCCUPATION_FIELDS
        logger.info(f"⚠️ Using {len(occupation_fields_to_use)} fallback occupation fields")
    
    # Initialize database
    init_database(DB_PATH)
    
    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="swedish_job_market",
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name=SCHEMA_NAME,
    )
    
    existing_ids = get_existing_ids()
    
    # Process occupation fields
    logger.info("\n" + "="*60)
    logger.info("STEP 2: Extracting job ads")
    logger.info("="*60)
    
    successful_extractions = 0
    total_new_ads = 0
    
    for i, (field_code, field_name) in enumerate(occupation_fields_to_use.items(), 1):
        logger.info(f"\n📋 Processing {i}/{len(occupation_fields_to_use)}: {field_name}")
        logger.info(f"   Code: {field_code}")
        
        params = {
            "limit": 100,
            "occupation-field": field_code,
        }
        
        try:
            load_info = pipeline.run(
                jobsearch_resource(
                    api_client=JobTechAPIClient(),
                    params=params,
                    existing_ids=existing_ids,
                    occupation_field_name=field_name
                ),
                table_name=TABLE_NAME
            )
            
            successful_extractions += 1
            time.sleep(1)  # Brief pause
            
        except Exception as e:
            logger.error(f"❌ Failed {field_name}: {e}")
            continue
    
    # Final verification
    logger.info("\n" + "="*80)
    logger.info("🏁 PIPELINE COMPLETED")
    logger.info("="*80)
    
    try:
        con = duckdb.connect(DB_PATH)
        total_count = con.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{TABLE_NAME}").fetchone()[0]
        
        field_stats = con.execute(f"""
        SELECT 
            _occupation_field_name,
            COUNT(*) as count
        FROM {SCHEMA_NAME}.{TABLE_NAME} 
        GROUP BY _occupation_field_name 
        ORDER BY count DESC
        """).fetchall()
        
        con.close()
        
        logger.info(f"📊 FINAL RESULTS:")
        logger.info(f"   Total job ads: {total_count:,}")
        logger.info(f"   Successful extractions: {successful_extractions}/{len(occupation_fields_to_use)}")
        logger.info(f"   By occupation field:")
        
        for field_name, count in field_stats:
            logger.info(f"     {field_name}: {count:,} ads")
            
    except Exception as e:
        logger.error(f"Could not verify final results: {e}")

def main():
    """Main function"""
    working_directory = Path(__file__).parent
    os.chdir(working_directory)
    
    logger.info("🎯 Starting CORRECTED Swedish Job Market Data Extraction")
    
    try:
        run_corrected_pipeline()
        logger.info("🎉 SUCCESS: Data extraction completed!")
        logger.info("📈 Ready for dbt transformation")
        
    except Exception as e:
        logger.error(f"💥 PIPELINE FAILED: {e}")
        raise

if __name__ == "__main__":
    main()