"""
Job Market Signal Detector (v6 - Wide Net)
Author: Andrew Gorin, PhD

Updates:
- General_Tech_DS: Removed industry constraints to capture all Tech DS roles.
- Geospatial_Tech: Added dedicated bucket for GIS/Remote Sensing (General).
- Noise Filter: Added strict title filtering to manage the broader search volume.
"""

import csv
from jobspy import scrape_jobs
import pandas as pd
from datetime import datetime, timedelta

# Configuration: Locations
target_locations = ["San Francisco Bay Area, CA", "Remote"]

# Configuration: Search Verticals
searches = [
    # --- TIER 1: BROAD TECH & GEOSPATIAL ---
    {
        "name": "General_Tech_DS",
        # BROADER: Captures any Python+SQL Data Science role (Product, Core ML, Analytics)
        "query": "(\"Data Scientist\" OR \"Applied Scientist\" OR \"Machine Learning Engineer\") AND (\"Python\") AND (\"SQL\")",
    },
    {
        "name": "Geospatial_Tech",
        # NEW: Captures Spatial Data Science across all sectors (Logistics, AgriTech, Urban)
        "query": "(\"Geospatial\" OR \"Remote Sensing\" OR \"GIS\" OR \"Spatial Data\") AND (\"Python\")",
    },
    
    # --- TIER 2: YOUR DOMAIN EXPERTISE (Keep these, they are high signal) ---
    {
        "name": "Climate_Risk_Modeling",
        "query": "(\"Catastrophe Modeler\" OR \"Risk Analyst\" OR \"Climate Scientist\") AND (\"Wildfire\" OR \"Flood\") AND (\"Python\")",
    },
    {
        "name": "Environmental_Consulting",
        "query": "(\"Climate Resilience\" OR \"Environmental Planner\" OR \"NEPA\") AND (\"Consulting\" OR \"Arcadis\" OR \"AECOM\" OR \"Jacobs\")",
    },

    # --- TIER 3: NICHE HIGH UPSIDE ---
    {
        "name": "Carbon_MRV_ClimateTech",
        "query": "(\"Remote Sensing\" OR \"Geospatial\" OR \"Data Scientist\") AND (\"Carbon\" OR \"Forestry\" OR \"MRV\" OR \"Nature-based Solutions\") AND (\"Python\")",
    },
    {
        "name": "Quant_Energy_Trading",
        "query": "(\"Quantitative Researcher\" OR \"Energy Trader\") AND (\"Python\") AND (\"Stochastic\" OR \"Time Series\" OR \"Power\")",
    }
]

all_jobs = []
date_str = datetime.now().strftime('%Y-%m-%d')

print(f"--- Starting Wide-Net Market Scan: {date_str} ---")

# Execute Scraper
for search in searches:
    for loc in target_locations:
        print(f"Querying {search['name']} in {loc}...")
        
        google_query = f"{search['query']} jobs in {loc}"
        
        try:
            jobs = scrape_jobs(
                site_name=["linkedin", "indeed", "glassdoor", "zip_recruiter", "google"],
                search_term=search['query'],
                google_search_term=google_query,
                location=loc,
                results_wanted=15, 
                hours_old=168, # 7 Days
                country_urlpatterns='USA'
            )
            
            if not jobs.empty:
                jobs['Category'] = search['name']
                jobs['Search_Location'] = loc
                all_jobs.append(jobs)
                print(f"   Signal found: {len(jobs)} new roles.")
            else:
                print("   No fresh data found.")
                
        except Exception as e:
            print(f"   Pipeline error in {search['name']} ({loc}): {e}")

# Data Cleaning & Export
if all_jobs:
    master_df = pd.concat(all_jobs, ignore_index=True)

    # --- 1. STRICT DATE FILTER ---
    master_df = master_df.dropna(subset=['date_posted'])
    master_df['date_posted'] = pd.to_datetime(master_df['date_posted']).dt.date
    cutoff_date = (datetime.now() - timedelta(days=7)).date()
    master_df = master_df[master_df['date_posted'] >= cutoff_date]

    # --- 2. INTERNATIONAL FIREWALL ---
    forbidden_locs = ["India", "UK", "United Kingdom", "London", "Germany", "France", "Italy", "China", "Australia", "Canada", "Europe"]
    loc_pattern = '|'.join(forbidden_locs)
    master_df = master_df[~master_df['location'].str.contains(loc_pattern, case=False, na=False)]

    # --- 3. NOISE FILTER (Forbidden Titles) ---
    # Because we cast a wider net, we need to block non-DS roles that mention "Python"
    forbidden_titles = [
        "Sales", "Account Executive", "Recruiter", "Marketing Manager", 
        "Nurse", "Driver", "Technician", "Intern", "Unpaid", "Volunteer",
        "Senior Director", "VP of", "Head of" # Filtering out roles likely too senior for first industry jump
    ]
    title_pattern = '|'.join(forbidden_titles)
    master_df = master_df[~master_df['title'].str.contains(title_pattern, case=False, na=False)]

    # --- 4. THE "CALIFORNIA OR REMOTE" RULE ---
    def is_valid_location(row):
        loc = str(row['location']).lower()
        is_remote = row.get('is_remote')
        
        if "ca" in loc or "california" in loc or "san francisco" in loc or "oakland" in loc or "san jose" in loc:
            return True
        if is_remote is True:
            return True
        if "remote" in loc:
            return True
        return False

    master_df = master_df[master_df.apply(is_valid_location, axis=1)]

    # Select high-value columns
    desired_columns = [
        'title', 'company', 'site', 'job_url', 'location', 'is_remote',
        'min_amount', 'max_amount', 'interval', 
        'date_posted', 'Category'
    ]
    
    available_columns = [col for col in desired_columns if col in master_df.columns]
    clean_df = master_df[available_columns]

    # --- 5. SORT BY RECENCY ---
    clean_df = clean_df.sort_values(by='date_posted', ascending=False)
    
    filename = f"market_scan_WideNet_{date_str}.csv"
    clean_df.to_csv(filename, index=False)
    
    print(f"\nPipeline complete. {len(clean_df)} valid, sorted jobs saved to {filename}")
else:
    print("\nScan complete. No new listings found matching criteria.")