"""
Job Market Signal Detector
Author: Andrew Gorin, PhD

A daily automation script to aggregate high-relevance job postings across 
Climate Risk, Data Science, and Environmental Consulting. 
Uses Boolean logic to filter for Python-heavy roles and remove low-relevance listings.
"""

import csv
from jobspy import scrape_jobs
import pandas as pd
from datetime import datetime

# Configuration: Search Verticals
# Grouping searches by industry to allow for targeted Boolean strings.
# The goal is to isolate roles that value the PhD/Physics background (e.g., stochastic modeling, geospatial).
searches = [
    # --- TIER 1: CORE TARGETS (High Domain Fit) ---
    {
        "name": "Climate_Risk_Modeling",
        # Filtering for "Python" is critical here to separate modeling roles from general insurance analysis.
        "query": "(\"Catastrophe Modeler\" OR \"Risk Analyst\" OR \"Climate Scientist\") AND (\"Wildfire\" OR \"Flood\") AND (\"Python\")",
        "location": "San Francisco Bay Area"
    },
    {
        "name": "Tech_Data_Science",
        # Targeting "Fraud" and "Decision Science" to leverage my anomaly detection/pipeline experience.
        "query": "(\"Data Scientist\" OR \"Applied Scientist\") AND (\"Fraud\" OR \"Risk\" OR \"Decision Science\") AND (\"Python\")",
        "location": "San Francisco, CA" 
    },
    {
        "name": "Environmental_Consulting",
        # Targeting specific AEC firms (Arcadis, AECOM) that value the EPA/NEPA regulatory background.
        "query": "(\"Climate Resilience\" OR \"Environmental Planner\" OR \"NEPA\") AND (\"Consulting\" OR \"Arcadis\" OR \"AECOM\" OR \"Jacobs\")",
        "location": "Oakland, CA"
    },

    # --- TIER 2: ADJACENT INDUSTRIES (High Technical Upside) ---
    {
        "name": "Quant_Energy_Trading",
        # Looking for "Stochastic" and "Time Series" to apply physical system modeling skills to commodities.
        "query": "(\"Quantitative Researcher\" OR \"Energy Trader\" OR \"Commodities Analyst\") AND (\"Python\") AND (\"Stochastic\" OR \"Time Series\" OR \"Power\")",
        "location": "San Francisco Bay Area"
    },
    {
        "name": "Carbon_MRV_ClimateTech",
        # Direct overlap with my remote sensing and glacial change research.
        "query": "(\"Remote Sensing\" OR \"Geospatial\" OR \"Data Scientist\") AND (\"Carbon\" OR \"Forestry\" OR \"MRV\" OR \"Nature-based Solutions\") AND (\"Python\")",
        "location": "Remote"
    },
    {
        "name": "Reinsurance_Analytics",
        # Focus on the analytical side of broking where communication of complex risk is key.
        "query": "(\"Reinsurance\" OR \"Catastrophe Analyst\" OR \"Broking\") AND (\"Analytics\" OR \"Risk\") AND (\"Guy Carpenter\" OR \"Aon\" OR \"Gallagher\")",
        "location": "San Francisco, CA"
    }
]

all_jobs = []
date_str = datetime.now().strftime('%Y-%m-%d')

print(f"--- Starting Market Scan: {date_str} ---")

# Execute Scraper
for search in searches:
    print(f"Querying vertical: {search['name']}...")
    
    try:
        # JobSpy aggregates LinkedIn, Indeed, and Glassdoor.
        # Keeping result count low (15) and window tight (24h) to use this as a daily morning check.
        jobs = scrape_jobs(
            site_name=["linkedin", "indeed", "glassdoor"],
            search_term=search['query'],
            location=search['location'],
            results_wanted=15,
            hours_old=24, 
            country_urlpatterns='USA'
        )
        
        if not jobs.empty:
            jobs['Category'] = search['name']
            all_jobs.append(jobs)
            print(f"   Signal found: {len(jobs)} new roles.")
        else:
            print("   No fresh data found.")
            
    except Exception as e:
        print(f"   Pipeline error in {search['name']}: {e}")

# Data Aggregation & Export
if all_jobs:
    master_df = pd.concat(all_jobs, ignore_index=True)
    
    # Selecting only high-value columns for quick review
    clean_df = master_df[['title', 'company', 'site', 'job_url', 'location', 'date_posted', 'Category']]
    
    filename = f"market_scan_{date_str}.csv"
    clean_df.to_csv(filename, index=False)
    
    print(f"\nPipeline complete. {len(clean_df)} jobs saved to {filename}")
else:
    print("\nScan complete. No new listings found matching criteria.")