"""
Seed Token Expander v2.0
========================
Discovers new company names from multiple tiered sources to expand
the seed token database for job board discovery.

TIER 1 - High Hit Rate (Tech/Startups):
- Y Combinator Company Directory
- GitHub Organizations API
- ProductHunt
- GitHub Awesome Lists
- Crunchbase (free tier)

TIER 2 - Medium Hit Rate (Established Businesses):
- SEC EDGAR Public Companies
- USASpending.gov Federal Contractors
- SAM.gov Federal Vendors
- Inc 5000 / Fortune Lists
- Glassdoor Company Lists

Features:
- Priority-based seed processing
- Source hit rate tracking
- Automatic low-performer disabling
- Trickle processing to stay efficient
"""

import asyncio
import aiohttp
import json
import re
import logging
import os
from typing import List, Set, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from xml.etree import ElementTree as ET

from database import get_db, Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class SourceConfig:
    """Configuration for a company source."""
    name: str
    tier: int
    priority: int
    enabled: bool = True


# Source configurations
SOURCES = {
    # Tier 1 - High hit rate (tech/startups)
    'yc': SourceConfig('Y Combinator', tier=1, priority=90),
    'github_orgs': SourceConfig('GitHub Organizations', tier=1, priority=85),
    'producthunt': SourceConfig('ProductHunt', tier=1, priority=80),
    'github_awesome': SourceConfig('GitHub Awesome Lists', tier=1, priority=75),
    'crunchbase': SourceConfig('Crunchbase', tier=1, priority=70),
    
    # Tier 2 - Medium hit rate (established businesses)
    'sec_edgar': SourceConfig('SEC EDGAR', tier=2, priority=55),
    'usaspending': SourceConfig('USASpending.gov', tier=2, priority=50),
    'sam_gov': SourceConfig('SAM.gov', tier=2, priority=45),
    'inc5000': SourceConfig('Inc 5000', tier=2, priority=50),
    'fortune500': SourceConfig('Fortune 500', tier=2, priority=55),
    'glassdoor': SourceConfig('Glassdoor', tier=2, priority=45),
}


class SeedExpander:
    """Expands seed tokens from multiple tiered sources."""
    
    def __init__(self, db: Database = None):
        self.db = db or get_db()
        self.session: Optional[aiohttp.ClientSession] = None
        self.discovered_companies: Set[str] = set()
        self.results: Dict[str, List[str]] = {}
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=60)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    'User-Agent': 'Mozilla/5.0 (compatible; JobIntelBot/2.0; +https://github.com/job-intel)',
                    'Accept': 'application/json, text/html, */*'
                }
            )
        return self.session
    
    async def close(self):
        """Close the session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    # ========================================================================
    # TIER 1 SOURCES - High Hit Rate (Tech/Startups)
    # ========================================================================
    
    async def fetch_yc_companies(self) -> List[str]:
        """Fetch companies from Y Combinator's public Algolia API."""
        source = 'yc'
        logger.info(f"🚀 Fetching from Y Combinator...")
        companies = []
        
        try:
            session = await self.get_session()
            
            url = "https://45bwzj1sgc-dsn.algolia.net/1/indexes/*/queries"
            headers = {
                'x-algolia-api-key': 'NDYzYmNmMTRjYzU3YTY1MTNlMzgwMzY5NGIwNmNkMTNkNjE2NjE1NTQ5OGY4NjkwMmZhNzRkZjVjYTViZDY1N3Jlc3RyaWN0SW5kaWNlcz1ZQ0NvbXBhbnlfcHJvZHVjdGlvbiZ0YWdGaWx0ZXJzPSU1QiUyMnljZGNfcHVibGljJTIyJTVE',
                'x-algolia-application-id': '45BWZJ1SGC',
                'Content-Type': 'application/json'
            }
            
            for page in range(0, 50):  # Up to 5000 companies
                payload = {
                    "requests": [{
                        "indexName": "YCCompany_production",
                        "params": f"hitsPerPage=100&page={page}"
                    }]
                }
                async with session.post(url, json=payload, headers=headers) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()
                    results = data.get('results', [{}])[0]
                    hits = results.get('hits', [])
                    if not hits:
                        break
                    
                    for hit in hits:
                        name = hit.get('name', '')
                        if name and self._is_valid_company_name(name):
                            companies.append(self._clean_company_name(name))
                    
                    await asyncio.sleep(0.1)
            
            logger.info(f" Found {len(companies)} YC companies")
            return companies
            
        except Exception as e:
            logger.error(f" Error fetching YC companies: {e}")
            return []
            
    async def fetch_github_organizations(self) -> List[str]:
        """Fetch companies from a curated list of GitHub organizations (simulated)."""
        source = 'github_orgs'
        logger.info(f"🚀 Fetching from GitHub Organizations (simulated)...")
        # In a real app, this would query an API or scrape a list.
        companies = [
            "stripe", "hashicorp", "grafana", "prisma", "vercel", "netlify", 
            "postmanlabs", "datadog", "sentry", "cockroachdb", "gitbook",
            "algolia", "figma", "notion", "airtable", "supabase", "novu",
            "openai", "anthropic", "cohere"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f" Found {len(cleaned_companies)} GitHub orgs (simulated)")
        return cleaned_companies
        
    async def fetch_producthunt_companies(self) -> List[str]:
        """Fetch companies from ProductHunt (simulated)."""
        source = 'producthunt'
        logger.info(f"🚀 Fetching from ProductHunt (simulated)...")
        # In a real app, this would scrape a list of top products/makers.
        companies = [
            "Loom", "Miro", "Linear", "Height", "Revolut", "Brex", "Ramp", 
            "Mendel", "Fivetran", "Airbyte", "Retool", "Webflow", "Gatsby",
            "Next.js", "Vercel", "Chime", "Affirm", "Klarna"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f" Found {len(cleaned_companies)} ProductHunt companies (simulated)")
        return cleaned_companies

    # ... (other Tier 1 sources would be implemented here)
    
    # ========================================================================
    # TIER 2 SOURCES - Medium Hit Rate (Established Businesses)
    # ========================================================================
    
    async def fetch_sec_edgar_companies(self) -> List[str]:
        """Fetch public companies from SEC EDGAR (simulated)."""
        source = 'sec_edgar'
        logger.info(f"🚀 Fetching from SEC EDGAR (simulated)...")
        # In a real app, this would process the public company index file.
        companies = [
            "Salesforce", "Alphabet", "Meta Platforms", "Tesla", "Microsoft", 
            "Apple", "Amazon", "Netflix", "Adobe", "Intel", 
            "IBM", "Oracle", "SAP", "Cisco", "Qualcomm"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f" Found {len(cleaned_companies)} SEC EDGAR companies (simulated)")
        return cleaned_companies
        
    async def fetch_usaspending_companies(self) -> List[str]:
        """Fetch federal contractors from USASpending (simulated)."""
        source = 'usaspending'
        logger.info(f"🚀 Fetching from USASpending.gov (simulated)...")
        companies = [
            "Raytheon", "Lockheed Martin", "General Dynamics", "Boeing", 
            "Northrop Grumman", "Leidos", "SAIC", "Booz Allen Hamilton"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f" Found {len(cleaned_companies)} USASpending companies (simulated)")
        return cleaned_companies

    # ... (other Tier 2 sources would be implemented here)
    
    # ========================================================================
    # EXPANSION MANAGEMENT
    # ========================================================================
    
    async def _run_source(self, source_key: str) -> List[str]:
        """Execute the fetching function for a single source."""
        config = SOURCES[source_key]
        if not config.enabled:
            logger.info(f"Skipping disabled source: {config.name}")
            return []
            
        fetch_func = getattr(self, f'fetch_{source_key}_companies', None)
        if not fetch_func:
            logger.error(f"No fetch function found for source: {source_key}")
            return []
            
        try:
            companies = await fetch_func()
            return companies
        except Exception as e:
            logger.error(f"Failed to run source {source_key}: {e}")
            return []

    async def _process_results(self, source_key: str, companies: List[str]):
        """Clean, dedup, and upsert companies into the database."""
        config = SOURCES[source_key]
        
        new_companies = [c for c in companies if c not in self.discovered_companies]
        
        # Upsert into database
        added = self.db.upsert_seed_companies(
            companies=new_companies,
            source=config.name,
            tier=config.tier,
            priority=config.priority
        )
        
        self.results[config.name] = new_companies
        self.discovered_companies.update(new_companies)
        
        logger.info(f"✅ Source {config.name}: {len(companies)} total, {added} new seeds added.")

    async def expand_tier1(self) -> Dict[str, List[str]]:
        """Run all Tier 1 expansion sources."""
        tier1_keys = [k for k, v in SOURCES.items() if v.tier == 1]
        
        tasks = [self._run_source(key) for key in tier1_keys]
        results = await asyncio.gather(*tasks)
        
        for key, companies in zip(tier1_keys, results):
            await self._process_results(key, companies)
            
        return self.results
        
    async def expand_tier2(self) -> Dict[str, List[str]]:
        """Run all Tier 2 expansion sources."""
        tier2_keys = [k for k, v in SOURCES.items() if v.tier == 2]
        
        tasks = [self._run_source(key) for key in tier2_keys]
        results = await asyncio.gather(*tasks)
        
        for key, companies in zip(tier2_keys, results):
            await self._process_results(key, companies)
            
        return self.results

    async def expand_all(self) -> Dict[str, List[str]]:
        """Run all expansion sources."""
        await self.expand_tier1()
        await self.expand_tier2()
        
        return self.results

    # ========================================================================
    # UTILITY FUNCTIONS
    # ========================================================================
    
    def _clean_company_name(self, name: str) -> str:
        """Standardize and clean company names for token generation."""
        name = name.lower().strip()
        # Remove common business suffixes
        name = re.sub(r'\s+(inc|co|corp|llc|ltd|gmbh|sa|bv)\.?$', '', name)
        # Remove common cruft
        name = re.sub(r'[^a-z0-9\s-]', '', name)
        # Standardize spaces to dashes (for potential token slug)
        name = re.sub(r'\s+', '-', name)
        return name
        
    def _is_valid_company_name(self, name: str) -> bool:
        """Simple validation check."""
        # Must be at least 2 characters long and not purely numerical
        if len(name) < 2 or name.isdigit():
            return False
        # Exclude common generic stop words
        generic_words = {"the", "a", "an", "software", "solutions", "group", "labs", "tech", "studio"}
        if name.lower() in generic_words:
            return False
        return True

    def print_source_stats(self):
        """Prints the final source statistics."""
        stats = self.db.get_source_stats()
        logger.info("\n=========================")
        logger.info("SEED EXPANSION SUMMARY")
        logger.info("=========================")
        for stat in stats:
            logger.info(f"Source: {stat['source']} (Tier {SOURCES[stat['source'].lower().split()[0]].tier})")
            logger.info(f"  Discovered: {stat['seeds_discovered']:,}")
            logger.info(f"  Tested: {stat['seeds_tested']:,}")
            logger.info(f"  Hit Rate: {stat['hit_rate']:.2%}")
            logger.info(f"  Enabled: {stat['enabled']}")
        logger.info("=========================")


# ========================================================================
# MAIN ENTRY POINTS
# ========================================================================

async def run_tier1_expansion() -> Dict[str, List[str]]:
    """Run Tier 1 expansion only."""
    expander = SeedExpander()
    try:
        results = await expander.expand_tier1()
        return results
    finally:
        await expander.close()


async def run_tier2_expansion() -> Dict[str, List[str]]:
    """Run Tier 2 expansion only."""
    expander = SeedExpander()
    try:
        results = await expander.expand_tier2()
        return results
    finally:
        await expander.close()


async def run_full_expansion() -> Dict[str, List[str]]:
    """Run full expansion (all tiers)."""
    expander = SeedExpander()
    try:
        results = await expander.expand_all()
        return results
    finally:
        await expander.close()


async def main():
    """Run the seed expander."""
    expander = SeedExpander()
    
    try:
        # Run full expansion
        results = await expander.expand_all()
        
        # Print source stats
        expander.print_source_stats()
        
        # Get total seeds available
        stats = expander.db.get_stats()
        print(f"\n✅ Total seeds in database: {stats.get('total_seeds', 0)}")
        print(f"   Seeds tested: {stats.get('seeds_tested', 0)}")
        
    finally:
        await expander.close()


if __name__ == "__main__":
    asyncio.run(main())
