import asyncio
import aiohttp
import json
import re
import logging
import os
from typing import List, Set, Dict, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
import csv

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
    url: Optional[str] = None # Added real URL field


# Source configurations (UPDATED for REAL Sources)
SOURCES = {
    # Tier 1 - High Hit Rate (Tech/Startups, High-Growth, Known ATS Users)
    # Note: These URLs are structural examples of stable public resources or curated lists.
    # The functions below contain simulated parsing logic for their typical data structure.
    'yc_top': SourceConfig(
        'Y Combinator Top Companies', 
        tier=1, 
        priority=95, 
        # Real-world list of top YC companies (must be scraped or paid for)
        url='https://example-data.com/yc-top-companies.json' 
    ),
    'gh_lever_users': SourceConfig(
        'Greenhouse/Lever Public Clients', 
        tier=1, 
        priority=90, 
        # A curated list of companies known to use a target ATS, often shared publicly.
        url='https://example-data.com/ats-users.csv' 
    ),
    'deloitte_fast_500': SourceConfig(
        'Deloitte Technology Fast 500', 
        tier=1, 
        priority=85, 
        # A list of fast-growing companies that are likely to be early ATS adopters.
        url='https://example-data.com/deloitte-fast500-snapshot.json' 
    ),
    

    # Tier 2 - Medium Hit Rate (Established Businesses, public/large non-tech)
    'sec_master_ciks': SourceConfig(
        'SEC EDGAR Master CIK List', 
        tier=2, 
        priority=75, 
        # Real SEC index file listing all public companies (requires parsing)
        url='https://www.sec.gov/Archives/edgar/full-index/master.idx' 
    ),
    's_and_p_500': SourceConfig(
        'S&P 500 Index Constituents', 
        tier=2, 
        priority=70, 
        # Real-world list of S&P 500 constituents, often available as CSV
        url='https://example-data.com/sp500-constituents.csv' 
    ),
    'fortune_100_list': SourceConfig(
        'Fortune 100 Companies', 
        tier=2, 
        priority=65, 
        # A widely scraped list of the largest corporations.
        url='https://example-data.com/fortune100.txt' 
    ),
}


class SeedExpander:
    """Discovers and manages company seeds for job intelligence collection."""

    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_db()
        self.client: Optional[aiohttp.ClientSession] = None

    async def _get_client(self) -> aiohttp.ClientSession:
        """Get or create HTTP client."""
        if self.client is None or self.client.closed:
            self.client = aiohttp.ClientSession(
                # Use a specific User-Agent for polite data fetching
                headers={'User-Agent': 'JobIntelExpander/1.0 (contact: your-email@example.com)'}, 
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self.client

    async def close(self):
        """Close HTTP client."""
        if self.client and not self.client.closed:
            await self.client.close()

    async def _fetch(self, url: str) -> Optional[Any]:
        """Fetch a single URL and return content based on content-type."""
        client = await self._get_client()
        try:
            async with client.get(url, allow_redirects=True, timeout=15) as response:
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'json' in content_type:
                        return await response.json()
                    else:
                        return await response.text()
                elif response.status == 429:
                    logger.warning(f"Rate limit hit for {url}")
                    await asyncio.sleep(10) 
                    return await self._fetch(url)
                else:
                    logger.debug(f"Failed to fetch {url}. Status: {response.status}")
                    return None
        except asyncio.TimeoutError:
            logger.debug(f"Timeout fetching {url}")
            return None
        except aiohttp.ClientError as e:
            logger.debug(f"Client error fetching {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}")
            return None

    # --- TIER 1 SOURCES ---

    async def _expand_yc_top(self, source_config: SourceConfig) -> List[str]:
        """Expands seeds from a list of Y Combinator's top companies."""
        logger.info(f"Expanding from {source_config.name}...")
        
        # Simulating data structure from a common JSON API response for YC
        # In a real system, you would call: data = await self._fetch(source_config.url)
        simulated_data = [
            {"name": "Stripe", "status": "active"}, 
            {"name": "Cloudflare", "status": "active"},
            {"name": "DoorDash", "status": "active"},
            {"name": "HubSpot", "status": "active"},
        ]
        
        return [item['name'] for item in simulated_data if item.get('status') == 'active']

    async def _expand_gh_lever_users(self, source_config: SourceConfig) -> List[str]:
        """Expands seeds from the curated list of known Greenhouse/Lever clients."""
        logger.info(f"Expanding from {source_config.name} (Curated List)...")
        
        # Using the list provided by the user as a real-world, curated data set.
        raw_names = [
            'zentai', 'brightcove', 'matic', 'fletcher jones imports', 'company cam', 
            'ionq', 'wiz', 'pay stand', 'slick deals', 'validation cloud', 'vega', 
            'SpaceX', 'Cloudflare', 'Cisco', 'DoorDash', 'DocuSign', 'Dropbox', 
            'HubSpot', 'Stripe', 'Pinterest', 'Squarespace', 'Wayfair', 'GoDaddy', 
            'Warby Parker', 'Lyft', 'Oscar Health', 'Tencent', 'PlayStation', 
            'Canonical', 'Okta', 'Affirm', 'Betterment', 'TripAdvisor', 'Vimeo', 
            'Instacart', 'Evernote', 'Foursquare', 'Notion', 'Postman', 'Outlier AI', 
            'Unity Technologies', 'Anduril Industries', '10x Genomics', 
            'Toyota Motor Corporation', 'Accenture', 'UPS', 'AB Global', 
            'Earnest Operations LLC', 'Ouihelp', 'Asset Living', 'TÜV Rheinland', 
            'Onica'
        ]
        return raw_names

    async def _expand_deloitte_fast_500(self, source_config: SourceConfig) -> List[str]:
        """Expands seeds from a list of fast-growing tech companies."""
        logger.info(f"Expanding from {source_config.name}...")
        
        # Simulating JSON data containing company names and growth details
        simulated_data = [
            {"company": "10x Genomics"},
            {"company": "Oscar Health"},
            {"company": "Anduril Industries"},
        ]
        
        return [item['company'] for item in simulated_data]


    # --- TIER 2 SOURCES ---

    async def _expand_sec_master_ciks(self, source_config: SourceConfig) -> List[str]:
        """
        Fetches and parses company names from the SEC EDGAR master CIK file.
        This demonstrates parsing of a large, publicly available index file.
        """
        logger.info(f"Expanding from {source_config.name}. (File: {source_config.url})")
        
        # The actual file is pipe-delimited and very large. We simulate parsing a small chunk.
        # REAL ACTION: raw_text = await self._fetch(source_config.url)
        
        simulated_sec_data = """
        CIK|Company Name|Form Type|Date Filed|Filename
        0000000001|General Electric Company|8-K|2023-01-01|edgar/data/...
        0000000002|Cisco Systems Inc.|8-K|2023-01-01|edgar/data/...
        0000000003|Toyota Motor Corporation|8-K|2023-01-01|edgar/data/...
        """
        
        company_names = []
        lines = [line.strip() for line in simulated_sec_data.split('\n') if line.strip()]
        
        for line in lines[1:]: # Skip header
            parts = line.split('|')
            if len(parts) >= 2:
                # The company name is the second field
                company_names.append(parts[1].strip())
                
        return company_names

    async def _expand_s_and_p_500(self, source_config: SourceConfig) -> List[str]:
        """Expands seeds from a list of companies in the S&P 500 Index."""
        logger.info(f"Expanding from {source_config.name}...")
        
        # Simulating fetching a CSV list of company names (common for public index data)
        # REAL ACTION: raw_text = await self._fetch(source_config.url)
        
        simulated_csv = """
        Symbol,Name,Sector
        MSFT,Microsoft Corporation,Technology
        GOOGL,Alphabet Inc.,Communication Services
        JPM,JPMorgan Chase & Co.,Financials
        """
        
        company_names = []
        # Use StringIO to treat the string as a file for csv.reader
        reader = csv.reader(StringIO(simulated_csv))
        # Skip header
        next(reader) 
        
        for row in reader:
            if len(row) > 1:
                company_names.append(row[1].strip())
                
        return company_names

    async def _expand_fortune_100_list(self, source_config: SourceConfig) -> List[str]:
        """Expands seeds from a list of the largest Fortune 100 companies."""
        logger.info(f"Expanding from {source_config.name}...")
        
        # Simulating fetching a newline-separated list
        # REAL ACTION: raw_text = await self._fetch(source_config.url)
        
        simulated_list = """
        Walmart
        Exxon Mobil
        Apple
        """
        
        return [name.strip() for name in simulated_list.split('\n') if name.strip()]


    # --- EXECUTION ---

    def _get_expansion_func(self, source_name: str):
        """Maps source name to the expansion function."""
        return getattr(self, f'_expand_{source_name}', None)

    def _process_names(self, company_names: List[str], source_config: SourceConfig) -> List[Tuple[str, str, str, int]]:
        """Filters, cleans, and prepares names for insertion."""
        processed_seeds = []
        for name in company_names:
            clean_name = name.strip()
            if not self._is_valid_company_name(clean_name):
                continue
            
            token_slug = self._name_to_token(clean_name)
            processed_seeds.append((clean_name, token_slug, source_config.name, source_config.tier))
            
        return processed_seeds

    def _name_to_token(self, name: str) -> str:
        """Converts a company name to a URL-friendly, lowercase ATS token/slug."""
        token = name.lower()
        token = re.sub(r'\s+(inc|llc|ltd|co|corp|gmbh|sa)\.?$', '', token, flags=re.IGNORECASE)
        token = re.sub(r'[^a-z0-9\s-]', '', token)
        token = re.sub(r'[\s-]+', '-', token).strip('-')
        return token

    def _is_valid_company_name(self, name: str) -> bool:
        """Simple check to filter out generic or invalid names."""
        generic_words = {'the', 'a', 'inc', 'llc', 'co', 'corp', 'group', 'solutions', 'services', 'systems', 'sa', 'gmbh', 'ltd', 'company', 'corporation'} 
        if not name or len(name.split()) < 1: 
            return False
            
        name_parts = name.lower().split()
        if all(part in generic_words for part in name_parts):
            return False

        return True


    # ========================================================================
    # MAIN ENTRY POINTS
    # ========================================================================

    async def expand_tier1(self) -> Dict[str, List[str]]:
        """Run Tier 1 expansion only."""
        return await self._run_expansion_tier(1)

    async def expand_tier2(self) -> Dict[str, List[str]]:
        """Run Tier 2 expansion only."""
        return await self._run_expansion_tier(2)

    async def expand_all(self) -> Dict[str, List[str]]:
        """Run full expansion (all tiers)."""
        return await self._run_expansion_tier(1, 2)

    async def _run_expansion_tier(self, *tiers: int) -> Dict[str, List[str]]:
        """Core execution logic for specified tiers."""
        logger.info(f"Starting seed expansion for tiers: {tiers}")
        results_by_source: Dict[str, List[str]] = {}
        all_tasks = []

        # 1. Filter and sort sources by priority
        active_sources = [
            (config.priority, name, config)
            for name, config in SOURCES.items()
            if config.enabled and config.tier in tiers
        ]
        active_sources.sort(key=lambda x: x[0], reverse=True)

        for priority, name, config in active_sources:
            func = self._get_expansion_func(name)
            if func:
                all_tasks.append(func(config))
                
        # 2. Run all expansion tasks concurrently
        raw_results = await asyncio.gather(*all_tasks, return_exceptions=True)
        
        # 3. Process results and insert into DB
        for (priority, name, config), result in zip(active_sources, raw_results):
            if isinstance(result, Exception):
                logger.error(f"Error expanding source {name}: {result}")
                continue
            
            if result:
                processed_seeds = self._process_names(result, config)
                self.db.insert_seeds(processed_seeds)
                results_by_source[name] = [name for name, _, _, _ in processed_seeds] # Store names
                logger.info(f"Source {name} finished. Inserted {len(processed_seeds)} new seeds.")

        logger.info("✅ Seed expansion complete.")
        return results_by_source

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
        results = await expander.expand_all()
        
        stats = expander.db.get_stats()
        print(f"\n✅ Total seeds in database: {stats.get('total_seeds', 0)}")
        print(f"   Untested: {stats.get('untested_seeds', 0)}")
        
    finally:
        await expander.close()


if __name__ == "__main__":
    asyncio.run(main())
