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


# CORE FIX: HELPER FUNCTION
def _name_to_token(name: str) -> str:
    """
    Converts a company name to a URL-friendly, lowercase ATS token/slug.
    This is the FIX for the core issue.
    """
    # 1. Convert to lowercase
    token = name.lower()
    # 2. Remove common business suffixes (to increase chance of matching slugs like 'plaid' vs 'plaid-inc')
    token = re.sub(r'\s+(inc|llc|ltd|co|corp|gmbh|sa)\.?$', '', token, flags=re.IGNORECASE)
    # 3. Replace non-alphanumeric characters (except space/hyphen) with nothing
    token = re.sub(r'[^a-z0-9\s-]', '', token)
    # 4. Replace spaces and multiple hyphens with a single hyphen
    token = re.sub(r'[\s-]+', '-', token).strip('-')
    return token

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
    'crunchbase_free': SourceConfig('Crunchbase Free', tier=1, priority=70),

    # Tier 2 - Medium hit rate (established businesses)
    'sec_edgar': SourceConfig('SEC EDGAR', tier=2, priority=50),
    'usas_gov': SourceConfig('USASpending.gov', tier=2, priority=45),
    'sam_gov': SourceConfig('SAM.gov', tier=2, priority=40),
    'inc_5000': SourceConfig('Inc 5000', tier=2, priority=35),
    'fortune_lists': SourceConfig('Fortune Lists', tier=2, priority=30),
    'glassdoor': SourceConfig('Glassdoor', tier=2, priority=25),
}


class SeedExpander:
    """Manages the discovery and expansion of new company seeds."""

    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_db()
        self.client = aiohttp.ClientSession(
            headers={'User-Agent': 'JobIntelExpander/2.0'},
            trust_env=True,
            connector=aiohttp.TCPConnector(ssl=False)
        )
        self.source_stats = {name: self.db.get_source_stats() for name in SOURCES}

    async def close(self):
        """Close HTTP client session."""
        await self.client.close()

    def _check_source_enabled(self, source_name: str) -> bool:
        """Checks if a source is currently enabled in the database."""
        stats = next((s for s in self.source_stats.get(source_name, []) if s['source'] == source_name), None)
        return stats['enabled'] if stats else True
    
    def _upsert_seed(self, name: str, source: str, priority: int) -> bool:
        """Wrapper to call database upsert with token slug generation."""
        # FIX IMPLEMENTATION: Generate the token slug here
        token_slug = _name_to_token(name)
        
        # Guard against empty slugs (e.g., if input was just 'Inc.')
        if not token_slug:
            return False
            
        return self.db.upsert_seed_company(name, token_slug, source, priority)

    # ========================================================================
    # TIER 1 Expansion Methods (High Hit Rate)
    # ========================================================================

    async def _fetch(self, url: str) -> Optional[str]:
        """Generic fetch with basic error handling."""
        try:
            async with self.client.get(url, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
                logger.warning(f"Failed to fetch {url}. Status: {response.status}")
                return None
        except asyncio.TimeoutError:
            logger.error(f"Timeout while fetching {url}")
            return None
        except aiohttp.ClientError as e:
            logger.error(f"Client error fetching {url}: {e}")
            return None

    async def _expand_yc(self, config: SourceConfig) -> List[str]:
        """Scrapes Y Combinator companies (Example implementation)."""
        logger.info(f"Expanding from {config.name}...")
        discovered_names = []
        
        # This is a mock example, replace with actual scraper logic
        try:
            # Simulate fetching a list of names
            names = ["Airbnb", "Stripe, Inc.", "DoorDash", "Dropbox", "Coinbase", "Plum Life", "Scribe", "Retool Technologies, Inc"]
            
            for name in names:
                if self._upsert_seed(name, config.name, config.priority):
                    discovered_names.append(name)
        except Exception as e:
            logger.error(f"Error in {config.name} expansion: {e}")

        logger.info(f"Discovered {len(discovered_names)} new seeds from {config.name}")
        return discovered_names

    async def _expand_github_orgs(self, config: SourceConfig) -> List[str]:
        """Gets large GitHub organizations (Example implementation)."""
        logger.info(f"Expanding from {config.name}...")
        discovered_names = []
        
        # This is a mock example, replace with actual API/scraper logic
        try:
            names = ["Microsoft", "Google", "Facebook", "Netflix", "Uber", "Lyft Inc", "Unity Technologies"]
            
            for name in names:
                if self._upsert_seed(name, config.name, config.priority):
                    discovered_names.append(name)
        except Exception as e:
            logger.error(f"Error in {config.name} expansion: {e}")

        logger.info(f"Discovered {len(discovered_names)} new seeds from {config.name}")
        return discovered_names

    async def _expand_producthunt(self, config: SourceConfig) -> List[str]:
        """Gets company names from ProductHunt (example for illustration)."""
        logger.info(f"Expanding from {config.name}...")
        discovered_names = []
        
        # This is a mock example, replace with actual scraper logic
        try:
            names = ["Figma", "Webflow", "Notion", "Airtable", "Zoom", "Slack, Co.", "Miro"]
            
            for name in names:
                # We use the product name as a seed company name
                if self._upsert_seed(name, config.name, config.priority):
                    discovered_names.append(name)
        except Exception as e:
            logger.error(f"Error in {config.name} expansion: {e}")

        logger.info(f"Discovered {len(discovered_names)} new seeds from {config.name}")
        return discovered_names
        
    # Placeholder methods for Tier 1
    async def _expand_github_awesome(self, config: SourceConfig) -> List[str]:
        logger.debug(f"Skipping {config.name} (Placeholder)")
        return []
    
    async def _expand_crunchbase_free(self, config: SourceConfig) -> List[str]:
        logger.debug(f"Skipping {config.name} (Placeholder)")
        return []

    # ========================================================================
    # TIER 2 Expansion Methods (Medium Hit Rate)
    # ========================================================================
    
    async def _expand_sec_edgar(self, config: SourceConfig) -> List[str]:
        """Fetches public company names from SEC EDGAR (Example implementation)."""
        logger.info(f"Expanding from {config.name}...")
        discovered_names = []
        
        # This is a mock example, replace with actual API/scraper logic
        try:
            names = ["General Motors", "Ford", "IBM", "Exxon Mobil", "Intel Corporation"]
            
            for name in names:
                if self._upsert_seed(name, config.name, config.priority):
                    discovered_names.append(name)
        except Exception as e:
            logger.error(f"Error in {config.name} expansion: {e}")

        logger.info(f"Discovered {len(discovered_names)} new seeds from {config.name}")
        return discovered_names

    # Placeholder methods for Tier 2
    async def _expand_usas_gov(self, config: SourceConfig) -> List[str]:
        logger.debug(f"Skipping {config.name} (Placeholder)")
        return []
    
    async def _expand_sam_gov(self, config: SourceConfig) -> List[str]:
        logger.debug(f"Skipping {config.name} (Placeholder)")
        return []

    async def _expand_inc_5000(self, config: SourceConfig) -> List[str]:
        logger.debug(f"Skipping {config.name} (Placeholder)")
        return []
    
    async def _expand_fortune_lists(self, config: SourceConfig) -> List[str]:
        logger.debug(f"Skipping {config.name} (Placeholder)")
        return []

    async def _expand_glassdoor(self, config: SourceConfig) -> List[str]:
        logger.debug(f"Skipping {config.name} (Placeholder)")
        return []

    # ========================================================================
    # MAIN EXPANSION RUNNER
    # ========================================================================

    async def _run_tier(self, tier: int) -> Dict[str, List[str]]:
        """Runs all expansion methods for a given tier."""
        tier_methods = {
            1: [self._expand_yc, self._expand_github_orgs, self._expand_producthunt, self._expand_github_awesome, self._expand_crunchbase_free],
            2: [self._expand_sec_edgar, self._expand_usas_gov, self._expand_sam_gov, self._expand_inc_5000, self._expand_fortune_lists, self._expand_glassdoor],
        }

        tasks = []
        tier_sources = [config for name, config in SOURCES.items() if config.tier == tier]
        
        for config in tier_sources:
            if self._check_source_enabled(config.name):
                # Map config to the correct method
                method_name = f'_expand_{config.name.lower().replace(" ", "_").replace(".", "").replace(",", "")}'
                # Attempt to find the corresponding method, otherwise skip (for placeholders)
                method = next((m for m in tier_methods[tier] if m.__name__ == method_name), None)
                
                if method:
                    tasks.append(method(config))
                else:
                    logger.warning(f"No implementation found for source: {config.name}")

        results = await asyncio.gather(*tasks)
        
        all_discovered = [name for result in results for name in result]
        return {
            'tier': tier,
            'total_new_seeds': len(all_discovered),
            'unique_names': all_discovered # In a real implementation, you'd track unique names better
        }

    async def expand_tier1(self) -> Dict[str, List[str]]:
        """Run Tier 1 expansion only."""
        return await self._run_tier(tier=1)

    async def expand_tier2(self) -> Dict[str, List[str]]:
        """Run Tier 2 expansion only."""
        return await self._run_tier(tier=2)

    async def expand_all(self) -> Dict[str, Any]:
        """Run full expansion (all tiers)."""
        logger.info("Starting full seed expansion (Tier 1 & 2)...")
        t1_results = await self._run_tier(tier=1)
        t2_results = await self._run_tier(tier=2)
        
        total_unique = set(t1_results['unique_names'] + t2_results['unique_names'])
        
        logger.info(f"Expansion finished. T1: {t1_results['total_new_seeds']} seeds. T2: {t2_results['total_new_seeds']} seeds.")
        
        return {
            'tier1': t1_results,
            'tier2': t2_results,
            'total_unique': total_unique
        }

    def print_source_stats(self):
        """Prints current source performance statistics."""
        stats = self.db.get_source_stats()
        print("\n--- Seed Source Performance ---")
        for s in stats:
            print(f"  [{'✓' if s['enabled'] else '✗'}] {s['source']:<20} | Discovered: {s['seeds_discovered']:,} | Tested: {s['seeds_tested']:,} | Hit Rate: {s['hit_rate'] * 100:.2f}%")
        print("-------------------------------")


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


async def run_full_expansion() -> Dict[str, Any]:
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
