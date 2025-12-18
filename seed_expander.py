import asyncio
import aiohttp
import json
import re
import logging
import random
from typing import List, Tuple, Dict, Optional
from dataclasses import dataclass
from datetime import datetime

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from database import get_db, Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ua = UserAgent()

@dataclass
class SourceConfig:
    name: str
    tier: int
    priority: int
    url: Optional[str] = None
    enabled: bool = True

# Real 2025 Sources
SOURCES = {
    'yc_directory': SourceConfig(
        'Y Combinator Companies Directory', tier=1, priority=95,
        url='https://www.ycombinator.com/companies'
    ),
    'ats_curated': SourceConfig(
        'Curated Greenhouse/Lever Users', tier=1, priority=90,
        url=None  # Hard-coded list
    ),
    'deloitte_fast500': SourceConfig(
        'Deloitte Technology Fast 500 2025', tier=1, priority=85,
        url='https://www.deloitte.com/us/en/Industries/tmt/articles/fast500-winners.html'
    ),
    'wikipedia_sp500': SourceConfig(
        'Wikipedia S&amp;P 500 Companies', tier=2, priority=80,
        url='https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    ),
    'sec_tickers': SourceConfig(
        'SEC Official Company Tickers', tier=2, priority=75,
        url='https://www.sec.gov/files/company_tickers.json'
    ),
}

class SeedExpander:
    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_db()
        self.client: Optional[aiohttp.ClientSession] = None

    async def _get_client(self) -> aiohttp.ClientSession:
        if self.client is None or self.client.closed:
            self.client = aiohttp.ClientSession(
                headers={'User-Agent': ua.random},
                timeout=aiohttp.ClientTimeout(total=60)
            )
        return self.client

    async def close(self):
        if self.client and not self.client.closed:
            await self.client.close()

    async def _fetch_text(self, url: str) -> Optional[str]:
        client = await self._get_client()
        try:
            async with client.get(url) as resp:
                if resp.status == 200:
                    return await resp.text()
                else:
                    logger.warning(f"HTTP {resp.status} for {url}")
        except Exception as e:
            logger.error(f"Fetch failed {url}: {e}")
        return None

    # ===== TIER 1 SOURCES =====

    async def _expand_yc_directory(self, config: SourceConfig) -> List[str]:
        logger.info(f"Expanding YC companies from {config.url}")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        # YC page has links with company names
        for a in soup.find_all('a', href=re.compile(r'/companies/')):
            text = a.get_text(strip=True)
            if text and len(text) > 2:
                companies.add(text)
        await asyncio.sleep(random.uniform(2, 5))
        return list(companies)[:300]  # Limit to avoid overload

    async def _expand_ats_curated(self, config: SourceConfig) -> List[str]:
        logger.info("Expanding curated ATS users")
        return [
            'Stripe', 'Airbnb', 'Dropbox', 'Reddit', 'Pinterest', 'Slack', 'Coinbase',
            'Instacart', 'DoorDash', 'Brex', 'Notion', 'Figma', 'Vercel', 'Cloudflare',
            'SpaceX', 'Anduril', 'Scale AI', 'Anthropic', 'OpenAI', 'Cruise', 'IonQ',
            'HubSpot', 'Okta', 'Affirm', 'Postman', 'Unity', 'Vimeo', 'Lyft', 'Oscar Health'
            # Add more from your original list
        ]

    async def _expand_deloitte_fast500(self, config: SourceConfig) -> List[str]:
        logger.info(f"Expanding Deloitte Fast 500 from {config.url}")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        # Look for table or list items
        for td in soup.find_all('td'):
            text = td.get_text(strip=True)
            if text and re.match(r'^\d+$', text):  # Rank column
                sibling = td.find_next_sibling('td')
                if sibling:
                    companies.add(sibling.get_text(strip=True))
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)

    # ===== TIER 2 SOURCES =====

    async def _expand_wikipedia_sp500(self, config: SourceConfig) -> List[str]:
        logger.info(f"Expanding S&amp;P 500 from Wikipedia")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table', {'id': 'constituents'})
        if not table:
            return []
        companies = []
        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            if cells:
                name = cells[1].get_text(strip=True)
                if name:
                    companies.append(name)
        await asyncio.sleep(random.uniform(2, 4))
        return companies

    async def _expand_sec_tickers(self, config: SourceConfig) -> List[str]:
        logger.info("Expanding from official SEC tickers JSON")
        text = await self._fetch_text(config.url)
        if not text:
            return []
        try:
            data = json.loads(text)
            return [item['title'] for item in data.values()]
        except Exception as e:
            logger.error(f"JSON parse error: {e}")
        return []

    # ===== UTILS =====

    def _name_to_token(self, name: str) -> str:
        token = name.lower()
        token = re.sub(r'\s+(inc|llc|ltd|co|corp|gmbh|sa)\.?$', '', token, flags=re.IGNORECASE)
        token = re.sub(r'[^a-z0-9\s-]', '', token)
        token = re.sub(r'[\s-]+', '-', token).strip('-')
        return token

    def _is_valid_company_name(self, name: str) -> bool:
        if not name or len(name) < 3:
            return False
        banned = {'inc', 'llc', 'corp', 'ltd', 'plc', 'gmbh', 'sa', 'ag', 'group', 'holdings', 'the'}
        words = set(name.lower().split())
        if words.issubset(banned):
            return False
        return True

    def _process_names(self, names: List[str], config: SourceConfig) -> List[Tuple[str, str, str, int]]:
        processed = []
        seen = set()
        for name in names:
            clean = name.strip().title()
            if not clean or clean.lower() in seen or not self._is_valid_company_name(clean):
                continue
            seen.add(clean.lower())
            token = self._name_to_token(clean)
            processed.append((clean, token, config.name, config.tier))
        return processed

    async def _run_expansion(self, *tiers: int) -> Dict[str, int]:
        active = [(c.priority, name, c) for name, c in SOURCES.items() if c.enabled and c.tier in tiers]
        active.sort(key=lambda x: x[0], reverse=True)

        results = {}
        for _, name, config in active:
            func = getattr(self, f'_expand_{name}', None)
            if not func:
                logger.warning(f"No function for {name}")
                continue
            try:
                raw_names = await func(config)
                processed = self._process_names(raw_names, config)
                if processed:
                    self.db.insert_seeds(processed)
                    results[name] = len(processed)
                    logger.info(f"{name}: Added {len(processed)} new seeds")
            except Exception as e:
                logger.error(f"Error in {name}: {e}")
            await asyncio.sleep(random.uniform(3, 8))  # Politeness

        return results

    async def expand_tier1(self):
        return await self._run_expansion(1)

    async def expand_tier2(self):
        return await self._run_expansion(2)

    async def expand_all(self):
        return await self._run_expansion(1, 2)

# Convenience functions
async def run_tier1_expansion():
    expander = SeedExpander()
    try:
        return await expander.expand_tier1()
    finally:
        await expander.close()

async def run_tier2_expansion():
    expander = SeedExpander()
    try:
        return await expander.expand_tier2()
    finally:
        await expander.close()

async def run_full_expansion():
    expander = SeedExpander()
    try:
        return await expander.expand_all()
    finally:
        await expander.close()

if __name__ == "__main__":
    asyncio.run(run_full_expansion())
