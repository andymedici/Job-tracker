"""
Job Intelligence Collector v2.0
===============================
A high-performance, robust job board intelligence system.

MERGED: Gemini's improved token generation + Original refresh functionality

Key Features:
- Uses Greenhouse/Lever JSON APIs (not HTML scraping)
- Advanced token variant generation for maximum hit rate
- Async/parallel processing for 10x speed improvement
- Intelligent rate limiting with exponential backoff
- BOTH discovery (new companies) AND refresh (existing companies)
- PostgreSQL database for Railway deployment
"""

import asyncio
import aiohttp
import json
import logging
import os
import re
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, field, asdict
from urllib.parse import urlparse, quote

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from database import get_db, Database


# ============================================================================
# TOKEN GENERATION HELPERS (from Gemini)
# ============================================================================

def _name_to_token(name: str) -> str:
    """Converts a company name to a URL-friendly, lowercase ATS token/slug."""
    token = name.lower()
    token = re.sub(r'\s+(inc|llc|ltd|co|corp|gmbh|sa)\.?$', '', token, flags=re.IGNORECASE)
    token = re.sub(r'[^a-z0-9\s-]', '', token)
    token = re.sub(r'[\s-]+', '-', token).strip('-')
    return token


def _generate_token_variants(slug: str) -> List[str]:
    """Generates a list of likely ATS token variants for a given base slug."""
    variants = {slug}
    
    # Common career page prefixes/suffixes
    variants.add(f'{slug}-jobs')
    variants.add(f'{slug}jobs')
    variants.add(f'{slug}-careers')
    variants.add(f'{slug}careers')
    
    # Variants without common business suffixes
    variants.add(f'{slug}inc')
    variants.add(f'{slug}co')
    
    # No hyphen variant (e.g., 'the-home-depot' -> 'homedepot')
    no_hyphen = slug.replace('-', '')
    variants.add(no_hyphen)
    
    # First word only (for multi-word names)
    if '-' in slug:
        first_word = slug.split('-')[0]
        if len(first_word) >= 3:
            variants.add(first_word)
    
    # Order by likelihood (cleanest first)
    return sorted(list(variants), key=lambda x: (len(x), x))


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class JobBoard:
    """Represents a discovered job board."""
    ats_type: str  # 'greenhouse' or 'lever'
    token: str  # company identifier
    company_name: str
    job_count: int = 0
    remote_count: int = 0
    hybrid_count: int = 0
    onsite_count: int = 0
    locations: List[str] = field(default_factory=list)
    departments: List[str] = field(default_factory=list)
    jobs: List[Dict] = field(default_factory=list)
    source: str = ""
    discovered_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class CollectorStats:
    """Stats for a single collection run."""
    total_companies: int = 0
    total_jobs: int = 0
    greenhouse_found: int = 0
    lever_found: int = 0
    total_tested: int = 0
    refreshed: int = 0
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None

    def to_dict(self) -> Dict:
        """Returns a dict representation of stats."""
        d = asdict(self)
        d['duration_seconds'] = (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        d['start_time'] = self.start_time.isoformat() if self.start_time else None
        d['end_time'] = self.end_time.isoformat() if self.end_time else None
        return d


# ============================================================================
# MAIN COLLECTOR CLASS
# ============================================================================

class JobIntelCollector:
    """Core class for running the job board intelligence collection."""

    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_db()
        self.stats = CollectorStats()
        self.client: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._semaphore = asyncio.Semaphore(10)  # Limit concurrent API requests

    async def _get_client(self) -> aiohttp.ClientSession:
        """Get or create HTTP client."""
        if self.client is None or self.client.closed:
            self.client = aiohttp.ClientSession(
                headers={'User-Agent': 'JobIntelCollector/2.0'},
                trust_env=True,
                connector=aiohttp.TCPConnector(ssl=False),
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self.client

    async def _close_client(self):
        """Close HTTP client."""
        if self.client and not self.client.closed:
            await self.client.close()

    async def _exponential_backoff(self, attempt: int):
        """Implements rate-limiting backoff."""
        delay = min(2 ** attempt + random.random(), 60)
        logger.warning(f"Rate limiting hit. Waiting for {delay:.2f}s (Attempt {attempt})...")
        await asyncio.sleep(delay)

    async def _fetch(self, url: str) -> Optional[Dict]:
        """Fetch a single URL with retries and backoff."""
        client = await self._get_client()
        attempt = 0
        while attempt < 3:
            try:
                async with self._semaphore:
                    async with client.get(url, timeout=15) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status in (429, 503):
                            attempt += 1
                            await self._exponential_backoff(attempt)
                            continue
                        elif response.status == 404:
                            return None
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
                logger.debug(f"Unexpected error fetching {url}: {e}")
                return None
        return None

    def _classify_work_type(self, location: str) -> str:
        """Classify a job location as remote, hybrid, or onsite."""
        location_lower = location.lower()
        if any(kw in location_lower for kw in ['remote', 'anywhere', 'distributed', 'work from home', 'wfh']):
            return 'remote'
        elif any(kw in location_lower for kw in ['hybrid', 'flexible']):
            return 'hybrid'
        else:
            return 'onsite'

    def _process_greenhouse_jobs(self, jobs: List[Dict]) -> Tuple[int, int, int, List[str], List[str]]:
        """Process Greenhouse job listings and extract stats."""
        remote_count = hybrid_count = onsite_count = 0
        locations = set()
        departments = set()
        
        for job in jobs:
            location = job.get('location', {}).get('name', 'Unknown')
            dept = job.get('departments', [{}])
            dept_name = dept[0].get('name', 'Unknown') if dept else 'Unknown'
            
            locations.add(location)
            departments.add(dept_name)
            
            work_type = self._classify_work_type(location)
            if work_type == 'remote':
                remote_count += 1
            elif work_type == 'hybrid':
                hybrid_count += 1
            else:
                onsite_count += 1
        
        return remote_count, hybrid_count, onsite_count, list(locations), list(departments)

    def _process_lever_jobs(self, data: List) -> Tuple[int, int, int, int, List[str], List[str]]:
        """Process Lever job listings and extract stats."""
        remote_count = hybrid_count = onsite_count = 0
        locations = set()
        departments = set()
        job_count = 0
        
        for group in data:
            postings = group.get('postings', [])
            for job in postings:
                job_count += 1
                categories = job.get('categories', {})
                location = categories.get('location', 'Unknown')
                dept_name = categories.get('team', 'Unknown')
                
                locations.add(location)
                departments.add(dept_name)
                
                work_type = self._classify_work_type(location)
                if work_type == 'remote':
                    remote_count += 1
                elif work_type == 'hybrid':
                    hybrid_count += 1
                else:
                    onsite_count += 1
        
        return job_count, remote_count, hybrid_count, onsite_count, list(locations), list(departments)

    async def check_greenhouse(self, company_name: str, tokens: List[str]) -> Optional[JobBoard]:
        """Check all provided tokens for a live Greenhouse job board."""
        for token in tokens:
            url = f"https://boards-api.greenhouse.io/v1/boards/{quote(token)}/jobs?content=true"
            data = await self._fetch(url)
            
            if data and data.get('jobs'):
                job_list = data.get('jobs', [])
                remote, hybrid, onsite, locations, departments = self._process_greenhouse_jobs(job_list)
                
                logger.info(f"🟢 GH HIT: {company_name} (Token: {token}) with {len(job_list)} jobs")
                
                return JobBoard(
                    ats_type='greenhouse',
                    token=token,
                    company_name=company_name,
                    job_count=len(job_list),
                    remote_count=remote,
                    hybrid_count=hybrid,
                    onsite_count=onsite,
                    locations=locations,
                    departments=departments,
                    jobs=job_list,
                    source='discovery'
                )
        return None

    async def check_lever(self, company_name: str, tokens: List[str]) -> Optional[JobBoard]:
        """Check all provided tokens for a live Lever job board."""
        for token in tokens:
            url = f"https://api.lever.co/v0/postings/{quote(token)}?group=team&mode=json"
            data = await self._fetch(url)
            
            if isinstance(data, list) and data:
                job_count, remote, hybrid, onsite, locations, departments = self._process_lever_jobs(data)
                
                if job_count > 0:
                    logger.info(f"🔵 LV HIT: {company_name} (Token: {token}) with {job_count} jobs")
                    
                    return JobBoard(
                        ats_type='lever',
                        token=token,
                        company_name=company_name,
                        job_count=job_count,
                        remote_count=remote,
                        hybrid_count=hybrid,
                        onsite_count=onsite,
                        locations=locations,
                        departments=departments,
                        jobs=[],
                        source='discovery'
                    )
        return None

    def _save_company(self, board: JobBoard):
        """Save company data to database."""
        company_data = {
            'id': f"{board.ats_type}_{board.token}",
            'company_name': board.company_name,
            'ats_type': board.ats_type,
            'token': board.token,
            'job_count': board.job_count,
            'remote_count': board.remote_count,
            'hybrid_count': board.hybrid_count,
            'onsite_count': board.onsite_count,
            'locations': board.locations,
            'departments': board.departments,
        }
        self.db.upsert_company(company_data)

    async def _test_company(self, seed_id: int, company_name: str, token_slug: str, source: str) -> Tuple[bool, int]:
        """Test a single company against both ATS platforms."""
        tokens_to_test = _generate_token_variants(token_slug)
        
        # Try Greenhouse first
        greenhouse = await self.check_greenhouse(company_name, tokens_to_test)
        if greenhouse:
            self._save_company(greenhouse)
            self.stats.greenhouse_found += 1
            self.stats.total_companies += 1
            self.stats.total_jobs += greenhouse.job_count
            return True, greenhouse.job_count
        
        # Try Lever
        lever = await self.check_lever(company_name, tokens_to_test)
        if lever:
            self._save_company(lever)
            self.stats.lever_found += 1
            self.stats.total_companies += 1
            self.stats.total_jobs += lever.job_count
            return True, lever.job_count
        
        return False, 0

    async def _refresh_company(self, company: Dict) -> bool:
        """Refresh a single existing company's job data."""
        company_id = company['id']
        ats_type = company['ats_type']
        token = company['token']
        company_name = company['company_name']
        
        if ats_type == 'greenhouse':
            url = f"https://boards-api.greenhouse.io/v1/boards/{quote(token)}/jobs?content=true"
            data = await self._fetch(url)
            
            if data and data.get('jobs'):
                job_list = data.get('jobs', [])
                remote, hybrid, onsite, locations, departments = self._process_greenhouse_jobs(job_list)
                
                company_data = {
                    'id': company_id,
                    'company_name': company_name,
                    'ats_type': ats_type,
                    'token': token,
                    'job_count': len(job_list),
                    'remote_count': remote,
                    'hybrid_count': hybrid,
                    'onsite_count': onsite,
                    'locations': locations,
                    'departments': departments,
                }
                self.db.upsert_company(company_data)
                self.stats.total_jobs += len(job_list)
                return True
                
        elif ats_type == 'lever':
            url = f"https://api.lever.co/v0/postings/{quote(token)}?group=team&mode=json"
            data = await self._fetch(url)
            
            if isinstance(data, list) and data:
                job_count, remote, hybrid, onsite, locations, departments = self._process_lever_jobs(data)
                
                company_data = {
                    'id': company_id,
                    'company_name': company_name,
                    'ats_type': ats_type,
                    'token': token,
                    'job_count': job_count,
                    'remote_count': remote,
                    'hybrid_count': hybrid,
                    'onsite_count': onsite,
                    'locations': locations,
                    'departments': departments,
                }
                self.db.upsert_company(company_data)
                self.stats.total_jobs += job_count
                return True
        
        return False

    # ========================================================================
    # MAIN ENTRY POINTS
    # ========================================================================

    async def run_discovery(self, max_companies: Optional[int] = None) -> CollectorStats:
        """Run discovery loop for new companies from the seed database."""
        self._running = True
        self.stats = CollectorStats()
        
        seeds = self.db.get_seeds_for_collection(max_companies or 500)
        logger.info(f"Starting discovery on {len(seeds)} seeds...")
        
        batch_size = 50
        for i in range(0, len(seeds), batch_size):
            if not self._running:
                logger.info("Collection stopped by request.")
                break
            
            batch = seeds[i:i + batch_size]
            tasks = []
            
            for seed_id, company_name, token_slug, source in batch:
                tasks.append(self._test_company(seed_id, company_name, token_slug, source))
            
            await asyncio.sleep(1)  # Rate limiting
            results = await asyncio.gather(*tasks)
            
            # Mark seeds as tested
            tested_ids = [s[0] for s in batch]
            self.db.mark_seeds_tested(tested_ids, datetime.utcnow())
            
            # Mark successful hits
            for idx, (found, job_count) in enumerate(results):
                if found:
                    self.db.mark_seed_hit(batch[idx][0])
            
            self.stats.total_tested += len(batch)
            progress = (i + len(batch)) / len(seeds) * 100
            logger.info(f"📊 Progress: {progress:.1f}% | Found: {self.stats.greenhouse_found} GH, {self.stats.lever_found} LV | Jobs: {self.stats.total_jobs:,}")
        
        # Create monthly snapshot after discovery
        self.db.create_monthly_snapshot()
        
        await self._close_client()
        self._running = False
        self.stats.end_time = datetime.utcnow()
        
        logger.info(f"✅ Discovery complete! {self.stats.to_dict()}")
        return self.stats

    async def run_refresh(self, hours_since_update: int = 6, max_companies: int = 500) -> CollectorStats:
        """Refresh existing companies that haven't been updated recently."""
        self._running = True
        self.stats = CollectorStats()
        
        companies = self.db.get_companies_for_refresh(hours_since_update, max_companies)
        logger.info(f"Starting refresh on {len(companies)} companies...")
        
        batch_size = 50
        for i in range(0, len(companies), batch_size):
            if not self._running:
                logger.info("Refresh stopped by request.")
                break
            
            batch = companies[i:i + batch_size]
            tasks = [self._refresh_company(c) for c in batch]
            
            await asyncio.sleep(1)  # Rate limiting
            results = await asyncio.gather(*tasks)
            
            self.stats.refreshed += sum(1 for r in results if r)
            progress = (i + len(batch)) / len(companies) * 100
            logger.info(f"📊 Refresh Progress: {progress:.1f}% | Refreshed: {self.stats.refreshed} | Jobs: {self.stats.total_jobs:,}")
        
        await self._close_client()
        self._running = False
        self.stats.end_time = datetime.utcnow()
        
        logger.info(f"✅ Refresh complete! {self.stats.to_dict()}")
        return self.stats

    def stop(self):
        """Stop the collector gracefully."""
        self._running = False


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

async def run_collection(max_companies: int = None) -> CollectorStats:
    """Run a complete discovery cycle."""
    collector = JobIntelCollector()
    return await collector.run_discovery(max_companies=max_companies)


async def run_refresh(hours_since_update: int = 6, max_companies: int = 500) -> CollectorStats:
    """Run a refresh cycle on existing companies."""
    collector = JobIntelCollector()
    return await collector.run_refresh(hours_since_update=hours_since_update, max_companies=max_companies)


if __name__ == "__main__":
    import sys
    
    mode = sys.argv[1] if len(sys.argv) > 1 else "discover"
    max_companies = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    if mode == "refresh":
        stats = asyncio.run(run_refresh(max_companies=max_companies or 500))
    else:
        stats = asyncio.run(run_collection(max_companies=max_companies))
    
    print(json.dumps(stats.to_dict(), indent=2))
