// collector.py

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

# ADDED: Import BeautifulSoup for HTML parsing
from bs4 import BeautifulSoup 

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

# REMOVED: _generate_token_variants is no longer needed as we are not guessing tokens.

# ADDED: New helper to generate career URL variants
def _generate_career_url_variants(slug: str) -> List[str]:
    """Generates likely career page URLs for a given base slug."""
    # Assuming slug.tld is the company domain (e.g. apple-inc -> apple.com)
    # This is a simplification; a dedicated domain expander would be more robust.
    domain = slug.replace('-', '') + '.com'
    
    # Prioritize common job board subdomains/paths
    return [
        f'https://jobs.{domain}',
        f'https://careers.{domain}',
        f'https://www.{domain}/careers',
        f'https://www.{domain}/jobs',
        f'https://{domain}/careers',
        f'https://{domain}/jobs',
    ]


# ============================================================================
# DATA CLASSES
# (No changes)
# ============================================================================

@dataclass
class JobBoard:
# ... (contents remain the same) ...
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
# ... (contents remain the same) ...
    total_companies: int = 0
    total_jobs: int = 0
    greenhouse_found: int = 0
    lever_found: int = 0
    total_tested: int = 0
    refreshed: int = 0
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None

    def to_dict(self) -> Dict:
# ... (contents remain the same) ...
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
# ... (contents remain the same) ...
        self.db = db or get_db()
        self.stats = CollectorStats()
        self.client: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._semaphore = asyncio.Semaphore(10)  # Limit concurrent API requests

    async def _get_client(self) -> aiohttp.ClientSession:
# ... (contents remain the same) ...
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
# ... (contents remain the same) ...
        """Close HTTP client."""
        if self.client and not self.client.closed:
            await self.client.close()

    async def _exponential_backoff(self, attempt: int):
# ... (contents remain the same) ...
        """Implements rate-limiting backoff."""
        delay = min(2 ** attempt + random.random(), 60)
        logger.warning(f"Rate limiting hit. Waiting for {delay:.2f}s (Attempt {attempt})...")
        await asyncio.sleep(delay)

    async def _fetch(self, url: str) -> Optional[Dict]:
# ... (contents remain the same) ...
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
# ... (contents remain the same) ...
        """Classify a job location as remote, hybrid, or onsite."""
        location_lower = location.lower()
        if any(kw in location_lower for kw in ['remote', 'anywhere', 'distributed', 'work from home', 'wfh']):
            return 'remote'
        elif any(kw in location_lower for kw in ['hybrid', 'flexible']):
            return 'hybrid'
        else:
            return 'onsite'

    def _process_greenhouse_jobs(self, jobs: List[Dict]) -> Tuple[int, int, int, List[str], List[str]]:
# ... (contents remain the same) ...
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
# ... (contents remain the same) ...
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

    
    # ADDED: Refactored function to fetch structured data from a known token
    async def _fetch_job_data_from_token(self, ats_type: str, token: str, company_name: str) -> Optional[JobBoard]:
        """Fetch and process job data using a confirmed ATS type and token."""
        
        if ats_type == 'greenhouse':
            url = f"https://boards-api.greenhouse.io/v1/boards/{quote(token)}/jobs?content=true"
            data = await self._fetch(url)
            
            if data and data.get('jobs'):
                job_list = data.get('jobs', [])
                remote, hybrid, onsite, locations, departments = self._process_greenhouse_jobs(job_list)
                
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
                    source='discovery_scrape'
                )
                
        elif ats_type == 'lever':
            url = f"https://api.lever.co/v0/postings/{quote(token)}?group=team&mode=json"
            data = await self._fetch(url)
            
            if isinstance(data, list) and data:
                job_count, remote, hybrid, onsite, locations, departments = self._process_lever_jobs(data)
                
                if job_count > 0:
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
                        source='discovery_scrape'
                    )
        return None

    # REMOVED: check_greenhouse and check_lever methods (they performed the token-guessing which is the functionality being removed)

    # ADDED: New core discovery function
    async def _scrape_for_token_and_data(self, company_name: str, token_slug: str) -> Optional[JobBoard]:
        """
        Scrapes common career page URLs for ATS tokens (Lever/Greenhouse)
        then uses the confirmed token to fetch job data.
        """
        urls_to_test = _generate_career_url_variants(token_slug)
        client = await self._get_client()

        for url in urls_to_test:
            try:
                # Use HEAD request first if possible to catch redirects, then GET
                async with self._semaphore:
                    async with client.get(url, timeout=15, allow_redirects=True) as response:
                        token = None
                        ats_type = None

                        # 1. Check for direct redirect to ATS domain (most reliable)
                        final_url = str(response.url)
                        if 'boards.greenhouse.io' in final_url:
                            ats_type = 'greenhouse'
                            token_match = re.search(r'boards\.greenhouse\.io/([^/?]+)', final_url)
                            token = token_match.group(1) if token_match else None
                        elif 'jobs.lever.co' in final_url:
                            ats_type = 'lever'
                            token_match = re.search(r'jobs\.lever\.co/([^/?]+)', final_url)
                            token = token_match.group(1) if token_match else None
                        
                        # 2. Scrape HTML content for embedded tokens
                        if response.status == 200 and not token:
                            html_content = await response.text()
                            soup = BeautifulSoup(html_content, 'html.parser')

                            # Greenhouse pattern: search for embed.js script with token
                            greenhouse_match = soup.find('script', src=re.compile(r'embed\.js\?for=([^"&]+)'))
                            if greenhouse_match:
                                ats_type = 'greenhouse'
                                token = re.search(r'for=([^"&]+)', greenhouse_match['src']).group(1)
                            
                            # Lever pattern: search for script with data-lever-for attribute
                            elif soup.find('script', src=re.compile(r'lever\.co/embed/script')):
                                lever_match = soup.find('script', id=re.compile(r'lever-sdk-js'))
                                
                                if lever_match and lever_match.has_attr('data-lever-for'):
                                    ats_type = 'lever'
                                    token = lever_match['data-lever-for']
                                else:
                                    # Fallback: search for the posting API URL in the HTML
                                    lever_api_match = re.search(r'api\.lever\.co/v0/postings/([^/?]+)', html_content)
                                    if lever_api_match:
                                        ats_type = 'lever'
                                        token = lever_api_match.group(1)

                        if token:
                            logger.info(f"✨ SCRAPE HIT: {company_name} found {ats_type} token: {token} via {url}")
                            
                            # Use the confirmed token to fetch structured JSON data
                            board = await self._fetch_job_data_from_token(ats_type, token, company_name)
                            
                            if board and board.job_count > 0:
                                return board
                            
                        # If no token or no jobs found on this URL, continue to the next
                            
            except Exception as e:
                logger.debug(f"Error scraping {url} for {company_name}: {e}")
                continue

        return None


    def _save_company(self, board: JobBoard):
# ... (contents remain the same) ...
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
        """
        Test a single company using web scraping to find the ATS token.
        (Replaces the reverse ATS search/token guessing functionality)
        """
        
        # 1. Attempt to find the token via web scraping of career pages
        board = await self._scrape_for_token_and_data(company_name, token_slug)

        if board:
            self._save_company(board)
            if board.ats_type == 'greenhouse':
                self.stats.greenhouse_found += 1
            elif board.ats_type == 'lever':
                self.stats.lever_found += 1
            
            self.stats.total_companies += 1
            return True, board.job_count
        
        return False, 0

    async def _refresh_company(self, company: Dict) -> bool:
        """Refresh a single existing company's job data."""
        company_id = company['id']
        ats_type = company['ats_type']
        token = company['token']
        company_name = company['company_name']
        
        # Refactored to use the new function
        board = await self._fetch_job_data_from_token(ats_type, token, company_name)

        if board:
            company_data = {
                'id': company_id,
                'company_name': company_name,
                'ats_type': ats_type,
                'token': token,
                'job_count': board.job_count,
                'remote_count': board.remote_count,
                'hybrid_count': board.hybrid_count,
                'onsite_count': board.onsite_count,
                'locations': board.locations,
                'departments': board.departments,
            }
            self.db.upsert_company(company_data)
            self.stats.total_jobs += board.job_count
            return True
        
        return False

    # ========================================================================
    # MAIN ENTRY POINTS
    # (No changes to these entry points)
    # ========================================================================

    async def run_discovery(self, max_companies: Optional[int] = None) -> CollectorStats:
# ... (contents remain the same) ...
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
# ... (contents remain the same) ...
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
# ... (contents remain the same) ...
        """Stop the collector gracefully."""
        self._running = False


# ============================================================================
# CONVENIENCE FUNCTIONS
# ... (contents remain the same) ...
# ============================================================================

async def run_collection(max_companies: int = None) -> CollectorStats:
# ... (contents remain the same) ...
    """Run a complete discovery cycle."""
    collector = JobIntelCollector()
    return await collector.run_discovery(max_companies=max_companies)


async def run_refresh(hours_since_update: int = 6, max_companies: int = 500) -> CollectorStats:
# ... (contents remain the same) ...
    """Run a refresh cycle on existing companies."""
    collector = JobIntelCollector()
    return await collector.run_refresh(hours_since_update=hours_since_update, max_companies=max_companies)


if __name__ == "__main__":
# ... (contents remain the same) ...
    import sys
    
    mode = sys.argv[1] if len(sys.argv) > 1 else "discover"
    max_companies = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    if mode == "refresh":
        stats = asyncio.run(run_refresh(max_companies=max_companies or 500))
    else:
        stats = asyncio.run(run_collection(max_companies=max_companies))
    
    print(json.dumps(stats.to_dict(), indent=2))
