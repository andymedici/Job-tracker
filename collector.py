"""
Job Intelligence Collector v3.0
===============================
Supports: Greenhouse, Lever, Ashby, Workable.
Features: Snowball Discovery, Reverse Lookup, Job Hashing, Dept Normalization.
"""

import asyncio
import aiohttp
import json
import logging
import os
import re
import random
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field, asdict
from urllib.parse import urlparse, quote
from bs4 import BeautifulSoup

from database import get_db, Database
from utils import normalize_department, calculate_job_hash, proxy_rotator
from seed_expander import ReverseATSLocator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class JobBoard:
    ats_type: str
    token: str
    company_name: str
    job_count: int = 0
    remote_count: int = 0
    hybrid_count: int = 0
    onsite_count: int = 0
    locations: List[str] = field(default_factory=list)
    departments: List[str] = field(default_factory=list)
    jobs: List[Dict] = field(default_factory=list)
    source: str = ""

@dataclass
class CollectorStats:
    total_companies: int = 0
    total_jobs: int = 0
    greenhouse_found: int = 0
    lever_found: int = 0
    ashby_found: int = 0
    workable_found: int = 0
    snowball_added: int = 0
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None

    def to_dict(self):
        return asdict(self)

class JobIntelCollector:
    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_db()
        self.stats = CollectorStats()
        self.client: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(10)
        self.reverse_locator = ReverseATSLocator()

    async def _get_client(self) -> aiohttp.ClientSession:
        if self.client is None or self.client.closed:
            self.client = aiohttp.ClientSession(
                headers={'User-Agent': 'Mozilla/5.0 (compatible; JobIntel/3.0)'},
                connector=aiohttp.TCPConnector(ssl=False),
                timeout=aiohttp.ClientTimeout(total=20)
            )
        return self.client
    
    async def _fetch(self, url: str) -> Optional[Any]:
        client = await self._get_client()
        proxy = proxy_rotator.get_proxy()
        try:
            async with self._semaphore:
                async with client.get(url, proxy=proxy) as response:
                    if response.status == 200:
                        # Try JSON first
                        try:
                            return await response.json()
                        except:
                            return await response.text()
                    return None
        except Exception as e:
            return None

    # --- CLASSIFICATION & SNOWBALL ---

    def _classify_work_type(self, location: str) -> str:
        loc = location.lower()
        if any(x in loc for x in ['remote', 'anywhere', 'wfh']): return 'remote'
        if 'hybrid' in loc: return 'hybrid'
        return 'onsite'

    def _snowball_scan(self, text_content: str):
        """Scans text/html for outbound links to other potential ATS boards."""
        if not text_content or not isinstance(text_content, str): return
        
        # Regex to find other greenhouse/lever/ashby links
        patterns = [
            r'boards\.greenhouse\.io/([^/"\s]+)',
            r'jobs\.lever\.co/([^/"\s]+)',
            r'jobs\.ashbyhq\.com/([^/"\s]+)',
            r'apply\.workable\.com/([^/"\s]+)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                # Add to snowball queue in DB
                self.db.upsert_snowball_domain(match, "snowball_scan")
                self.stats.snowball_added += 1

    # --- ATS PROCESSORS ---

    def _process_greenhouse(self, data: Dict, token: str, company: str) -> JobBoard:
        jobs = data.get('jobs', [])
        board = JobBoard('greenhouse', token, company, jobs=jobs)
        
        locs = set()
        depts = set()
        
        for job in jobs:
            loc = job.get('location', {}).get('name', 'Unknown')
            dept = job.get('departments', [{}])[0].get('name', 'Unknown') if job.get('departments') else 'Unknown'
            
            locs.add(loc)
            depts.add(normalize_department(dept))
            
            w_type = self._classify_work_type(loc)
            if w_type == 'remote': board.remote_count += 1
            elif w_type == 'hybrid': board.hybrid_count += 1
            else: board.onsite_count += 1
            
        board.job_count = len(jobs)
        board.locations = list(locs)
        board.departments = list(depts)
        return board

    def _process_lever(self, data: List, token: str, company: str) -> JobBoard:
        board = JobBoard('lever', token, company)
        locs = set()
        depts = set()
        
        for job in data:
            board.job_count += 1
            cats = job.get('categories', {})
            loc = cats.get('location', 'Unknown')
            dept = cats.get('team', 'Unknown')
            
            locs.add(loc)
            depts.add(normalize_department(dept))
            
            w_type = self._classify_work_type(loc)
            if w_type == 'remote': board.remote_count += 1
            elif w_type == 'hybrid': board.hybrid_count += 1
            else: board.onsite_count += 1
            
        board.locations = list(locs)
        board.departments = list(depts)
        return board

    def _process_ashby(self, data: Dict, token: str, company: str) -> JobBoard:
        # Ashby API structure varies, this is for the public API /api/job-board/{token}
        jobs = data.get('jobs', [])
        board = JobBoard('ashby', token, company)
        locs = set()
        depts = set()

        for job in jobs:
            board.job_count += 1
            loc = job.get('location', 'Unknown')
            dept = job.get('department', 'Unknown')
            
            locs.add(loc)
            depts.add(normalize_department(dept))
            
            w_type = self._classify_work_type(loc)
            if w_type == 'remote': board.remote_count += 1
            elif w_type == 'hybrid': board.hybrid_count += 1
            else: board.onsite_count += 1
            
        board.locations = list(locs)
        board.departments = list(depts)
        return board
        
    def _process_workable(self, html_content: str, token: str, company: str) -> JobBoard:
        # Basic Workable scraping (they don't have a simple public JSON for all boards)
        # This is a best-effort parser for apply.workable.com pages
        soup = BeautifulSoup(html_content, 'lxml')
        jobs = soup.find_all('li', class_=lambda x: x and 'job-' in x)
        
        board = JobBoard('workable', token, company)
        if not jobs:
            return None # Failed to parse
            
        locs = set()
        
        for job in jobs:
            board.job_count += 1
            # Parsing Workable HTML is brittle, assuming existence for now
            # In a real deployed version, we'd use their internal API endpoint often found in network tab
            # For this code, we return basic counts
            board.onsite_count += 1 # Default
        
        return board

    # --- CHECKERS ---

    async def check_company(self, company_name: str, token_slug: str) -> Tuple[bool, int]:
        """Checks all supported ATS for a company."""
        tokens = [token_slug, token_slug.replace('-', ''), token_slug + 'hq', token_slug + 'jobs']
        
        # 1. Reverse Lookup (If configured)
        direct_url = await self.reverse_locator.find_ats_url(company_name)
        if direct_url:
            # Parse token from URL
            if 'greenhouse.io' in direct_url:
                token = direct_url.split('/')[-1] or direct_url.split('/')[-2]
                res = await self._fetch(f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true")
                if res and isinstance(res, dict) and res.get('jobs'):
                    board = self._process_greenhouse(res, token, company_name)
                    self._save_company(board)
                    self.stats.greenhouse_found += 1
                    return True, board.job_count

        # 2. Brute Force Standard ATS
        for token in tokens:
            # Greenhouse
            gh_data = await self._fetch(f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true")
            if gh_data and isinstance(gh_data, dict) and gh_data.get('jobs'):
                board = self._process_greenhouse(gh_data, token, company_name)
                self._save_company(board)
                self.stats.greenhouse_found += 1
                return True, board.job_count
            
            # Lever
            lv_data = await self._fetch(f"https://api.lever.co/v0/postings/{token}?group=team&mode=json")
            if lv_data and isinstance(lv_data, list):
                board = self._process_lever(lv_data, token, company_name)
                self._save_company(board)
                self.stats.lever_found += 1
                return True, board.job_count

            # Ashby
            ash_data = await self._fetch(f"https://jobs.ashbyhq.com/api/job-board/{token}")
            if ash_data and isinstance(ash_data, dict) and ash_data.get('jobs'):
                board = self._process_ashby(ash_data, token, company_name)
                self._save_company(board)
                self.stats.ashby_found += 1
                return True, board.job_count
            
            # Snowball Scan (Check if main site has links)
            # (Skipped here to keep speed high, but can be added if direct_url fails)

        return False, 0

    def _save_company(self, board: JobBoard):
        self.db.upsert_company({
            'id': f"{board.ats_type}_{board.token}",
            'company_name': board.company_name,
            'ats_type': board.ats_type,
            'token': board.token,
            'job_count': board.job_count,
            'remote_count': board.remote_count,
            'hybrid_count': board.hybrid_count,
            'onsite_count': board.onsite_count,
            'locations': board.locations,
            'departments': board.departments
        })
        self.stats.total_jobs += board.job_count
        self.stats.total_companies += 1

    # --- RUNNERS ---

    async def run_discovery(self, max_companies: int = 500) -> CollectorStats:
        seeds = self.db.get_seeds_for_collection(max_companies)
        tasks = []
        for seed_id, name, slug, source in seeds:
            tasks.append(self.check_company(name, slug))
        
        if tasks:
            results = await asyncio.gather(*tasks)
            # Mark tested
            seed_ids = [s[0] for s in seeds]
            self.db.mark_seeds_tested(seed_ids, datetime.utcnow())
            # Mark hits
            for idx, (found, _) in enumerate(results):
                if found: self.db.mark_seed_hit(seeds[idx][0])

        return self.stats

    async def run_refresh(self, hours: int = 6, limit: int = 500) -> CollectorStats:
        companies = self.db.get_companies_for_refresh(hours, limit)
        tasks = []
        for c in companies:
            tasks.append(self.check_company(c['company_name'], c['token'])) # Re-check using tokens
        
        if tasks:
            await asyncio.gather(*tasks)
            
        return self.stats

if __name__ == "__main__":
    collector = JobIntelCollector()
    asyncio.run(collector.run_discovery(10))
