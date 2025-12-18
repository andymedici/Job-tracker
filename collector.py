import asyncio
import aiohttp
import json
import logging
import os
import re
import time
import random
import hashlib 
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, field, asdict
from urllib.parse import urlparse, quote

from bs4 import BeautifulSoup 

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from database import get_db, Database


# ============================================================================
# TOKEN GENERATION HELPERS
# ============================================================================

def _name_to_token(name: str) -> str:
    """Converts a company name to a URL-friendly, lowercase ATS token/slug."""
    token = name.lower()
    token = re.sub(r'\s+(inc|llc|ltd|co|corp|gmbh|sa)\.?$', '', token, flags=re.IGNORECASE)
    token = re.sub(r'[^a-z0-9\s-]', '', token)
    token = re.sub(r'[\s-]+', '-', token).strip('-')
    return token

def _generate_career_url_variants(slug: str) -> List[str]:
    """Generates likely career page URLs for a given base slug."""
    domain_base = slug.replace('-', '').lower()
    if not domain_base:
        return []
        
    # Attempt to derive a .com domain (simplification)
    domain = f'{domain_base}.com'
    
    # Priority list of common job board subdomains/paths
    return [
        f'https://jobs.{domain}',
        f'https://careers.{domain}',
        f'https://www.{domain}/careers',
        f'https://www.{domain}/jobs',
        f'https://{domain}/careers',
        f'https://{domain}/jobs',
    ]

# ============================================================================
# INTELLIGENCE HELPERS (D & F)
# ============================================================================

# Simplified list of tech skills for extraction (D)
TECH_SKILLS = {
    'python': re.compile(r'\b(python|django|flask|celery)\b', re.IGNORECASE),
    'javascript': re.compile(r'\b(javascript|node(\.js)?|react|vue|angular|typescript)\b', re.IGNORECASE),
    'go': re.compile(r'\b(go|golang)\b', re.IGNORECASE),
    'java': re.compile(r'\b(java|spring|kotlin)\b', re.IGNORECASE),
    'cloud': re.compile(r'\b(aws|azure|gcp|terraform|kubernetes|docker)\b', re.IGNORECASE),
    'database': re.compile(r'\b(postgresql|mysql|mongodb|redis)\b', re.IGNORECASE),
    'ai_ml': re.compile(r'\b(ai|ml|machine\s*learning|deep\s*learning|pytorch|tensorflow)\b', re.IGNORECASE),
}

def _extract_skills(description_text: str) -> Dict[str, int]:
    """Extracts and counts mentions of predefined tech skills from text."""
    skills_count: Dict[str, int] = {}
    
    # Ensure text is lowercase to catch more matches
    text_lower = description_text.lower()
    
    for skill_name, pattern in TECH_SKILLS.items():
        count = len(pattern.findall(text_lower))
        if count > 0:
            skills_count[skill_name] = count
            
    # Return only the top 5 by count
    return dict(sorted(skills_count.items(), key=lambda item: item[1], reverse=True)[:5])

# Simple location normalization (F)
def _normalize_location(location_string: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Converts a free-text location string to structured (city, region, country)."""
    
    city, region, country = None, None, None
    location_lower = location_string.lower().strip()
    
    # Simple Country Extraction
    if re.search(r'\b(canada|can|ca)\b', location_lower):
        country = 'Canada'
    elif re.search(r'\b(united\s*states|usa|us)\b', location_lower):
        country = 'USA'
    elif re.search(r'\b(united\s*kingdom|uk|gb)\b', location_lower):
        country = 'UK'
    elif re.search(r'\b(germany|de)\b', location_lower):
        country = 'Germany'
    elif re.search(r'\b(remote|anywhere|global)\b', location_lower):
        # For remote, try to find a country in the string, default to Global
        country_match = re.search(r'\((us|usa|canada|uk|gb)\)', location_lower)
        country = country_match.group(1).upper() if country_match else 'Global'
        return None, None, country

    # City/Region Extraction (e.g. "San Francisco, CA" or "Berlin, Germany")
    city_region_match = re.search(r'([A-Za-z\s]+),\s*([A-Za-z]{2,})', location_string)
    if city_region_match:
        city = city_region_match.group(1).strip()
        region_or_country = city_region_match.group(2).strip()
        
        # If country wasn't set, try to infer from region
        if not country:
            if region_or_country.upper() in ['CA', 'TX', 'NY', 'MA']: country = 'USA'
            elif region_or_country.upper() in ['UK', 'DE', 'GB']: country = region_or_country.upper()

        # Simple assignment
        if len(region_or_country) <= 3:
            region = region_or_country # e.g. CA
        elif not country:
            country = region_or_country # e.g. Germany

    # Default country
    if not country and location_lower and 'remote' not in location_lower:
        country = 'Unknown' # Can be improved with an external API

    return city, region, country


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class JobBoard:
    """Represents the collected and processed data for a single job board."""
    ats_type: str
    token: str
    company_name: str
    job_count: int = 0
    remote_count: int = 0
    hybrid_count: int = 0
    onsite_count: int = 0
    locations: List[str] = field(default_factory=list) # Raw locations
    departments: List[str] = field(default_factory=list)
    
    # NEW FIELDS (F & D)
    normalized_locations: Dict[str, Dict[str, int]] = field(default_factory=dict) # {'city': {'SF': 10}, 'country': {'USA': 50}}
    extracted_skills: Dict[str, int] = field(default_factory=dict) # Top 5 skills/counts (D)
    
    # Updated Jobs structure to hold hash and metadata (B)
    jobs: List[Dict] = field(default_factory=list) # List of dicts, each containing job_hash and processed data
    
    source: str = ""
    discovered_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class CollectorStats:
    """Statistics for a collection run."""
    total_companies: int = 0
    total_jobs: int = 0
    greenhouse_found: int = 0
    lever_found: int = 0
    workday_found: int = 0 # NEW (A)
    ashby_found: int = 0    # NEW (A)
    jobvite_found: int = 0  # NEW (A)
    total_tested: int = 0
    refreshed: int = 0
    closed_jobs: int = 0 # NEW (B)
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
                headers={'User-Agent': 'JobIntelCollector/3.0'},
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

    async def _fetch(self, url: str, is_json=True) -> Optional[Any]:
        """Fetch a single URL with retries and backoff."""
        client = await self._get_client()
        attempt = 0
        while attempt < 3:
            try:
                async with self._semaphore:
                    async with client.get(url, timeout=15) as response:
                        if response.status == 200:
                            if is_json:
                                return await response.json()
                            else:
                                return await response.text()
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
        if any(kw in location_lower for kw in ['remote', 'anywhere', 'distributed', 'work from home', 'wfh', 'global']):
            return 'remote'
        elif any(kw in location_lower for kw in ['hybrid', 'flexible']):
            return 'hybrid'
        else:
            return 'onsite'

    def _process_job_data(self, 
                          job_data: Dict, 
                          title_key: str, 
                          location_key: str, 
                          dept_key: str, 
                          desc_key: Optional[str] = None
                          ) -> Tuple[Dict, str]:
        """
        Generic job data processor to extract info, hash, and normalize.
        Returns the processed job dictionary and its hash.
        (B, D, F)
        """
        title = job_data.get(title_key, 'N/A')
        location = job_data.get(location_key, 'Unknown')
        dept = job_data.get(dept_key, 'Unknown')
        description = job_data.get(desc_key, '') if desc_key else ''
        
        work_type = self._classify_work_type(location)
        city, region, country = _normalize_location(location)
        skills = _extract_skills(title + ' ' + description)
        
        # B: Hashing the job based on core content (title, raw location, and a description snippet)
        hash_input = f"{title}|{location}|{description[:200]}"
        job_hash = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
        
        processed_job = {
            'hash': job_hash,
            'title': title,
            'location_raw': location,
            'department': dept,
            'work_type': work_type,
            'city': city,
            'region': region,
            'country': country,
            'skills': list(skills.keys()), # Store top skills keys
            'skills_count': skills
        }
        return processed_job, job_hash

    # ============================================================================
    # ATS SPECIFIC FETCHING AND PROCESSING (A)
    # ============================================================================

    async def _fetch_greenhouse_jobs(self, token: str) -> Optional[List[Dict]]:
        url = f"https://boards-api.greenhouse.io/v1/boards/{quote(token)}/jobs?content=true"
        data = await self._fetch(url)
        if data and data.get('jobs'):
            processed_jobs = []
            for job in data['jobs']:
                dept_name = job.get('departments', [{}])[0].get('name', 'Unknown')
                
                processed_job, _ = self._process_job_data(
                    job, 
                    title_key='title',
                    location_key='location.name', 
                    dept_key=dept_name,
                    desc_key='content'
                )
                # Simple fix for location path, otherwise the location will be 'Unknown'
                location_name = job.get('location', {}).get('name', 'Unknown')
                processed_job['location_raw'] = location_name
                processed_job['city'], processed_job['region'], processed_job['country'] = _normalize_location(location_name)
                
                processed_jobs.append(processed_job)
            return processed_jobs
        return None

    async def _fetch_lever_jobs(self, token: str) -> Optional[List[Dict]]:
        url = f"https://api.lever.co/v0/postings/{quote(token)}?group=team&mode=json"
        data = await self._fetch(url)
        if isinstance(data, list) and data:
            processed_jobs = []
            for group in data:
                team = group.get('title', 'Unknown')
                for job in group.get('postings', []):
                    processed_job, _ = self._process_job_data(
                        job, 
                        title_key='text',
                        location_key='categories.location',
                        dept_key='categories.team',
                        desc_key='description'
                    )
                    processed_job['department'] = team 
                    processed_jobs.append(processed_job)
            return processed_jobs
        return None
        
    async def _fetch_workday_jobs(self, token: str) -> Optional[List[Dict]]:
        # A: Workday requires POST and uses custom fields
        url = f"https://{token}.wd5.myworkdayjobs.com/wday/c/jobsite/WdayService/GetJobPostings"
        headers = {
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
        }
        # Fetch up to 500 jobs, assuming Workday API pagination is complex
        payload = json.dumps({"limit": 500, "offset": 0, "searchText": "", "sortBy": "postedDate"})
        
        client = await self._get_client()
        try:
            async with self._semaphore:
                async with client.post(url, headers=headers, data=payload, timeout=15) as response:
                    if response.status == 200:
                        data = await response.json()
                        jobs = data.get('jobPostings', [])
                        processed_jobs = []
                        for job in jobs:
                            processed_job, _ = self._process_job_data(
                                job,
                                title_key='title',
                                location_key='locationsText',
                                dept_key='jobType',
                                desc_key='summary'
                            )
                            processed_jobs.append(processed_job)
                        return processed_jobs
        except Exception:
            logger.debug(f"Workday fetch failed for token {token}.")
            return None

    async def _fetch_ashby_jobs(self, token: str) -> Optional[List[Dict]]:
        # A: Ashby has a simple public JSON endpoint
        url = f"https://api.ashby.app/api/posting-board/{token}"
        data = await self._fetch(url)
        if data and data.get('jobs'):
            processed_jobs = []
            for job in data['jobs']:
                processed_job, _ = self._process_job_data(
                    job, 
                    title_key='title',
                    location_key='locationName',
                    dept_key='departmentName',
                    desc_key='descriptionPlain'
                )
                processed_jobs.append(processed_job)
            return processed_jobs
        return None

    async def _fetch_jobvite_jobs(self, token: str) -> Optional[List[Dict]]:
        # A: Jobvite has a public JSON endpoint
        url = f"https://jobs.jobvite.com/api/v2/boards/{token}/jobs"
        data = await self._fetch(url)
        if isinstance(data, list) and data:
            processed_jobs = []
            for job in data:
                processed_job, _ = self._process_job_data(
                    job, 
                    title_key='title',
                    location_key='location',
                    dept_key='category',
                    desc_key='jobDescription'
                )
                processed_jobs.append(processed_job)
            return processed_jobs
        return None
        
    async def _fetch_job_data_from_token(self, ats_type: str, token: str, company_name: str) -> Optional[JobBoard]:
        """Fetch, process, and archive job data using a confirmed ATS type and token."""
        
        job_fetchers = {
            'greenhouse': self._fetch_greenhouse_jobs,
            'lever': self._fetch_lever_jobs,
            'workday': self._fetch_workday_jobs,
            'ashby': self._fetch_ashby_jobs,
            'jobvite': self._fetch_jobvite_jobs,
        }
        
        processed_jobs = await job_fetchers.get(ats_type, lambda token: None)(token)
        
        if not processed_jobs:
            return None
        
        # Aggregation and normalization
        job_count = len(processed_jobs)
        remote_count = hybrid_count = onsite_count = 0
        raw_locations = set()
        departments = set()
        # F: Structure for normalized locations
        normalized_locations: Dict[str, Dict[str, int]] = {'city': {}, 'region': {}, 'country': {}}
        total_skills: Dict[str, int] = {}
        
        for job in processed_jobs:
            raw_locations.add(job['location_raw'])
            departments.add(job['department'])
            
            # Count by work type
            work_type = job['work_type']
            if work_type == 'remote': remote_count += 1
            elif work_type == 'hybrid': hybrid_count += 1
            else: onsite_count += 1
            
            # F: Count normalized locations
            if job['city']: normalized_locations['city'][job['city']] = normalized_locations['city'].get(job['city'], 0) + 1
            if job['region']: normalized_locations['region'][job['region']] = normalized_locations['region'].get(job['region'], 0) + 1
            if job['country']: normalized_locations['country'][job['country']] = normalized_locations['country'].get(job['country'], 0) + 1
            
            # D: Aggregate skills across all jobs
            for skill, count in job['skills_count'].items():
                 total_skills[skill] = total_skills.get(skill, 0) + count

        # B: Archive jobs (This call is critical for time-to-fill)
        try:
            self.db.archive_jobs(f"{ats_type}_{token}", processed_jobs)
        except Exception as e:
            logger.error(f"Failed to archive jobs for {company_name}: {e}")

        # D: Final top 5 skills for the company
        final_skills = dict(sorted(total_skills.items(), key=lambda item: item[1], reverse=True)[:5])

        return JobBoard(
            ats_type=ats_type,
            token=token,
            company_name=company_name,
            job_count=job_count,
            remote_count=remote_count,
            hybrid_count=hybrid_count,
            onsite_count=onsite_count,
            locations=list(raw_locations),
            departments=list(departments),
            normalized_locations=normalized_locations,
            extracted_skills=final_skills,
            jobs=processed_jobs,
            source='discovery_scrape'
        )

    async def _scrape_for_token_and_data(self, company_name: str, token_slug: str) -> Optional[JobBoard]:
        """
        Scrapes common career page URLs for ATS tokens (A: Workday, Ashby, Jobvite, Lever, Greenhouse)
        then uses the confirmed token to fetch job data.
        """
        urls_to_test = _generate_career_url_variants(token_slug)
        client = await self._get_client()

        for url in urls_to_test:
            try:
                async with self._semaphore:
                    async with client.get(url, timeout=15, allow_redirects=True) as response:
                        token = None
                        ats_type = None

                        final_url = str(response.url)
                        
                        # --- 1. Check for direct redirect to known ATS domain ---
                        if 'boards.greenhouse.io' in final_url:
                            ats_type = 'greenhouse'
                            token_match = re.search(r'boards\.greenhouse\.io/([^/?]+)', final_url)
                            token = token_match.group(1) if token_match else None
                        elif 'jobs.lever.co' in final_url:
                            ats_type = 'lever'
                            token_match = re.search(r'jobs\.lever\.co/([^/?]+)', final_url)
                            token = token_match.group(1) if token_match else None
                        elif 'jobs.ashby.app' in final_url: 
                            ats_type = 'ashby'
                            token_match = re.search(r'jobs\.ashby\.app/([^/?]+)', final_url)
                            token = token_match.group(1) if token_match else None
                        elif 'jobs.jobvite.com' in final_url: 
                            ats_type = 'jobvite'
                            token_match = re.search(r'jobs\.jobvite\.com/([^/?]+)', final_url)
                            token = token_match.group(1) if token_match else None
                            
                        # --- 2. Scrape HTML content for embedded tokens ---
                        if response.status == 200:
                            html_content = await response.text()
                            soup = BeautifulSoup(html_content, 'html.parser')

                            if not token and re.search(r'boards\.greenhouse\.io', html_content):
                                greenhouse_match = soup.find('script', src=re.compile(r'embed\.js\?for=([^"&]+)'))
                                if greenhouse_match:
                                    ats_type = 'greenhouse'
                                    token = re.search(r'for=([^"&]+)', greenhouse_match['src']).group(1)
                            
                            elif not token and re.search(r'lever\.co/embed', html_content):
                                lever_match = soup.find('script', id=re.compile(r'lever-sdk-js'))
                                if lever_match and lever_match.has_attr('data-lever-for'):
                                    ats_type = 'lever'
                                    token = lever_match['data-lever-for']
                            
                            # A: Workday pattern: search for the wdUrl token
                            elif not token and re.search(r'myworkdayjobs\.com', html_content):
                                workday_match = re.search(r'"wdUrl"\s*:\s*"([^"]+)"', html_content)
                                if workday_match:
                                    ats_type = 'workday'
                                    token = workday_match.group(1).split('.')[0].replace('https://', '')
                            
                            # A: Ashby pattern: search for the public JSON URL token
                            elif not token and re.search(r'ashby\.app', html_content):
                                ashby_match = re.search(r'/api/posting-board/([^"]+)', html_content)
                                if ashby_match:
                                    ats_type = 'ashby'
                                    token = ashby_match.group(1)
                            
                        if token and ats_type:
                            logger.info(f"✨ SCRAPE HIT: {company_name} found {ats_type} token: {token} via {url}")
                            
                            board = await self._fetch_job_data_from_token(ats_type, token, company_name)
                            
                            if board and board.job_count > 0:
                                return board
                            
            except Exception as e:
                logger.debug(f"Error scraping {url} for {company_name}: {e}")
                continue

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
            'normalized_locations': board.normalized_locations, # NEW (F)
            'extracted_skills': board.extracted_skills,         # NEW (D)
        }
        self.db.upsert_company(company_data)

    async def _test_company(self, seed_id: int, company_name: str, token_slug: str, source: str) -> Tuple[bool, int]:
        """Test a single company using web scraping to find the ATS token."""
        
        self.stats.total_tested += 1
        
        board = await self._scrape_for_token_and_data(company_name, token_slug)

        if board:
            self._save_company(board)
            # Update stats for new ATS types (A)
            if board.ats_type == 'greenhouse':
                self.stats.greenhouse_found += 1
            elif board.ats_type == 'lever':
                self.stats.lever_found += 1
            elif board.ats_type == 'workday':
                self.stats.workday_found += 1
            elif board.ats_type == 'ashby':
                self.stats.ashby_found += 1
            elif board.ats_type == 'jobvite':
                self.stats.jobvite_found += 1
            
            self.stats.total_companies += 1
            self.stats.total_jobs += board.job_count
            return True, board.job_count
        
        return False, 0

    async def _refresh_company(self, company: Dict) -> bool:
        """Refresh a single existing company's job data and archive old jobs (B)."""
        
        company_id = company['id']
        ats_type = company['ats_type']
        token = company['token']
        company_name = company['company_name']
        refresh_time = datetime.utcnow()
        
        # 1. Fetch, process, and archive new/updated job data
        board = await self._fetch_job_data_from_token(ats_type, token, company_name)

        if board:
            # 2. Save the newly collected aggregated data
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
                'normalized_locations': board.normalized_locations, # NEW (F)
                'extracted_skills': board.extracted_skills,         # NEW (D)
            }
            self.db.upsert_company(company_data)
            self.stats.total_jobs += board.job_count
            
            # 3. B: Mark stale jobs as closed and calculate time-to-fill
            closed_count = self.db.mark_stale_jobs_closed(company_id, refresh_time)
            self.stats.closed_jobs += closed_count
            
            return True
        
        return False

    # ========================================================================
    # MAIN ENTRY POINTS (batching/logging updated for new ATS/stats)
    # ========================================================================

    async def run_discovery(self, max_companies: Optional[int] = None) -> CollectorStats:
        """Run discovery loop for new companies from the seed database."""
        self._running = True
        self.stats = CollectorStats()
        
        seeds = self.db.get_seeds_for_collection(max_companies or 500)
        logger.info(f"Starting discovery on {len(seeds)} seeds...")
        
        batch_size = 25 # Reduced batch size due to more aggressive scraping/ATS checks
        for i in range(0, len(seeds), batch_size):
            if not self._running:
                logger.info("Collection stopped by request.")
                break
            
            batch = seeds[i:i + batch_size]
            tasks = []
            
            for seed_id, company_name, token_slug, source in batch:
                tasks.append(self._test_company(seed_id, company_name, token_slug, source))
            
            await asyncio.sleep(2)  # Increased sleep for rate limiting
            results = await asyncio.gather(*tasks)
            
            # Mark seeds as tested
            tested_ids = [s[0] for s in batch]
            self.db.mark_seeds_tested(tested_ids, datetime.utcnow())
            
            # Mark successful hits
            for idx, (found, job_count) in enumerate(results):
                if found:
                    self.db.mark_seed_hit(batch[idx][0])
            
            progress = (i + len(batch)) / len(seeds) * 100
            logger.info(f"📊 Progress: {progress:.1f}% | Found: GH:{self.stats.greenhouse_found}, LV:{self.stats.lever_found}, WD:{self.stats.workday_found}, AS:{self.stats.ashby_found}, JV:{self.stats.jobvite_found} | Tested: {self.stats.total_tested:,} | Jobs: {self.stats.total_jobs:,}")
        
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
        
        batch_size = 25 # Reduced batch size
        for i in range(0, len(companies), batch_size):
            if not self._running:
                logger.info("Refresh stopped by request.")
                break
            
            batch = companies[i:i + batch_size]
            tasks = [self._refresh_company(c) for c in batch]
            
            await asyncio.sleep(2)  # Increased sleep
            results = await asyncio.gather(*tasks)
            
            self.stats.refreshed += sum(1 for r in results if r)
            progress = (i + len(batch)) / len(companies) * 100
            logger.info(f"📊 Refresh Progress: {progress:.1f}% | Refreshed: {self.stats.refreshed} | Closed Jobs: {self.stats.closed_jobs:,} | Jobs: {self.stats.total_jobs:,}")
        
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
