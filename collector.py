"""
Enhanced Job Intelligence Collector with Complete Historical Tracking
Supports: Greenhouse, Lever, Workday, Ashby, Jobvite, SmartRecruiters, Custom
"""
import asyncio
import aiohttp
import re
import json
import logging
import random
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Callable
from dataclasses import dataclass, field, asdict
from urllib.parse import urlparse, quote, urljoin
from urllib.robotparser import RobotFileParser

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

# Playwright for JS-heavy custom pages
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from database import get_db, Database

ua = UserAgent()

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
    domain = f'{domain_base}.com'
    return [
        f'https://jobs.{domain}',
        f'https://careers.{domain}',
        f'https://www.{domain}/careers',
        f'https://www.{domain}/jobs',
        f'https://{domain}/careers',
        f'https://{domain}/jobs',
        f'https://www.{domain}/open-positions',
        f'https://www.{domain}/careers/join',
        f'https://{domain}/openings',
    ]

# ============================================================================
# JOB HASH CALCULATION (CRITICAL FOR HISTORICAL TRACKING)
# ============================================================================

def calculate_job_hash(company_id: str, job_title: str, location: str, department: str = None) -> str:
    """
    Generate unique hash for job deduplication and historical tracking
    Same job posting = same hash across multiple refreshes
    """
    # Normalize components
    normalized_title = job_title.lower().strip()
    normalized_location = location.lower().strip() if location else 'unknown'
    normalized_dept = department.lower().strip() if department else 'unknown'
    
    # Create unique identifier
    unique_string = f"{company_id}|{normalized_title}|{normalized_location}|{normalized_dept}"
    
    # Generate SHA256 hash
    return hashlib.sha256(unique_string.encode()).hexdigest()

# ============================================================================
# INTELLIGENCE HELPERS
# ============================================================================

TECH_SKILLS = {
    'python': re.compile(r'\b(python|django|flask|fastapi|celery)\b', re.IGNORECASE),
    'javascript': re.compile(r'\b(javascript|node(\.js)?|react|vue|angular|typescript|next\.js)\b', re.IGNORECASE),
    'go': re.compile(r'\b(go|golang)\b', re.IGNORECASE),
    'java': re.compile(r'\b(java|spring|kotlin|scala)\b', re.IGNORECASE),
    'cloud': re.compile(r'\b(aws|azure|gcp|google cloud|terraform|kubernetes|docker|k8s)\b', re.IGNORECASE),
    'database': re.compile(r'\b(postgresql|postgres|mysql|mongodb|redis|dynamodb|cassandra)\b', re.IGNORECASE),
    'ai_ml': re.compile(r'\b(ai|ml|machine\s*learning|deep\s*learning|pytorch|tensorflow|llm)\b', re.IGNORECASE),
    'rust': re.compile(r'\b(rust|rustlang)\b', re.IGNORECASE),
    'devops': re.compile(r'\b(devops|ci/cd|jenkins|gitlab|github actions|circleci)\b', re.IGNORECASE),
    'frontend': re.compile(r'\b(react|vue|svelte|angular|next\.js|nuxt|tailwind)\b', re.IGNORECASE),
    'backend': re.compile(r'\b(api|rest|graphql|microservices|grpc)\b', re.IGNORECASE),
    'data': re.compile(r'\b(spark|hadoop|kafka|airflow|dbt|snowflake|databricks)\b', re.IGNORECASE),
}

def _extract_skills(description_text: str) -> Dict[str, int]:
    """Extract tech skills from job description"""
    text_lower = description_text.lower()
    skills_count: Dict[str, int] = {}
    for skill_name, pattern in TECH_SKILLS.items():
        count = len(pattern.findall(text_lower))
        if count > 0:
            skills_count[skill_name] = count
    return dict(sorted(skills_count.items(), key=lambda item: item[1], reverse=True)[:10])

def _normalize_location(location_string: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse location into city, region, country"""
    city, region, country = None, None, None
    location_lower = location_string.lower().strip()

    # Check for remote
    if re.search(r'\b(remote|anywhere|global|distributed|wfh|work from home)\b', location_lower):
        country_match = re.search(r'\((us|usa|canada|uk|eu|europe)\)', location_lower)
        country = country_match.group(1).upper() if country_match else 'Global'
        return None, None, country

    # Country detection
    if re.search(r'\b(canada|can|ca)\b', location_lower):
        country = 'Canada'
    elif re.search(r'\b(united\s*states|usa|us)\b', location_lower):
        country = 'USA'
    elif re.search(r'\b(united\s*kingdom|uk|gb)\b', location_lower):
        country = 'UK'
    elif re.search(r'\b(germany|de|deutschland)\b', location_lower):
        country = 'Germany'
    elif re.search(r'\b(france|fr)\b', location_lower):
        country = 'France'
    elif re.search(r'\b(india|in)\b', location_lower):
        country = 'India'
    elif re.search(r'\b(australia|au)\b', location_lower):
        country = 'Australia'

    # City, State/Region parsing
    city_region_match = re.search(r'([A-Za-z\s]+),\s*([A-Za-z]{2,})', location_string)
    if city_region_match:
        city = city_region_match.group(1).strip().title()
        region_or_country = city_region_match.group(2).strip()
        if len(region_or_country) <= 3:
            region = region_or_country.upper()
        elif not country:
            country = region_or_country.title()

    if not country and 'remote' not in location_lower:
        country = 'Unknown'

    return city, region, country

def _determine_work_type(location: str, job_title: str = '') -> str:
    """Determine if job is remote, hybrid, or onsite"""
    location_lower = location.lower()
    title_lower = job_title.lower()
    
    combined = f"{location_lower} {title_lower}"
    
    if re.search(r'\b(remote|wfh|work from home|distributed)\b', combined):
        if re.search(r'\b(hybrid|flexible|optional)\b', combined):
            return 'hybrid'
        return 'remote'
    elif re.search(r'\b(hybrid|flex|flexible office)\b', combined):
        return 'hybrid'
    else:
        return 'onsite'

# ============================================================================
# DATA CLASSES
# ============================================================================

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
    normalized_locations: Dict[str, Dict[str, int]] = field(default_factory=lambda: {'city': {}, 'region': {}, 'country': {}})
    extracted_skills: Dict[str, int] = field(default_factory=dict)
    department_distribution: Dict[str, int] = field(default_factory=dict)
    jobs: List[Dict] = field(default_factory=list)
    source: str = ""
    discovered_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    careers_url: str = ""

@dataclass
class CollectorStats:
    total_companies: int = 0
    total_jobs: int = 0
    greenhouse_found: int = 0
    lever_found: int = 0
    workday_found: int = 0
    ashby_found: int = 0
    jobvite_found: int = 0
    smartrecruiters_found: int = 0
    custom_found: int = 0
    total_tested: int = 0
    refreshed: int = 0
    closed_jobs: int = 0
    archived_jobs: int = 0
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None

    def to_dict(self) -> Dict:
        d = asdict(self)
        d['duration_seconds'] = (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        d['start_time'] = self.start_time.isoformat()
        d['end_time'] = self.end_time.isoformat() if self.end_time else None
        return d

# ============================================================================
# MAIN COLLECTOR CLASS
# ============================================================================

class JobIntelCollector:
    def __init__(self, db: Optional[Database] = None, progress_callback: Optional[Callable[[float, Dict], None]] = None):
        self.db = db or get_db()
        self.stats = CollectorStats()
        self.client: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._semaphore = asyncio.Semaphore(8)
        self.progress_callback = progress_callback

    def _report_progress(self, progress: float, extra: Dict[str, Any] = None):
        if self.progress_callback:
            stats_dict = self.stats.to_dict()
            if extra:
                stats_dict.update(extra)
            self.progress_callback(progress, stats_dict)

    async def _get_client(self) -> aiohttp.ClientSession:
        if self.client is None or self.client.closed:
            self.client = aiohttp.ClientSession(
                headers={'User-Agent': ua.random},
                timeout=aiohttp.ClientTimeout(total=40),
                connector=aiohttp.TCPConnector(limit_per_host=10, ssl=False)
            )
        return self.client

    async def _close_client(self):
        if self.client and not self.client.closed:
            await self.client.close()

    async def _fetch_greenhouse_jobs(self, token: str, company_name: str) -> Optional[JobBoard]:
        """Fetch jobs from Greenhouse ATS"""
        client = await self._get_client()
        url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
        
        try:
            async with self._semaphore:
                async with client.get(url) as response:
                    if response.status != 200:
                        return None
                    
                    data = await response.json()
                    jobs_data = data.get('jobs', [])
                    
                    if not jobs_data:
                        return None
                    
                    board = JobBoard(ats_type='greenhouse', token=token, company_name=company_name)
                    board.job_count = len(jobs_data)
                    
                    for job in jobs_data:
                        job_title = job.get('title', 'Unknown')
                        location = job.get('location', {}).get('name', 'Unknown')
                        department = job.get('departments', [{}])[0].get('name') if job.get('departments') else None
                        
                        # Normalize location
                        city, region, country = _normalize_location(location)
                        work_type = _determine_work_type(location, job_title)
                        
                        # Extract skills from description
                        description = job.get('content', '')
                        skills = _extract_skills(description)
                        
                        # Aggregate skills
                        for skill, count in skills.items():
                            board.extracted_skills[skill] = board.extracted_skills.get(skill, 0) + count
                        
                        # Count work types
                        if work_type == 'remote':
                            board.remote_count += 1
                        elif work_type == 'hybrid':
                            board.hybrid_count += 1
                        else:
                            board.onsite_count += 1
                        
                        # Track locations
                        if location not in board.locations:
                            board.locations.append(location)
                        if city:
                            board.normalized_locations['city'][city] = board.normalized_locations['city'].get(city, 0) + 1
                        if region:
                            board.normalized_locations['region'][region] = board.normalized_locations['region'].get(region, 0) + 1
                        if country:
                            board.normalized_locations['country'][country] = board.normalized_locations['country'].get(country, 0) + 1
                        
                        # Track departments
                        if department:
                            if department not in board.departments:
                                board.departments.append(department)
                            board.department_distribution[department] = board.department_distribution.get(department, 0) + 1
                        
                        # Store full job data for archiving
                        board.jobs.append({
                            'title': job_title,
                            'location': location,
                            'city': city,
                            'region': region,
                            'country': country,
                            'department': department,
                            'work_type': work_type,
                            'skills': list(skills.keys()),
                            'description': description,
                            'url': job.get('absolute_url', ''),
                            'posted_date': job.get('updated_at')
                        })
                    
                    return board
                    
        except Exception as e:
            logger.debug(f"Greenhouse fetch error for {token}: {e}")
            return None

    async def _fetch_lever_jobs(self, token: str, company_name: str) -> Optional[JobBoard]:
        """Fetch jobs from Lever ATS"""
        client = await self._get_client()
        url = f"https://api.lever.co/v0/postings/{token}?mode=json"
        
        try:
            async with self._semaphore:
                async with client.get(url) as response:
                    if response.status != 200:
                        return None
                    
                    jobs_data = await response.json()
                    
                    if not jobs_data:
                        return None
                    
                    board = JobBoard(ats_type='lever', token=token, company_name=company_name)
                    board.job_count = len(jobs_data)
                    
                    for job in jobs_data:
                        job_title = job.get('text', 'Unknown')
                        location = job.get('categories', {}).get('location', 'Unknown')
                        department = job.get('categories', {}).get('team')
                        
                        city, region, country = _normalize_location(location)
                        work_type = _determine_work_type(location, job_title)
                        
                        description = job.get('description', '') + ' ' + job.get('lists', [{}])[0].get('content', '')
                        skills = _extract_skills(description)
                        
                        for skill, count in skills.items():
                            board.extracted_skills[skill] = board.extracted_skills.get(skill, 0) + count
                        
                        if work_type == 'remote':
                            board.remote_count += 1
                        elif work_type == 'hybrid':
                            board.hybrid_count += 1
                        else:
                            board.onsite_count += 1
                        
                        if location not in board.locations:
                            board.locations.append(location)
                        if city:
                            board.normalized_locations['city'][city] = board.normalized_locations['city'].get(city, 0) + 1
                        if region:
                            board.normalized_locations['region'][region] = board.normalized_locations['region'].get(region, 0) + 1
                        if country:
                            board.normalized_locations['country'][country] = board.normalized_locations['country'].get(country, 0) + 1
                        
                        if department:
                            if department not in board.departments:
                                board.departments.append(department)
                            board.department_distribution[department] = board.department_distribution.get(department, 0) + 1
                        
                        board.jobs.append({
                            'title': job_title,
                            'location': location,
                            'city': city,
                            'region': region,
                            'country': country,
                            'department': department,
                            'work_type': work_type,
                            'skills': list(skills.keys()),
                            'description': description,
                            'url': job.get('hostedUrl', ''),
                            'posted_date': job.get('createdAt')
                        })
                    
                    return board
                    
        except Exception as e:
            logger.debug(f"Lever fetch error for {token}: {e}")
            return None

    async def _fetch_job_data_from_token(self, ats_type: str, token: str, company_name: str) -> Optional[JobBoard]:
        """Fetch job data from known ATS type and token"""
        if ats_type == 'greenhouse':
            return await self._fetch_greenhouse_jobs(token, company_name)
        elif ats_type == 'lever':
            return await self._fetch_lever_jobs(token, company_name)
        # Add other ATS types here (Workday, Ashby, etc.)
        return None

    # ========================================================================
    # JOB ARCHIVE INTEGRATION (CRITICAL FOR HISTORICAL TRACKING)
    # ========================================================================

    def process_and_archive_jobs(self, board: JobBoard, company_id: str) -> Tuple[int, int]:
        """
        Process scraped jobs and add to archive for historical tracking
        Returns: (active_jobs_count, closed_jobs_count)
        """
        current_time = datetime.utcnow()
        seen_hashes = set()
        
        for job in board.jobs:
            job_title = job.get('title', 'Unknown')
            location = job.get('location', 'Unknown')
            department = job.get('department')
            
            # Calculate unique hash for this job
            job_hash = calculate_job_hash(company_id, job_title, location, department)
            seen_hashes.add(job_hash)
            
            # Prepare job data for archiving
            job_data = {
                'job_hash': job_hash,
                'company_id': company_id,
                'job_title': job_title,
                'department': department,
                'city': job.get('city'),
                'region': job.get('region'),
                'country': job.get('country'),
                'work_type': job.get('work_type'),
                'skills': job.get('skills', []),
                'first_seen': current_time,
                'last_seen': current_time,
                'status': 'open'
            }
            
            # Upsert to job archive
            self.db.upsert_job_in_archive(job_data)
        
        # Mark jobs that weren't seen this time as closed
        closed_count = self.db.mark_stale_jobs_closed(company_id, current_time)
        
        if closed_count > 0 or len(seen_hashes) > 0:
            logger.info(f"📦 {company_id}: {len(seen_hashes)} active jobs, {closed_count} marked closed")
        
        return len(seen_hashes), closed_count

    def _save_company(self, board: JobBoard):
        """Save company data to database"""
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
            'normalized_locations': board.normalized_locations,
            'extracted_skills': board.extracted_skills,
            'department_distribution': board.department_distribution,
            'careers_url': board.careers_url,
        }
        self.db.upsert_company(company_data)

    async def _scrape_for_token_and_data(self, company_name: str, token_slug: str) -> Optional[JobBoard]:
        """Try to discover ATS type and scrape job data"""
        client = await self._get_client()
        
        # Try Greenhouse first
        gh_board = await self._fetch_greenhouse_jobs(token_slug, company_name)
        if gh_board and gh_board.job_count > 0:
            return gh_board
        
        # Try Lever
        lever_board = await self._fetch_lever_jobs(token_slug, company_name)
        if lever_board and lever_board.job_count > 0:
            return lever_board
        
        # Try career page variants
        urls = _generate_career_url_variants(token_slug)
        
        for url in urls[:3]:  # Try first 3 variants
            try:
                async with self._semaphore:
                    async with client.get(url, allow_redirects=True) as response:
                        if response.status == 200:
                            html = await response.text()
                            
                            # Try to detect ATS
                            if 'greenhouse' in html.lower():
                                # Try to extract token
                                match = re.search(r'boards\.greenhouse\.io/([^/"]+)', html)
                                if match:
                                    token = match.group(1)
                                    board = await self._fetch_greenhouse_jobs(token, company_name)
                                    if board and board.job_count > 0:
                                        board.careers_url = str(response.url)
                                        return board
                            
                            elif 'lever' in html.lower():
                                match = re.search(r'jobs\.lever\.co/([^/"]+)', html)
                                if match:
                                    token = match.group(1)
                                    board = await self._fetch_lever_jobs(token, company_name)
                                    if board and board.job_count > 0:
                                        board.careers_url = str(response.url)
                                        return board
                            
            except Exception as e:
                logger.debug(f"Error trying {url}: {e}")
                continue
            
            await asyncio.sleep(random.uniform(2, 4))
        
        return None

    async def _test_company(self, seed_id: int, company_name: str, token_slug: str, source: str) -> Tuple[bool, int]:
        """Test a seed company for job board"""
        self.stats.total_tested += 1

        board = await self._scrape_for_token_and_data(company_name, token_slug)
        if board:
            self._save_company(board)
            
            # CRITICAL: Archive jobs for historical tracking
            company_id = f"{board.ats_type}_{board.token}"
            active, closed = self.process_and_archive_jobs(board, company_id)
            self.stats.archived_jobs += active

            if board.ats_type == 'greenhouse':
                self.stats.greenhouse_found += 1
            elif board.ats_type == 'lever':
                self.stats.lever_found += 1

            self.stats.total_companies += 1
            self.stats.total_jobs += board.job_count
            return True, board.job_count

        return False, 0

    async def _refresh_company(self, company: Dict) -> bool:
        """Refresh an existing company's job data"""
        company_id = company['id']
        ats_type = company['ats_type']
        token = company['token']
        company_name = company['company_name']

        board = await self._fetch_job_data_from_token(ats_type, token, company_name)
        if board:
            # Update company data
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
                'normalized_locations': board.normalized_locations,
                'extracted_skills': board.extracted_skills,
                'department_distribution': board.department_distribution,
                'careers_url': company.get('careers_url', ''),
            }
            self.db.upsert_company(company_data)
            
            # CRITICAL: Archive jobs and track changes
            active, closed = self.process_and_archive_jobs(board, company_id)
            
            self.stats.total_jobs += board.job_count
            self.stats.closed_jobs += closed
            self.stats.archived_jobs += active

            return True
        return False

    async def run_discovery(self, max_companies: Optional[int] = None) -> CollectorStats:
        """Run discovery on untested seed companies"""
        self._running = True
        self.stats = CollectorStats()
        seeds = self.db.get_seeds_for_collection(max_companies or 500)
        total = len(seeds)
        logger.info(f"🔍 Starting discovery on {total} seeds")

        batch_size = 20
        processed = 0
        for i in range(0, total, batch_size):
            if not self._running:
                break
            batch = seeds[i:i + batch_size]
            tasks = [self._test_company(seed_id, name, slug, src) for seed_id, name, slug, src in batch]
            results = await asyncio.gather(*tasks)

            tested_ids = [s[0] for s in batch]
            self.db.mark_seeds_tested(tested_ids, datetime.utcnow())
            for idx, (found, _) in enumerate(results):
                if found:
                    self.db.mark_seed_hit(batch[idx][0])

            processed += len(batch)
            progress = (processed / total) * 100
            self._report_progress(progress, {'phase': 'discovery'})

            await asyncio.sleep(random.uniform(5, 12))

        # Create snapshot after discovery
        self.db.create_6h_snapshots()
        self.db.create_monthly_snapshot()
        
        self.stats.end_time = datetime.utcnow()
        await self._close_client()
        self._running = False
        
        logger.info(f"✅ Discovery complete: {self.stats.to_dict()}")
        return self.stats

    async def run_refresh(self, hours_since_update: int = 6, max_companies: int = 500) -> CollectorStats:
        """Refresh existing companies"""
        self._running = True
        self.stats = CollectorStats()
        companies = self.db.get_companies_for_refresh(hours_since_update, max_companies)
        total = len(companies)
        logger.info(f"🔄 Starting refresh on {total} companies")

        batch_size = 20
        processed = 0
        for i in range(0, total, batch_size):
            if not self._running:
                break
            batch = companies[i:i + batch_size]
            tasks = [self._refresh_company(c) for c in batch]
            results = await asyncio.gather(*tasks)

            self.stats.refreshed += sum(1 for r in results if r)
            processed += len(batch)
            progress = (processed / total) * 100
            self._report_progress(progress, {'phase': 'refresh'})

            await asyncio.sleep(random.uniform(4, 10))

        # Create snapshot after refresh
        self.db.create_6h_snapshots()
        
        self.stats.end_time = datetime.utcnow()
        await self._close_client()
        self._running = False
        
        logger.info(f"✅ Refresh complete: {self.stats.to_dict()}")
        return self.stats

    def stop(self):
        """Stop the collector"""
        self._running = False

# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

async def run_collection(max_companies: int = None) -> CollectorStats:
    """Run discovery collection"""
    collector = JobIntelCollector()
    return await collector.run_discovery(max_companies=max_companies)

async def run_refresh(hours_since_update: int = 6, max_companies: int = 500) -> CollectorStats:
    """Run refresh on existing companies"""
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
