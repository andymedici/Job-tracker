import asyncio
import aiohttp
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
# INTELLIGENCE HELPERS
# ============================================================================

TECH_SKILLS = {
    'python': re.compile(r'\b(python|django|flask|celery)\b', re.IGNORECASE),
    'javascript': re.compile(r'\b(javascript|node(\.js)?|react|vue|angular|typescript)\b', re.IGNORECASE),
    'go': re.compile(r'\b(go|golang)\b', re.IGNORECASE),
    'java': re.compile(r'\b(java|spring|kotlin)\b', re.IGNORECASE),
    'cloud': re.compile(r'\b(aws|azure|gcp|terraform|kubernetes|docker)\b', re.IGNORECASE),
    'database': re.compile(r'\b(postgresql|mysql|mongodb|redis)\b', re.IGNORECASE),
    'ai_ml': re.compile(r'\b(ai|ml|machine\s*learning|deep\s*learning|pytorch|tensorflow)\b', re.IGNORECASE),
    'rust': re.compile(r'\b(rust|rustlang)\b', re.IGNORECASE),
    'devops': re.compile(r'\b(devops|ci/cd|jenkins|gitlab|github actions)\b', re.IGNORECASE),
    'frontend': re.compile(r'\b(react|vue|svelte|angular|next\.js|nuxt)\b', re.IGNORECASE),
}

def _extract_skills(description_text: str) -> Dict[str, int]:
    text_lower = description_text.lower()
    skills_count: Dict[str, int] = {}
    for skill_name, pattern in TECH_SKILLS.items():
        count = len(pattern.findall(text_lower))
        if count > 0:
            skills_count[skill_name] = count
    return dict(sorted(skills_count.items(), key=lambda item: item[1], reverse=True)[:5])

def _normalize_location(location_string: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    city, region, country = None, None, None
    location_lower = location_string.lower().strip()

    if re.search(r'\b(remote|anywhere|global|distributed|wfh)\b', location_lower):
        country_match = re.search(r'\((us|usa|canada|uk|eu)\)', location_lower)
        country = country_match.group(1).upper() if country_match else 'Global'
        return None, None, country

    if re.search(r'\b(canada|can|ca)\b', location_lower):
        country = 'Canada'
    elif re.search(r'\b(united\s*states|usa|us)\b', location_lower):
        country = 'USA'
    elif re.search(r'\b(united\s*kingdom|uk|gb)\b', location_lower):
        country = 'UK'
    elif re.search(r'\b(germany|de)\b', location_lower):
        country = 'Germany'

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

    async def _allowed_by_robots(self, url: str) -> bool:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        try:
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            return rp.can_fetch('*', url)
        except Exception:
            return True  # Default allow if can't fetch robots.txt

    async def _fetch(self, url: str, is_json=True) -> Optional[Any]:
        if not await self._allowed_by_robots(url):
            logger.debug(f"Blocked by robots.txt: {url}")
            return None

        client = await self._get_client()
        attempt = 0
        while attempt < 4:
            try:
                async with self._semaphore:
                    async with client.get(url, timeout=20) as response:
                        if response.status == 200:
                            return await response.json() if is_json else await response.text()
                        elif response.status in (429, 503):
                            await asyncio.sleep(min(2 ** attempt + random.random(), 60))
                            attempt += 1
                        else:
                            return None
            except Exception as e:
                logger.debug(f"Fetch error {url}: {e}")
                attempt += 1
                await asyncio.sleep(2 + random.random())
        return None

    def _classify_work_type(self, location: str) -> str:
        location_lower = location.lower()
        if any(kw in location_lower for kw in ['remote', 'anywhere', 'distributed', 'wfh', 'global']):
            return 'remote'
        elif 'hybrid' in location_lower or 'flexible' in location_lower:
            return 'hybrid'
        return 'onsite'

    def _process_job_data(self, job_data: Dict, title_key: str, location_key: str, dept_key: str, desc_key: Optional[str] = None) -> Tuple[Dict, str]:
        title = str(job_data.get(title_key, 'N/A') or 'N/A')
        location = str(job_data.get(location_key, 'Unknown') or 'Unknown')
        dept = str(job_data.get(dept_key, 'Unknown') or 'Unknown')
        description = str(job_data.get(desc_key, '') or '')

        work_type = self._classify_work_type(location)
        city, region, country = _normalize_location(location)
        skills = _extract_skills(title + ' ' + description)

        hash_input = f"{title}|{location}|{description[:300]}"
        job_hash = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

        return {
            'hash': job_hash,
            'title': title,
            'location_raw': location,
            'department': dept,
            'work_type': work_type,
            'city': city,
            'region': region,
            'country': country,
            'skills': list(skills.keys()),
            'skills_count': skills
        }, job_hash

    # ============================================================================
    # ATS SPECIFIC FETCHING
    # ============================================================================

    async def _fetch_greenhouse_jobs(self, token: str) -> Optional[List[Dict]]:
        url = f"https://boards-api.greenhouse.io/v1/boards/{quote(token)}/jobs?content=true"
        data = await self._fetch(url)
        if data and data.get('jobs'):
            processed_jobs = []
            for job in data['jobs']:
                location_name = job.get('location', {}).get('name', 'Unknown')
                dept_name = next((d.get('name', 'Unknown') for d in job.get('departments', [])), 'Unknown')
                processed_job, _ = self._process_job_data(
                    job,
                    title_key='title',
                    location_key='location.name',
                    dept_key=dept_name,
                    desc_key='content'
                )
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
        url = f"https://{token}.wd5.myworkdayjobs.com/wday/c/jobsite/WdayService/GetJobPostings"
        headers = {'Content-Type': 'application/json'}
        payload = json.dumps({"limit": 500, "offset": 0, "searchText": "", "sortBy": "postedDate"})
        client = await self._get_client()
        try:
            async with self._semaphore:
                async with client.post(url, headers=headers, data=payload, timeout=20) as response:
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
        except Exception as e:
            logger.debug(f"Workday fetch failed for {token}: {e}")
        return None

    async def _fetch_ashby_jobs(self, token: str) -> Optional[List[Dict]]:
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

    async def _fetch_smartrecruiters_jobs(self, token: str) -> Optional[List[Dict]]:
        url = f"https://api.smartrecruiters.com/v1/companies/{token}/postings"
        data = await self._fetch(url)
        if data and 'content' in data:
            processed_jobs = []
            for job in data['content']:
                processed_job, _ = self._process_job_data(
                    job,
                    title_key='jobTitle',
                    location_key='location',
                    dept_key='department',
                    desc_key='description'
                )
                processed_jobs.append(processed_job)
            return processed_jobs
        return None

    async def _playwright_custom_scrape(self, url: str, company_name: str) -> Optional[JobBoard]:
        if not PLAYWRIGHT_AVAILABLE:
            return None
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.set_extra_http_headers({'User-Agent': ua.random})
                await page.goto(url, timeout=60000, wait_until="networkidle")
                await page.wait_for_timeout(4000)

                jobs = []
                title_elements = await page.locator('h1, h2, h3, [class*="title" i], a[href*="/job/"], [class*="position" i]').all_text_contents()
                location_elements = await page.locator('[class*="location" i], [class*="city" i], [class*="place" i], [class*="address" i]').all_text_contents()

                for i, title in enumerate(title_elements):
                    title = title.strip()
                    location = location_elements[i].strip() if i < len(location_elements) else "Unknown"
                    if title and len(title) > 8 and any(word in title.lower() for word in ['engineer', 'developer', 'manager', 'director', 'analyst']):
                        processed_job, _ = self._process_job_data(
                            {'title': title, 'location': location, 'department': 'Unknown', 'description': title},
                            'title', 'location', 'Unknown'
                        )
                        jobs.append(processed_job)

                await browser.close()

                if jobs:
                    self.stats.custom_found += 1
                    return JobBoard(
                        ats_type='custom',
                        token=url,
                        company_name=company_name,
                        job_count=len(jobs),
                        remote_count=sum(1 for j in jobs if j['work_type'] == 'remote'),
                        hybrid_count=sum(1 for j in jobs if j['work_type'] == 'hybrid'),
                        onsite_count=sum(1 for j in jobs if j['work_type'] == 'onsite'),
                        locations=[j['location_raw'] for j in jobs],
                        departments=list({j['department'] for j in jobs}),
                        normalized_locations={'city': {}, 'region': {}, 'country': {}},  # Simplified for custom
                        extracted_skills={},
                        jobs=jobs,
                        careers_url=url,
                        source='playwright'
                    )
        except Exception as e:
            logger.debug(f"Playwright fallback failed for {url}: {e}")
        return None

    async def _fetch_job_data_from_token(self, ats_type: str, token: str, company_name: str) -> Optional[JobBoard]:
        fetchers = {
            'greenhouse': self._fetch_greenhouse_jobs,
            'lever': self._fetch_lever_jobs,
            'workday': self._fetch_workday_jobs,
            'ashby': self._fetch_ashby_jobs,
            'jobvite': self._fetch_jobvite_jobs,
            'smartrecruiters': self._fetch_smartrecruiters_jobs,
        }
        fetcher = fetchers.get(ats_type)
        if not fetcher:
            return None

        processed_jobs = await fetcher(token)
        if not processed_jobs:
            return None

        # Aggregation
        job_count = len(processed_jobs)
        remote_count = hybrid_count = onsite_count = 0
        raw_locations = set()
        departments = set()
        normalized_locations = {'city': {}, 'region': {}, 'country': {}}
        total_skills: Dict[str, int] = {}

        for job in processed_jobs:
            raw_locations.add(job['location_raw'])
            departments.add(job['department'])
            if job['work_type'] == 'remote':
                remote_count += 1
            elif job['work_type'] == 'hybrid':
                hybrid_count += 1
            else:
                onsite_count += 1

            for key in ['city', 'region', 'country']:
                val = job[key]
                if val:
                    normalized_locations[key][val] = normalized_locations[key].get(val, 0) + 1

            for skill, count in job['skills_count'].items():
                total_skills[skill] = total_skills.get(skill, 0) + count

        final_skills = dict(sorted(total_skills.items(), key=lambda x: x[1], reverse=True)[:5])

        try:
            self.db.archive_jobs(f"{ats_type}_{token}", processed_jobs)
        except Exception as e:
            logger.error(f"Failed to archive jobs for {company_name}: {e}")

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
            source='ats',
            careers_url=''
        )

    async def _scrape_for_token_and_data(self, company_name: str, token_slug: str) -> Optional[JobBoard]:
        urls_to_test = _generate_career_url_variants(token_slug)
        client = await self._get_client()
        last_successful_url = None

        for url in urls_to_test:
            if not await self._allowed_by_robots(url):
                continue

            try:
                async with self._semaphore:
                    async with client.get(url, allow_redirects=True, timeout=20) as response:
                        final_url = str(response.url)
                        if response.status == 200:
                            last_successful_url = final_url

                        token = None
                        ats_type = None

                        if 'boards.greenhouse.io' in final_url:
                            m = re.search(r'boards\.greenhouse\.io/([^/?]+)', final_url)
                            if m:
                                ats_type = 'greenhouse'
                                token = m.group(1)
                        elif 'jobs.lever.co' in final_url:
                            m = re.search(r'jobs\.lever\.co/([^/?]+)', final_url)
                            if m:
                                ats_type = 'lever'
                                token = m.group(1)
                        elif 'jobs.ashby.app' in final_url:
                            m = re.search(r'jobs\.ashby\.app/([^/?]+)', final_url)
                            if m:
                                ats_type = 'ashby'
                                token = m.group(1)
                        elif 'jobs.jobvite.com' in final_url:
                            m = re.search(r'jobs\.jobvite\.com/([^/?]+)', final_url)
                            if m:
                                ats_type = 'jobvite'
                                token = m.group(1)
                        elif 'myworkdayjobs.com' in final_url:
                            m = re.search(r'([^.]+)\.wd5\.myworkdayjobs\.com', final_url)
                            if m:
                                ats_type = 'workday'
                                token = m.group(1)

                        if response.status == 200 and not token:
                            html = await response.text()
                            soup = BeautifulSoup(html, 'html.parser')
                            scripts = soup.find_all('script')
                            for script in scripts:
                                if script.string and 'greenhouse' in script.string.lower():
                                    m = re.search(r'for=([^"&]+)', script.string)
                                    if m:
                                        ats_type = 'greenhouse'
                                        token = m.group(1)

                        if token and ats_type:
                            logger.info(f"SCRAPE HIT: {company_name} → {ats_type}:{token} via {url}")
                            board = await self._fetch_job_data_from_token(ats_type, token, company_name)
                            if board and board.job_count > 0:
                                board.careers_url = final_url
                                return board

            except Exception as e:
                logger.debug(f"Error testing {url}: {e}")

            await asyncio.sleep(random.uniform(4, 9))

        # Fallback to Playwright if we have a valid careers page
        if last_successful_url:
            logger.info(f"Attempting Playwright fallback for {company_name} at {last_successful_url}")
            return await self._playwright_custom_scrape(last_successful_url, company_name)

        return None

    def _save_company(self, board: JobBoard):
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
            'careers_url': board.careers_url,
        }
        self.db.upsert_company(company_data)

    async def _test_company(self, seed_id: int, company_name: str, token_slug: str, source: str) -> Tuple[bool, int]:
        self.stats.total_tested += 1

        board = await self._scrape_for_token_and_data(company_name, token_slug)
        if board:
            self._save_company(board)

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
            elif board.ats_type == 'smartrecruiters':
                self.stats.smartrecruiters_found += 1
            elif board.ats_type == 'custom':
                self.stats.custom_found += 1

            self.stats.total_companies += 1
            self.stats.total_jobs += board.job_count
            return True, board.job_count

        return False, 0

    async def _refresh_company(self, company: Dict) -> bool:
        company_id = company['id']
        ats_type = company['ats_type']
        token = company['token']
        company_name = company['company_name']

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
                'normalized_locations': board.normalized_locations,
                'extracted_skills': board.extracted_skills,
                'careers_url': company.get('careers_url', ''),
            }
            self.db.upsert_company(company_data)
            self.stats.total_jobs += board.job_count

            closed_count = self.db.mark_stale_jobs_closed(company_id, datetime.utcnow())
            self.stats.closed_jobs += closed_count

            return True
        return False

    async def run_discovery(self, max_companies: Optional[int] = None) -> CollectorStats:
        self._running = True
        self.stats = CollectorStats()
        seeds = self.db.get_seeds_for_collection(max_companies or 500)
        total = len(seeds)
        logger.info(f"Starting discovery on {total} seeds")

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

        self.db.create_monthly_snapshot()
        self.stats.end_time = datetime.utcnow()
        await self._close_client()
        self._running = False
        logger.info(f"Discovery complete: {self.stats.to_dict()}")
        return self.stats

    async def run_refresh(self, hours_since_update: int = 6, max_companies: int = 500) -> CollectorStats:
        self._running = True
        self.stats = CollectorStats()
        companies = self.db.get_companies_for_refresh(hours_since_update, max_companies)
        total = len(companies)
        logger.info(f"Starting refresh on {total} companies")

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

        self.stats.end_time = datetime.utcnow()
        await self._close_client()
        self._running = False
        logger.info(f"Refresh complete: {self.stats.to_dict()}")
        return self.stats

    def stop(self):
        self._running = False

# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

async def run_collection(max_companies: int = None) -> CollectorStats:
    collector = JobIntelCollector()
    return await collector.run_discovery(max_companies=max_companies)

async def run_refresh(hours_since_update: int = 6, max_companies: int = 500) -> CollectorStats:
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
