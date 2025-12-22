"""Job Intelligence Collector - ULTIMATE PRODUCTION v5.2 - Enhanced Fallbacks & Logging"""

import asyncio
import aiohttp
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json
import re
from urllib.parse import urljoin
import random

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from playwright.async_api import async_playwright, Playwright, Browser, TimeoutError as PlaywrightTimeout

from database import get_db, Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ua = UserAgent()

@dataclass
class JobPosting:
    id: str
    title: str
    url: str
    location: Optional[str] = None
    department: Optional[str] = None
    work_type: Optional[str] = None
    posted_date: Optional[str] = None
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    salary_currency: Optional[str] = None
    metadata: Dict = field(default_factory=dict)

@dataclass
class JobBoard:
    company_name: str
    ats_type: str
    board_url: str
    jobs: List[JobPosting] = field(default_factory=list)

@dataclass
class CollectionStats:
    total_tested: int = 0
    total_discovered: int = 0
    total_jobs_collected: int = 0
    total_new_jobs: int = 0
    total_updated_jobs: int = 0
    total_closed_jobs: int = 0
    companies_skipped_no_jobs: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None

class JobIntelCollector:
    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_db()
        self.client: Optional[aiohttp.ClientSession] = None
        self.stats = CollectionStats()
        self._semaphore = asyncio.Semaphore(50)
        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.proxies = []
    
    async def initialize_playwright(self):
        """Initialize Playwright browser with stealth"""
        if self.browser is None:
            try:
                self.playwright = await async_playwright().start()
                self.browser = await self.playwright.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-blink-features=AutomationControlled'
                    ]
                )
                logger.info("✅ Playwright browser initialized with stealth")
            except Exception as e:
                logger.error(f"Failed to start Playwright: {e}")
                self.browser = None

    async def close_playwright(self):
        """Close Playwright browser"""
        if self.browser:
            await self.browser.close()
            self.browser = None
        if self.playwright:
            await self.playwright.stop()
            self.playwright = None
    
    async def _get_client(self) -> aiohttp.ClientSession:
        if self.client is None or self.client.closed:
            headers = {
                'User-Agent': ua.random,
                'Accept': 'text/html,application/json,*/*',
                'Accept-Language': 'en-US,en;q=0.5',
            }
            connector_args = {'limit': 100, 'limit_per_host': 20}
            if self.proxies:
                connector_args['proxy'] = random.choice(self.proxies)
            self.client = aiohttp.ClientSession(
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=45),
                connector=aiohttp.TCPConnector(**connector_args)
            )
        return self.client
    
    async def close(self):
        """Clean up resources"""
        if self.client and not self.client.closed:
            await self.client.close()
        await self.close_playwright()
    
    def _extract_salary(self, text: str) -> Dict:
        """Extract salary from text"""
        if not text:
            return {}
        
        patterns = [
            r'\$(\d{1,3}(?:,\d{3})*)\s*-\s*\$?(\d{1,3}(?:,\d{3})*)',
            r'(\d{1,3})k\s*-\s*(\d{1,3})k',
            r'£(\d{1,3}(?:,\d{3})*)\s*-\s*£?(\d{1,3}(?:,\d{3})*)',
            r'€(\d{1,3}(?:,\d{3})*)\s*-\s*€?(\d{1,3}(?:,\d{3})*)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                min_sal = match.group(1).replace(',', '')
                max_sal = match.group(2).replace(',', '')
                
                currency = 'USD'
                if '£' in match.group(0):
                    currency = 'GBP'
                elif '€' in match.group(0):
                    currency = 'EUR'
                
                if 'k' in match.group(0).lower():
                    min_sal = int(min_sal) * 1000
                    max_sal = int(max_sal) * 1000
                else:
                    min_sal = int(min_sal)
                    max_sal = int(max_sal)
                
                return {
                    'salary_min': min_sal,
                    'salary_max': max_sal,
                    'salary_currency': currency
                }
        
        return {}
    
    def with_retries(func):
        async def wrapper(self, *args, **kwargs):
            for attempt in range(3):
                try:
                    return await func(self, *args, **kwargs)
                except Exception as e:
                    logger.debug(f"Retry {attempt+1} for {func.__name__}: {e}")
                    await asyncio.sleep(random.uniform(1, 3))
            logger.warning(f"Failed after 3 retries: {func.__name__}")
            return None
        return wrapper
    
    @with_retries
    async def _test_greenhouse(self, company_name: str) -> Optional[JobBoard]:
        token = self.db._name_to_token(company_name)
        test_urls = [
            f"https://boards.greenhouse.io/{token}",
            f"https://boards.greenhouse.io/embed/job_board?for={token}",
        ]
        client = await self._get_client()
        
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if any(keyword in text.lower() for keyword in ['greenhouse', 'job', 'position', 'career']):
                            if 'not found' not in text.lower() and 'no open positions' not in text.lower():
                                logger.info(f"✅ Found Greenhouse: {company_name}")
                                return JobBoard(company_name, 'greenhouse', url)
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.debug(f"Greenhouse test failed for {company_name}: {e}")
                continue
        return None
    
    @with_retries
    async def _test_lever(self, company_name: str) -> Optional[JobBoard]:
        token = self.db._name_to_token(company_name)
        test_urls = [f"https://jobs.lever.co/{token}"]
        client = await self._get_client()
        
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'lever' in text.lower() and ('posting' in text.lower() or 'job' in text.lower()):
                            if 'not found' not in text.lower():
                                logger.info(f"✅ Found Lever: {company_name}")
                                return JobBoard(company_name, 'lever', url)
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.debug(f"Lever test failed for {company_name}: {e}")
                continue
        return None
    
    @with_retries
    async def _test_workday(self, company_name: str) -> Optional[JobBoard]:
        token = self.db._name_to_token(company_name)
        test_patterns = [
            f"https://{token}.wd5.myworkdayjobs.com/{token}",
            f"https://{token}.wd1.myworkdayjobs.com/{token}",
            f"https://{token}.wd5.myworkdayjobs.com/External",
            f"https://{token}.wd5.myworkdayjobs.com/Careers",
            f"https://{token}.wd12.myworkdayjobs.com/{token}",
        ]
        client = await self._get_client()
        
        for url in test_patterns:
            try:
                async with client.get(url, allow_redirects=True) as resp:
                    if resp.status == 200 and 'myworkdayjobs.com' in str(resp.url):
                        text = await resp.text()
                        if any(keyword in text.lower() for keyword in ['job', 'career', 'position']):
                            if 'no open positions' not in text.lower():
                                logger.info(f"✅ Found Workday: {company_name}")
                                return JobBoard(company_name, 'workday', str(resp.url))
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.debug(f"Workday test failed for {company_name}: {e}")
                continue
        return None
    
    @with_retries
    async def _test_ashby(self, company_name: str) -> Optional[JobBoard]:
        token = self.db._name_to_token(company_name)
        test_urls = [
            f"https://jobs.ashbyhq.com/{token}",
            f"https://{token}.ashbyhq.com",
        ]
        client = await self._get_client()
        
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'ashby' in text.lower():
                            if any(keyword in text.lower() for keyword in ['posting', 'position', 'opening']):
                                if all(excluded not in text.lower() for excluded in ['not found', 'no positions', 'no openings']):
                                    logger.info(f"✅ Found Ashby: {company_name}")
                                    return JobBoard(company_name, 'ashby', url)
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.debug(f"Ashby test failed for {company_name}: {e}")
                continue
        return None

    @with_retries
    async def _test_bamboohr(self, company_name: str) -> Optional[JobBoard]:
        token = self.db._name_to_token(company_name)
        test_urls = [
            f"https://{token}.bamboohr.com/jobs/",
            f"https://{token}.bamboohr.com/careers",
        ]
        client = await self._get_client()
        
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'bamboohr' in text.lower() and 'job' in text.lower():
                            logger.info(f"✅ Found BambooHR: {company_name}")
                            return JobBoard(company_name, 'bamboohr', url)
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.debug(f"BambooHR test failed for {company_name}: {e}")
                continue
        return None

    @with_retries
    async def _test_taleo(self, company_name: str) -> Optional[JobBoard]:
        token = self.db._name_to_token(company_name)
        test_urls = [
            f"https://{token}.taleo.net/careersection/external/jobsearch.ftl",
            f"https://{token}.taleo.net/careersection",
        ]
        client = await self._get_client()
        
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'taleo' in text.lower() and 'job' in text.lower():
                            logger.info(f"✅ Found Taleo: {company_name}")
                            return JobBoard(company_name, 'taleo', url)
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.debug(f"Taleo test failed for {company_name}: {e}")
                continue
        return None

    @with_retries
    async def _test_icims(self, company_name: str) -> Optional[JobBoard]:
        token = self.db._name_to_token(company_name)
        test_urls = [
            f"https://careers-{token}.icims.com/jobs/search",
            f"https://jobs-{token}.icims.com/jobs/search",
        ]
        client = await self._get_client()
        
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'icims' in text.lower() and 'job' in text.lower():
                            logger.info(f"✅ Found iCIMS: {company_name}")
                            return JobBoard(company_name, 'icims', url)
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.debug(f"iCIMS test failed for {company_name}: {e}")
                continue
        return None

    @with_retries
    async def _test_smartrecruiters(self, company_name: str) -> Optional[JobBoard]:
        token = self.db._name_to_token(company_name)
        test_urls = [
            f"https://careers.smartrecruiters.com/{token}",
            f"https://jobs.smartrecruiters.com/{token}",
        ]
        client = await self._get_client()
        
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'smartrecruiters' in text.lower() and 'job' in text.lower():
                            logger.info(f"✅ Found SmartRecruiters: {company_name}")
                            return JobBoard(company_name, 'smartrecruiters', url)
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.debug(f"SmartRecruiters test failed for {company_name}: {e}")
                continue
        return None

    # ========================================================================
    # NEW: GENERIC CAREERS PAGE FALLBACK
    # ========================================================================
    
    @with_retries
    async def _test_generic_careers(self, company_name: str) -> Optional[JobBoard]:
        """Fallback: Try to find jobs on generic career pages"""
        token = self.db._name_to_token(company_name)
        
        career_urls = [
            f"https://{token}.com/careers",
            f"https://www.{token}.com/careers",
            f"https://careers.{token}.com",
            f"https://{token}.com/jobs",
            f"https://www.{token}.com/jobs",
            f"https://jobs.{token}.com",
        ]
        
        client = await self._get_client()
        
        for url in career_urls:
            try:
                async with client.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        
                        # Check for job indicators
                        job_indicators = ['position', 'opening', 'opportunity', 'apply now', 'job listing', 'current openings']
                        if any(indicator in text.lower() for indicator in job_indicators):
                            # Check if it's actually an ATS we support (redirect detection)
                            if 'greenhouse' in text.lower():
                                logger.info(f"🔄 Generic page redirected to Greenhouse: {company_name}")
                                return await self._test_greenhouse(company_name)
                            elif 'lever' in text.lower():
                                logger.info(f"🔄 Generic page redirected to Lever: {company_name}")
                                return await self._test_lever(company_name)
                            elif 'workday' in text.lower():
                                logger.info(f"🔄 Generic page redirected to Workday: {company_name}")
                                return await self._test_workday(company_name)
                            elif 'ashby' in text.lower():
                                logger.info(f"🔄 Generic page redirected to Ashby: {company_name}")
                                return await self._test_ashby(company_name)
                            else:
                                # Generic board found
                                logger.info(f"✅ Found generic careers page: {company_name}")
                                return JobBoard(company_name, 'generic', url)
            except:
                continue
        
        return None

    # ========================================================================
    # ENHANCED: Better Logging in _test_company
    # ========================================================================

    async def _test_company(self, company_name: str, board_hint: str = None) -> Optional[JobBoard]:
        self.stats.total_tested += 1
        
        # Track which ATS types we're testing
        ats_tested = []
        
        try:
            self.db.increment_seed_tested(company_name)
        except:
            pass
        
        test_order = [
            ('greenhouse', self._test_greenhouse),
            ('lever', self._test_lever),
            ('workday', self._test_workday),
            ('ashby', self._test_ashby),
            ('bamboohr', self._test_bamboohr),
            ('taleo', self._test_taleo),
            ('icims', self._test_icims),
            ('smartrecruiters', self._test_smartrecruiters),
        ]
        
        # Try hint first if provided
        if board_hint:
            for ats_type, test_func in test_order:
                if ats_type == board_hint.lower():
                    ats_tested.append(ats_type)
                    board = await test_func(company_name)
                    if board:
                        self.db.increment_seed_success(company_name)
                        logger.info(f"✅ {company_name}: {ats_type} (hint)")
                        return board
                    break
        
        # Try all others
        for ats_type, test_func in test_order:
            if board_hint and ats_type == board_hint.lower():
                continue
            ats_tested.append(ats_type)
            board = await test_func(company_name)
            if board:
                self.db.increment_seed_success(company_name)
                logger.info(f"✅ {company_name}: {ats_type}")
                return board
            await asyncio.sleep(0.3)
        
        # Try generic careers page fallback
        logger.debug(f"🔍 {company_name}: No ATS found, trying generic careers...")
        ats_tested.append('generic')
        board = await self._test_generic_careers(company_name)
        if board:
            self.db.increment_seed_success(company_name)
            logger.info(f"✅ {company_name}: generic careers page")
            return board
        
        # Log what we tested
        logger.debug(f"❌ {company_name}: No jobs found (tested: {', '.join(ats_tested)})")
        
        return None

    @with_retries
    async def _scrape_greenhouse(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        client = await self._get_client()
        
        token = None
        if 'for=' in board.board_url:
            token = board.board_url.split('for=')[-1].split('&')[0]
        else:
            token_match = re.search(r'greenhouse\.io/([a-z0-9-]+)', board.board_url)
            if token_match:
                token = token_match.group(1)
        
        if not token:
            return []
        
        api_urls = [
            f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs",
            f"https://boards.greenhouse.io/embed/job_board/jobs?for={token}",
        ]
        
        for api_url in api_urls:
            try:
                async with client.get(api_url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        job_list = data.get('jobs', []) if isinstance(data, dict) else data
                        
                        if not isinstance(job_list, list):
                            continue
                        
                        for job in job_list:
                            if not isinstance(job, dict):
                                continue
                            
                            salary_info = self._extract_salary(job.get('content', '') or job.get('description', ''))
                            jobs.append(JobPosting(
                                id=str(job.get('id', '')),
                                title=job.get('title', ''),
                                url=job.get('absolute_url', ''),
                                location=job.get('location', {}).get('name') if isinstance(job.get('location'), dict) else str(job.get('location', '')),
                                department=job.get('departments', [{}])[0].get('name') if job.get('departments') else None,
                                salary_min=salary_info.get('salary_min'),
                                salary_max=salary_info.get('salary_max'),
                                salary_currency=salary_info.get('salary_currency'),
                                metadata=job
                            ))
                        
                        if jobs:
                            logger.info(f"✅ Greenhouse JSON: {len(jobs)} jobs for {board.company_name}")
                            return jobs
            except Exception as e:
                logger.debug(f"Greenhouse API error for {board.company_name}: {e}")
                continue
        
        return jobs

    @with_retries
    async def _scrape_lever(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        client = await self._get_client()
        
        api_url = board.board_url.rstrip('/') + '/postings'
        
        try:
            async with client.get(api_url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    if not isinstance(data, list):
                        return []
                    
                    for job in data:
                        if not isinstance(job, dict):
                            continue
                        
                        salary_info = self._extract_salary(job.get('description', ''))
                        jobs.append(JobPosting(
                            id=job.get('id', ''),
                            title=job.get('text', ''),
                            url=job.get('hostedUrl', ''),
                            location=job.get('categories', {}).get('location'),
                            department=job.get('categories', {}).get('team'),
                            work_type=job.get('categories', {}).get('commitment'),
                            posted_date=job.get('createdAt'),
                            salary_min=salary_info.get('salary_min'),
                            salary_max=salary_info.get('salary_max'),
                            salary_currency=salary_info.get('salary_currency'),
                            metadata=job
                        ))
                    logger.info(f"✅ Lever JSON: {len(jobs)} jobs for {board.company_name}")
        except Exception as e:
            logger.debug(f"Lever API error for {board.company_name}: {e}")
        
        return jobs

    @with_retries
    async def _scrape_workday(self, board: JobBoard) -> List[JobPosting]:
        if not self.browser:
            logger.warning(f"Playwright not available for {board.company_name}")
            return []

        jobs = []
        page = None
        
        try:
            page = await self.browser.new_page()
            page.set_default_timeout(45000)
            
            logger.info(f"🌐 Loading Workday: {board.board_url}")
            await page.goto(board.board_url, wait_until='domcontentloaded', timeout=30000)
            
            await asyncio.sleep(6)
            
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await asyncio.sleep(2)
            
            job_elements = []
            
            elements = await page.query_selector_all('[data-automation-id="jobTitle"]')
            if elements and len(elements) > 0:
                job_elements = elements
                logger.info(f"✅ Workday: Found {len(elements)} jobs (automation-id)")
            
            if not job_elements:
                elements = await page.query_selector_all('li[role="listitem"] a, li a[href*="/job/"]')
                if elements and len(elements) > 3:
                    job_elements = elements
                    logger.info(f"✅ Workday: Found {len(elements)} jobs (list items)")
            
            if not job_elements:
                all_links = await page.query_selector_all('a')
                for link in all_links:
                    href = await link.get_attribute('href')
                    text = await link.text_content()
                    if href and '/job/' in href and text and len(text.strip()) > 5:
                        job_elements.append(link)
                if len(job_elements) > 0:
                    logger.info(f"✅ Workday: Found {len(job_elements)} jobs (href filter)")
            
            if not job_elements:
                logger.warning(f"No jobs found for {board.company_name}")
                return []
            
            for element in job_elements[:300]:
                try:
                    title = (await element.text_content()).strip()
                    href = await element.get_attribute('href')
                    
                    if not title or not href or len(title) < 3:
                        continue
                    
                    job_url = urljoin(board.board_url, href) if not href.startswith('http') else href
                    job_id = job_url.split('/')[-1] or title.replace(' ', '-').lower()
                    
                    location = None
                    parent = await element.evaluate_handle('el => el.closest("li") || el.closest("div")')
                    if parent:
                        parent_text = await parent.evaluate('el => el.textContent')
                        location_match = re.search(r'(Remote|Hybrid|[A-Z][a-z]+,\s*[A-Z]{2})', parent_text)
                        if location_match:
                            location = location_match.group(1)
                    
                    jobs.append(JobPosting(
                        id=job_id,
                        title=title,
                        url=job_url,
                        location=location
                    ))
                except Exception as e:
                    logger.debug(f"Error extracting Workday job: {e}")
                    continue
            
            logger.info(f"✅ Workday: Scraped {len(jobs)} jobs for {board.company_name}")
            
        except Exception as e:
            logger.error(f"Workday scraping error for {board.company_name}: {e}")
        
        finally:
            if page:
                await page.close()
        
        return jobs

    # ========================================================================
    # CRITICAL FIX: Ashby Scraper - Enhanced with Aggressive Playwright
    # ========================================================================
    
    @with_retries
    async def _scrape_ashby(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        client = await self._get_client()

        api_endpoints = [
            '/api/posting',
            '/api/postings',
            '/postings.json',
        ]
        
        for endpoint in api_endpoints:
            try:
                api_url = board.board_url.rstrip('/') + endpoint
                async with client.get(api_url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        
                        # CRITICAL FIX: Comprehensive None/type checks
                        if data is None:
                            logger.debug(f"Ashby API returned None for {board.company_name}")
                            continue
                        
                        # Extract job list from various response formats
                        job_list = []
                        
                        if isinstance(data, list):
                            job_list = data
                        elif isinstance(data, dict):
                            job_list = data.get('postings', data.get('jobs', data.get('positions', [])))
                            
                            if not job_list and 'title' in data and 'id' in data:
                                job_list = [data]
                        
                        # CRITICAL: Verify job_list is actually a list
                        if not isinstance(job_list, list):
                            logger.debug(f"Ashby API returned non-list data for {board.company_name}")
                            continue
                        
                        # CRITICAL: Check if list is empty
                        if len(job_list) == 0:
                            logger.debug(f"Ashby API returned empty list for {board.company_name}")
                            continue
                        
                        # Process jobs
                        for job in job_list:
                            if not isinstance(job, dict):
                                continue
                            
                            try:
                                # Extract location
                                location_list = []
                                if isinstance(job.get('locationNames'), list):
                                    location_list = [loc.get('name') if isinstance(loc, dict) else str(loc) 
                                                   for loc in job['locationNames'] if loc]
                                elif job.get('location'):
                                    if isinstance(job['location'], dict):
                                        location_list = [job['location'].get('name', '')]
                                    else:
                                        location_list = [str(job['location'])]
                                
                                location = ', '.join(filter(None, location_list)) if location_list else None
                                
                                # Extract salary
                                salary_info = {}
                                if job.get('compensationTier'):
                                    salary_info = self._extract_salary(str(job['compensationTier']))
                                elif job.get('salary'):
                                    salary_info = self._extract_salary(str(job['salary']))
                                
                                # Build job URL
                                job_url = job.get('url', '')
                                if not job_url:
                                    job_id = job.get('id', job.get('slug', ''))
                                    job_url = f"{board.board_url.rstrip('/')}/job/{job_id}"
                                
                                jobs.append(JobPosting(
                                    id=str(job.get('id', job.get('slug', ''))),
                                    title=job.get('title', ''),
                                    url=job_url,
                                    location=location,
                                    department=job.get('departmentName') or job.get('department', {}).get('name') if isinstance(job.get('department'), dict) else job.get('department'),
                                    salary_min=salary_info.get('salary_min'),
                                    salary_max=salary_info.get('salary_max'),
                                    salary_currency=salary_info.get('salary_currency'),
                                    metadata=job
                                ))
                            except Exception as e:
                                logger.debug(f"Error parsing Ashby job: {e}")
                                continue
                        
                        if jobs:
                            logger.info(f"✅ Ashby API: {len(jobs)} jobs for {board.company_name}")
                            return jobs
                            
            except asyncio.TimeoutError:
                logger.debug(f"Timeout on Ashby endpoint {endpoint} for {board.company_name}")
                continue
            except Exception as e:
                logger.debug(f"Error fetching Ashby {endpoint} for {board.company_name}: {e}")
                continue
        
        # CRITICAL: If API returned 0 jobs, ALWAYS try Playwright
        logger.warning(f"⚠️ Ashby API failed for {board.company_name}, trying Playwright...")
        
        if self.browser:
            try:
                page = await self.browser.new_page()
                await page.goto(board.board_url, wait_until='domcontentloaded', timeout=20000)
                await asyncio.sleep(3)
                
                # Try multiple selectors
                selectors = [
                    'a[href*="/jobs/"]',
                    'a[href*="/job/"]',
                    'div[class*="posting"] a',
                    'div[class*="job"] a',
                    'a[class*="job"]',
                ]
                
                job_links = []
                for selector in selectors:
                    try:
                        job_links = await page.query_selector_all(selector)
                        if len(job_links) > 0:
                            logger.info(f"✅ Found {len(job_links)} links with selector: {selector}")
                            break
                    except:
                        continue
                
                for link in job_links[:200]:
                    try:
                        title = (await link.text_content()).strip()
                        href = await link.get_attribute('href')
                        if not title or not href or len(title) < 5:
                            continue
                        job_url = urljoin(board.board_url, href)
                        job_id = job_url.split('/')[-1]
                        jobs.append(JobPosting(id=job_id, title=title, url=job_url))
                    except:
                        continue
                
                if jobs:
                    logger.info(f"✅ Ashby Playwright: {len(jobs)} jobs for {board.company_name}")
                else:
                    logger.warning(f"❌ Ashby Playwright also failed for {board.company_name}")
                
                await page.close()
            except Exception as e:
                logger.error(f"Ashby Playwright error for {board.company_name}: {e}")
        
        return jobs

    @with_retries
    async def _scrape_bamboohr(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        client = await self._get_client()
        
        try:
            async with client.get(board.board_url) as resp:
                if resp.status == 200:
                    soup = BeautifulSoup(await resp.text(), 'html.parser')
                    job_cards = soup.find_all('div', class_='bamboo-job-card')
                    
                    for card in job_cards:
                        try:
                            title_tag = card.find('a')
                            title = title_tag.text.strip() if title_tag else ''
                            url = urljoin(board.board_url, title_tag['href']) if title_tag and title_tag.get('href') else ''
                            location = card.find('div', class_='bamboo-job-location').text.strip() if card.find('div', class_='bamboo-job-location') else None
                            department = card.find('div', class_='bamboo-job-department').text.strip() if card.find('div', class_='bamboo-job-department') else None
                            job_id = url.split('/')[-1] or title.replace(' ', '-')
                            
                            if title and url:
                                jobs.append(JobPosting(id=job_id, title=title, url=url, location=location, department=department))
                        except Exception as e:
                            logger.debug(f"Error parsing BambooHR job: {e}")
                            continue
                    
                    logger.info(f"✅ BambooHR: {len(jobs)} jobs for {board.company_name}")
        except Exception as e:
            logger.debug(f"BambooHR scraping error for {board.company_name}: {e}")
        
        return jobs

    @with_retries
    async def _scrape_taleo(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        
        if self.browser:
            page = None
            try:
                page = await self.browser.new_page()
                await page.goto(board.board_url, wait_until='domcontentloaded', timeout=20000)
                await asyncio.sleep(3)
                
                elements = await page.query_selector_all('a[href*="/jobdetail.ftl"]')
                for elem in elements:
                    try:
                        title = (await elem.text_content()).strip()
                        href = await elem.get_attribute('href')
                        url = urljoin(board.board_url, href)
                        job_id = url.split('job=')[-1] if 'job=' in url else url.split('/')[-1]
                        
                        if title and url:
                            jobs.append(JobPosting(id=job_id, title=title, url=url))
                    except:
                        continue
                
                logger.info(f"✅ Taleo: {len(jobs)} jobs for {board.company_name}")
            except Exception as e:
                logger.debug(f"Taleo scraping error for {board.company_name}: {e}")
            finally:
                if page:
                    await page.close()
        
        return jobs

    @with_retries
    async def _scrape_icims(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        
        if self.browser:
            page = None
            try:
                page = await self.browser.new_page()
                await page.goto(board.board_url, wait_until='domcontentloaded', timeout=20000)
                await asyncio.sleep(3)
                
                elements = await page.query_selector_all('div.iCIMS_JobHeader a')
                for elem in elements:
                    try:
                        title = (await elem.text_content()).strip()
                        href = await elem.get_attribute('href')
                        url = urljoin(board.board_url, href)
                        job_id = url.split('/')[-1]
                        
                        if title and url:
                            jobs.append(JobPosting(id=job_id, title=title, url=url))
                    except:
                        continue
                
                logger.info(f"✅ iCIMS: {len(jobs)} jobs for {board.company_name}")
            except Exception as e:
                logger.debug(f"iCIMS scraping error for {board.company_name}: {e}")
            finally:
                if page:
                    await page.close()
        
        return jobs

    @with_retries
    async def _scrape_smartrecruiters(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        client = await self._get_client()
        
        api_url = board.board_url.rstrip('/') + '/postings'
        
        try:
            async with client.get(api_url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    if not isinstance(data, list):
                        return []
                    
                    for job in data:
                        if not isinstance(job, dict):
                            continue
                        
                        salary_info = self._extract_salary(job.get('description', ''))
                        jobs.append(JobPosting(
                            id=job.get('id', ''),
                            title=job.get('name', ''),
                            url=job.get('url', ''),
                            location=job.get('location', {}).get('text') if isinstance(job.get('location'), dict) else None,
                            department=job.get('department', {}).get('label') if isinstance(job.get('department'), dict) else None,
                            work_type=job.get('typeOfEmployment', {}).get('label') if isinstance(job.get('typeOfEmployment'), dict) else None,
                            posted_date=job.get('releasedDate'),
                            salary_min=salary_info.get('salary_min'),
                            salary_max=salary_info.get('salary_max'),
                            salary_currency=salary_info.get('salary_currency'),
                            metadata=job
                        ))
                    logger.info(f"✅ SmartRecruiters: {len(jobs)} jobs for {board.company_name}")
        except Exception as e:
            logger.debug(f"SmartRecruiters API error for {board.company_name}: {e}")
        
        return jobs

    @with_retries
    async def _scrape_generic(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        
        if self.browser:
            page = None
            try:
                page = await self.browser.new_page()
                await page.goto(board.board_url, wait_until='domcontentloaded', timeout=20000)
                await asyncio.sleep(3)
                
                # Try multiple selectors for generic pages
                selectors = [
                    'a[href*="/job/"]',
                    'a[href*="/jobs/"]',
                    'a[href*="/careers/"]',
                    'a[href*="/position"]',
                    'div.job a',
                    'div.position a',
                ]
                
                for selector in selectors:
                    try:
                        elements = await page.query_selector_all(selector)
                        if len(elements) > 0:
                            logger.info(f"✅ Generic: Found {len(elements)} jobs with selector: {selector}")
                            for elem in elements[:200]:
                                try:
                                    title = (await elem.text_content()).strip()
                                    href = await elem.get_attribute('href')
                                    if not title or not href or len(title) < 5:
                                        continue
                                    job_url = urljoin(board.board_url, href)
                                    job_id = job_url.split('/')[-1]
                                    
                                    if title and job_url:
                                        jobs.append(JobPosting(id=job_id, title=title, url=job_url))
                                except:
                                    continue
                            if jobs:
                                break
                    except:
                        continue
                
                if jobs:
                    logger.info(f"✅ Generic: {len(jobs)} jobs for {board.company_name}")
                else:
                    logger.warning(f"❌ Generic scraping failed for {board.company_name}")
            except Exception as e:
                logger.debug(f"Generic scraping error for {board.company_name}: {e}")
            finally:
                if page:
                    await page.close()
        
        return jobs

    async def scrape_board(self, board: JobBoard) -> JobBoard:
        logger.info(f"🔍 Scraping {board.ats_type} for {board.company_name}")
        
        scraper_map = {
            'greenhouse': self._scrape_greenhouse,
            'lever': self._scrape_lever,
            'workday': self._scrape_workday,
            'ashby': self._scrape_ashby,
            'bamboohr': self._scrape_bamboohr,
            'taleo': self._scrape_taleo,
            'icims': self._scrape_icims,
            'smartrecruiters': self._scrape_smartrecruiters,
            'generic': self._scrape_generic,
        }
        
        scraper = scraper_map.get(board.ats_type)
        if scraper:
            try:
                board.jobs = await scraper(board)
            except Exception as e:
                logger.error(f"Scraper failed for {board.company_name} ({board.ats_type}): {e}")
                board.jobs = []
        else:
            logger.warning(f"No scraper for {board.ats_type}")
            board.jobs = []
        
        logger.info(f"✅ Scraped {len(board.jobs)} jobs from {board.company_name}")
        self.stats.total_jobs_collected += len(board.jobs)
        return board
    
    async def _discover_and_scrape(self, company_name: str):
        async with self._semaphore:
            try:
                board = await self._test_company(company_name)
                if board:
                    self.stats.total_discovered += 1
                    board = await self.scrape_board(board)
                    
                    if len(board.jobs) == 0:
                        logger.warning(f"⚠️ Skipping {company_name} - no jobs found")
                        self.stats.companies_skipped_no_jobs += 1
                        return
                    
                    company_id = self.db.add_company(
                        company_name=board.company_name,
                        ats_type=board.ats_type,
                        board_url=board.board_url,
                        job_count=len(board.jobs)
                    )
                    
                    if company_id:
                        new, updated, closed = self.db.archive_jobs(company_id, [
                            {
                                'id': job.id,
                                'title': job.title,
                                'location': job.location,
                                'department': job.department,
                                'work_type': job.work_type,
                                'url': job.url,
                                'posted_date': job.posted_date,
                                'salary_min': job.salary_min,
                                'salary_max': job.salary_max,
                                'salary_currency': job.salary_currency,
                                'metadata': job.metadata
                            }
                            for job in board.jobs
                        ])
                        self.stats.total_new_jobs += new
                        self.stats.total_updated_jobs += updated
                        self.stats.total_closed_jobs += closed
            except Exception as e:
                logger.error(f"Error processing {company_name}: {e}")

    async def add_external_seeds(self):
        sources = [
            'https://en.wikipedia.org/wiki/List_of_largest_technology_companies_by_revenue',
            'https://en.wikipedia.org/wiki/List_of_largest_companies_in_the_United_States_by_revenue',
        ]
        client = await self._get_client()
        for url in sources:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        soup = BeautifulSoup(await resp.text(), 'html.parser')
                        companies = []
                        for tag in soup.find_all(['td', 'li', 'a']):
                            name = tag.text.strip()
                            if name and len(name) > 3 and not re.match(r'^\d+$', name):
                                companies.append(name)
                        unique = list(set(companies))[:1000]
                        seeds = [(name, self.db._name_to_token(name), 'external', 2) for name in unique]
                        inserted = self.db.insert_seeds(seeds)
                        logger.info(f"✅ Added {inserted} seeds from {url}")
            except Exception as e:
                logger.debug(f"External seed source failed: {e}")

    async def run_discovery(self, max_companies: int = 2000) -> CollectionStats:
        await self.initialize_playwright()
        logger.info(f"🔍 Starting discovery on {max_companies} seeds")
        
        await self.add_external_seeds()
        
        seeds = self.db.get_seeds(limit=max_companies, prioritize_quality=True)
        logger.info(f"📋 Testing {len(seeds)} seeds")
        
        tasks = [self._discover_and_scrape(seed['company_name']) for seed in seeds]
        
        batch_size = 50
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            await asyncio.gather(*batch, return_exceptions=True)
            await asyncio.sleep(2)
        
        try:
            self.db.blacklist_poor_seeds(min_tests=3, max_success_rate=5.0)
        except:
            pass
        
        self.stats.end_time = datetime.now()
        duration = (self.stats.end_time - self.stats.start_time).total_seconds()
        
        logger.info(f"=" * 80)
        logger.info(f"✅ Discovery complete!")
        logger.info(f"   Companies with jobs: {self.stats.total_discovered - self.stats.companies_skipped_no_jobs}")
        logger.info(f"   Companies skipped (0 jobs): {self.stats.companies_skipped_no_jobs}")
        logger.info(f"   Total jobs collected: {self.stats.total_jobs_collected}")
        logger.info(f"   Average jobs/company: {self.stats.total_jobs_collected / max(self.stats.total_discovered - self.stats.companies_skipped_no_jobs, 1):.1f}")
        logger.info(f"   Duration: {duration:.1f}s")
        logger.info(f"=" * 80)
        
        return self.stats

    async def run_refresh(self, hours_since_update: int = 6, max_companies: int = 1000) -> CollectionStats:
        await self.initialize_playwright()
        stats = CollectionStats()
        
        companies = self.db.get_companies_for_refresh(hours_since_update, max_companies)
        logger.info(f"🔄 Refreshing {len(companies)} companies")
        
        tasks = []
        for company in companies:
            tasks.append(self._refresh_company(company))
        
        batch_size = 50
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            await asyncio.gather(*batch, return_exceptions=True)
            await asyncio.sleep(2)
        
        stats.end_time = datetime.now()
        logger.info(f"✅ Refresh complete: {stats.total_jobs_collected} jobs")
        return stats
    
    async def _refresh_company(self, company: Dict):
        async with self._semaphore:
            try:
                board = JobBoard(company['company_name'], company['ats_type'], company['board_url'])
                board = await self.scrape_board(board)
                
                self.db.update_company_job_count(company['id'], len(board.jobs))
                new, updated, closed = self.db.archive_jobs(company['id'], [
                    {
                        'id': job.id,
                        'title': job.title,
                        'location': job.location,
                        'department': job.department,
                        'work_type': job.work_type,
                        'url': job.url,
                        'posted_date': job.posted_date,
                        'salary_min': job.salary_min,
                        'salary_max': job.salary_max,
                        'salary_currency': job.salary_currency,
                        'metadata': job.metadata
                    } for job in board.jobs
                ])
                
                self.stats.total_jobs_collected += len(board.jobs)
                self.stats.total_new_jobs += new
                self.stats.total_updated_jobs += updated
                self.stats.total_closed_jobs += closed
            except Exception as e:
                logger.error(f"Error refreshing {company['company_name']}: {e}")

async def run_collection(max_companies: int = 2000) -> CollectionStats:
    collector = JobIntelCollector()
    try:
        return await collector.run_discovery(max_companies=max_companies)
    finally:
        await collector.close()

async def run_refresh(hours_since_update: int = 6, max_companies: int = 1000) -> CollectionStats:
    collector = JobIntelCollector()
    try:
        return await collector.run_refresh(hours_since_update, max_companies)
    finally:
        await collector.close()
