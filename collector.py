"""Job Intelligence Collector - ULTRA v6.1 - Fixed Scrapers
Critical fixes: Workday, Ashby, Generic scrapers completely rewritten
"""

import asyncio
import aiohttp
import logging
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
import json
import re
from urllib.parse import urljoin, urlparse
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
        self._ats_cache: Dict[str, str] = {}
    
    def _generate_token_variations(self, company_name: str) -> List[str]:
        """Generate multiple token variations for a company name"""
        tokens = set()
        
        # Base token
        base = self.db._name_to_token(company_name)
        tokens.add(base)
        
        # Remove common suffixes
        clean_name = re.sub(r'\s+(inc|llc|corp|corporation|company|co|ltd|limited|group|holding|holdings|technologies|technology|systems|solutions|services)\.?$', 
                           '', company_name.lower(), flags=re.IGNORECASE)
        clean_token = re.sub(r'[^a-z0-9]+', '-', clean_name).strip('-')
        tokens.add(clean_token)
        
        # No hyphens version
        tokens.add(clean_token.replace('-', ''))
        
        # First word only
        first_word = clean_name.split()[0] if clean_name.split() else clean_name
        tokens.add(re.sub(r'[^a-z0-9]+', '', first_word))
        
        # Acronyms and combinations
        words = clean_name.split()
        if len(words) > 1:
            acronym = ''.join([w[0] for w in words if w and len(w) > 0])
            if len(acronym) >= 2:
                tokens.add(acronym)
            
            if len(words) >= 2:
                tokens.add(words[0] + words[-1])
                # Also try without spaces
                tokens.add(''.join(words[:2]))
        
        # Special case mappings
        special_mappings = {
            'meta': ['meta', 'facebook', 'metafacebook'],
            'alphabet': ['alphabet', 'google'],
            'amazon': ['amazon', 'amzn'],
            'microsoft': ['microsoft', 'msft'],
            'jpmorgan': ['jpmorgan', 'jpmorganchase', 'jpmc'],
            'bankofamerica': ['bankofamerica', 'bofa', 'boa'],
            'goldmansachs': ['goldmansachs', 'gs', 'goldman'],
            'morganstanley': ['morganstanley', 'ms'],
            'adobe': ['adobe'],
            'salesforce': ['salesforce'],
        }
        
        for key, variations in special_mappings.items():
            if key in clean_token.replace('-', ''):
                tokens.update(variations)
        
        # Filter short tokens
        tokens = {t for t in tokens if len(t) >= 2}
        
        return list(tokens)[:15]
    
    async def initialize_playwright(self):
        """Initialize Playwright browser"""
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
                logger.info("✅ Playwright initialized")
            except Exception as e:
                logger.error(f"Failed to start Playwright: {e}")
                self.browser = None

    async def close_playwright(self):
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
            self.client = aiohttp.ClientSession(
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=100, limit_per_host=20)
            )
        return self.client
    
    async def close(self):
        if self.client and not self.client.closed:
            await self.client.close()
        await self.close_playwright()
    
    def _extract_salary(self, text: str) -> Dict:
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
            for attempt in range(2):
                try:
                    return await func(self, *args, **kwargs)
                except Exception as e:
                    if attempt == 1:
                        logger.debug(f"Failed {func.__name__}: {e}")
                    await asyncio.sleep(0.5)
            return None
        return wrapper
    
    # =========================================================================
    # ATS DETECTION (Keep existing - they work fine)
    # =========================================================================
    
    @with_retries
    async def _test_greenhouse(self, company_name: str) -> Optional[JobBoard]:
        tokens = self._generate_token_variations(company_name)
        client = await self._get_client()
        
        test_urls = []
        for token in tokens[:5]:
            test_urls.extend([
                f"https://boards.greenhouse.io/{token}",
                f"https://boards.greenhouse.io/embed/job_board?for={token}",
                f"https://{token}.greenhouse.io/",
            ])
        
        for url in test_urls[:15]:
            try:
                async with client.get(url, timeout=aiohttp.ClientTimeout(total=8), allow_redirects=True) as resp:
                    if resp.status == 200:
                        final_url = str(resp.url)
                        if 'greenhouse' in final_url.lower():
                            text = await resp.text()
                            if any(kw in text.lower() for kw in ['job', 'position', 'career']):
                                if 'no open position' not in text.lower():
                                    logger.info(f"✅ Greenhouse: {company_name}")
                                    return JobBoard(company_name, 'greenhouse', final_url)
                await asyncio.sleep(0.2)
            except:
                continue
        return None
    
    @with_retries
    async def _test_lever(self, company_name: str) -> Optional[JobBoard]:
        tokens = self._generate_token_variations(company_name)
        client = await self._get_client()
        
        test_urls = []
        for token in tokens[:5]:
            test_urls.extend([
                f"https://jobs.lever.co/{token}",
                f"https://{token}.lever.co",
            ])
        
        for url in test_urls[:10]:
            try:
                async with client.get(url, timeout=aiohttp.ClientTimeout(total=8), allow_redirects=True) as resp:
                    if resp.status == 200:
                        final_url = str(resp.url)
                        if 'lever' in final_url.lower():
                            text = await resp.text()
                            if 'posting' in text.lower() or 'job' in text.lower():
                                if 'not found' not in text.lower():
                                    logger.info(f"✅ Lever: {company_name}")
                                    return JobBoard(company_name, 'lever', final_url)
                await asyncio.sleep(0.2)
            except:
                continue
        return None
    
    @with_retries
    async def _test_workday(self, company_name: str) -> Optional[JobBoard]:
        tokens = self._generate_token_variations(company_name)
        client = await self._get_client()
        
        test_patterns = []
        for token in tokens[:4]:
            for instance in ['wd5', 'wd1', 'wd3', 'wd12', 'wd2']:
                test_patterns.extend([
                    f"https://{token}.{instance}.myworkdayjobs.com/{token}",
                    f"https://{token}.{instance}.myworkdayjobs.com/External",
                    f"https://{token}.{instance}.myworkdayjobs.com/Careers",
                ])
        
        for url in test_patterns[:20]:
            try:
                async with client.get(url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200 and 'myworkdayjobs.com' in str(resp.url):
                        text = await resp.text()
                        if any(kw in text.lower() for kw in ['job', 'career', 'position']):
                            if 'no open position' not in text.lower():
                                logger.info(f"✅ Workday: {company_name}")
                                return JobBoard(company_name, 'workday', str(resp.url))
                await asyncio.sleep(0.2)
            except:
                continue
        return None
    
    @with_retries
    async def _test_ashby(self, company_name: str) -> Optional[JobBoard]:
        tokens = self._generate_token_variations(company_name)
        client = await self._get_client()
        
        test_urls = []
        for token in tokens[:4]:
            test_urls.extend([
                f"https://jobs.ashbyhq.com/{token}",
                f"https://{token}.ashbyhq.com",
            ])
        
        for url in test_urls[:8]:
            try:
                async with client.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'ashby' in text.lower():
                            if any(kw in text.lower() for kw in ['posting', 'position', 'job']):
                                if 'not found' not in text.lower():
                                    logger.info(f"✅ Ashby: {company_name}")
                                    return JobBoard(company_name, 'ashby', url)
                await asyncio.sleep(0.2)
            except:
                continue
        return None

    @with_retries
    async def _test_jobvite(self, company_name: str) -> Optional[JobBoard]:
        tokens = self._generate_token_variations(company_name)
        client = await self._get_client()
        
        test_urls = []
        for token in tokens[:3]:
            test_urls.extend([
                f"https://jobs.jobvite.com/{token}/jobs",
                f"https://{token}.jobvite.com",
            ])
        
        for url in test_urls[:6]:
            try:
                async with client.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'jobvite' in text.lower() and 'job' in text.lower():
                            logger.info(f"✅ Jobvite: {company_name}")
                            return JobBoard(company_name, 'jobvite', url)
                await asyncio.sleep(0.2)
            except:
                continue
        return None

    @with_retries
    async def _test_bamboohr(self, company_name: str) -> Optional[JobBoard]:
        tokens = self._generate_token_variations(company_name)
        client = await self._get_client()
        
        test_urls = []
        for token in tokens[:3]:
            test_urls.extend([
                f"https://{token}.bamboohr.com/jobs/",
                f"https://{token}.bamboohr.com/careers",
            ])
        
        for url in test_urls[:6]:
            try:
                async with client.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'bamboohr' in text.lower() and 'job' in text.lower():
                            logger.info(f"✅ BambooHR: {company_name}")
                            return JobBoard(company_name, 'bamboohr', url)
                await asyncio.sleep(0.2)
            except:
                continue
        return None

    @with_retries
    async def _test_smartrecruiters(self, company_name: str) -> Optional[JobBoard]:
        tokens = self._generate_token_variations(company_name)
        client = await self._get_client()
        
        test_urls = []
        for token in tokens[:3]:
            test_urls.extend([
                f"https://careers.smartrecruiters.com/{token}",
                f"https://jobs.smartrecruiters.com/{token}",
            ])
        
        for url in test_urls[:6]:
            try:
                async with client.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'smartrecruiters' in text.lower() and 'job' in text.lower():
                            logger.info(f"✅ SmartRecruiters: {company_name}")
                            return JobBoard(company_name, 'smartrecruiters', url)
                await asyncio.sleep(0.2)
            except:
                continue
        return None

    @with_retries
    async def _test_generic_careers(self, company_name: str) -> Optional[JobBoard]:
        """Ultra-aggressive generic fallback"""
        tokens = self._generate_token_variations(company_name)
        client = await self._get_client()
        
        career_urls = []
        for token in tokens[:3]:
            for domain in ['.com', '.io', '.ai']:
                career_urls.extend([
                    f"https://{token}{domain}/careers",
                    f"https://www.{token}{domain}/careers",
                    f"https://careers.{token}{domain}",
                    f"https://{token}{domain}/jobs",
                    f"https://jobs.{token}{domain}",
                ])
        
        for url in career_urls[:20]:
            try:
                async with client.get(url, timeout=aiohttp.ClientTimeout(total=8), allow_redirects=True) as resp:
                    if resp.status == 200:
                        final_url = str(resp.url)
                        text = await resp.text()
                        text_lower = text.lower()
                        
                        # Check for ATS redirects
                        if 'greenhouse' in text_lower or 'greenhouse' in final_url:
                            return await self._test_greenhouse(company_name)
                        elif 'lever' in text_lower:
                            return await self._test_lever(company_name)
                        elif 'workday' in text_lower or 'myworkdayjobs' in final_url:
                            return await self._test_workday(company_name)
                        elif 'ashby' in final_url:
                            return await self._test_ashby(company_name)
                        
                        # Check for job indicators
                        job_indicators = ['current opening', 'apply now', 'job listing', 'join our team', 'we\'re hiring']
                        negative_indicators = ['no open position', 'no current opening', 'not hiring']
                        
                        if any(ind in text_lower for ind in job_indicators):
                            if not any(neg in text_lower for neg in negative_indicators):
                                soup = BeautifulSoup(text, 'html.parser')
                                job_links = soup.find_all('a', href=re.compile(r'/(job|position|opening|apply)', re.I))
                                
                                if len(job_links) >= 3:
                                    logger.info(f"✅ Generic: {company_name}")
                                    return JobBoard(company_name, 'generic', final_url)
                
                await asyncio.sleep(0.2)
            except:
                continue
        
        return None

    async def _test_company(self, company_name: str, board_hint: str = None) -> Optional[JobBoard]:
        self.stats.total_tested += 1
        
        try:
            self.db.increment_seed_tested(company_name)
        except:
            pass
        
        test_order = [
            ('greenhouse', self._test_greenhouse),
            ('lever', self._test_lever),
            ('workday', self._test_workday),
            ('ashby', self._test_ashby),
            ('jobvite', self._test_jobvite),
            ('bamboohr', self._test_bamboohr),
            ('smartrecruiters', self._test_smartrecruiters),
        ]
        
        if board_hint:
            for ats_type, test_func in test_order:
                if ats_type == board_hint.lower():
                    board = await test_func(company_name)
                    if board:
                        self.db.increment_seed_success(company_name)
                        return board
                    break
        
        for ats_type, test_func in test_order:
            if board_hint and ats_type == board_hint.lower():
                continue
            
            board = await test_func(company_name)
            if board:
                self.db.increment_seed_success(company_name)
                return board
            
            await asyncio.sleep(0.1)
        
        # Generic fallback
        board = await self._test_generic_careers(company_name)
        if board:
            self.db.increment_seed_success(company_name)
            return board
        
        return None

    # =========================================================================
    # SCRAPERS - COMPLETELY REWRITTEN (THE FIX!)
    # =========================================================================
    
    @with_retries
    async def _scrape_greenhouse(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        client = await self._get_client()
        
        # Extract token
        token = None
        if 'for=' in board.board_url:
            token = board.board_url.split('for=')[-1].split('&')[0]
        else:
            token_match = re.search(r'greenhouse\.io/([a-z0-9-]+)', board.board_url)
            if token_match:
                token = token_match.group(1)
        
        if not token:
            return []
        
        # Try API
        api_urls = [
            f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs",
            f"https://boards.greenhouse.io/embed/job_board/jobs?for={token}",
        ]
        
        for api_url in api_urls:
            try:
                async with client.get(api_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        job_list = data.get('jobs', []) if isinstance(data, dict) else data
                        
                        if not isinstance(job_list, list):
                            continue
                        
                        for job in job_list:
                            if not isinstance(job, dict):
                                continue
                            
                            salary_info = self._extract_salary(job.get('content', '') or job.get('description', ''))
                            
                            location = None
                            if isinstance(job.get('location'), dict):
                                location = job['location'].get('name')
                            else:
                                location = str(job.get('location', '')) if job.get('location') else None
                            
                            department = None
                            if job.get('departments') and len(job['departments']) > 0:
                                department = job['departments'][0].get('name')
                            
                            jobs.append(JobPosting(
                                id=str(job.get('id', '')),
                                title=job.get('title', ''),
                                url=job.get('absolute_url', ''),
                                location=location,
                                department=department,
                                salary_min=salary_info.get('salary_min'),
                                salary_max=salary_info.get('salary_max'),
                                salary_currency=salary_info.get('salary_currency'),
                                metadata=job
                            ))
                        
                        if jobs:
                            logger.info(f"✅ Greenhouse: {len(jobs)} jobs")
                            return jobs
            except Exception as e:
                logger.debug(f"Greenhouse API error: {e}")
                continue
        
        return jobs

    @with_retries
    async def _scrape_lever(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        client = await self._get_client()
        
        api_url = board.board_url.rstrip('/') + '/postings'
        
        try:
            async with client.get(api_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    if not isinstance(data, list):
                        return []
                    
                    for job in data:
                        if not isinstance(job, dict):
                            continue
                        
                        salary_info = self._extract_salary(job.get('description', ''))
                        categories = job.get('categories', {})
                        
                        jobs.append(JobPosting(
                            id=job.get('id', ''),
                            title=job.get('text', ''),
                            url=job.get('hostedUrl', ''),
                            location=categories.get('location') if isinstance(categories, dict) else None,
                            department=categories.get('team') if isinstance(categories, dict) else None,
                            work_type=categories.get('commitment') if isinstance(categories, dict) else None,
                            posted_date=job.get('createdAt'),
                            salary_min=salary_info.get('salary_min'),
                            salary_max=salary_info.get('salary_max'),
                            salary_currency=salary_info.get('salary_currency'),
                            metadata=job
                        ))
                    
                    if jobs:
                        logger.info(f"✅ Lever: {len(jobs)} jobs")
        except Exception as e:
            logger.debug(f"Lever error: {e}")
        
        return jobs

    @with_retries
    async def _scrape_workday(self, board: JobBoard) -> List[JobPosting]:
        """COMPLETELY REWRITTEN - Multiple strategies"""
        if not self.browser:
            logger.warning("Playwright unavailable")
            return []

        jobs = []
        page = None
        
        try:
            page = await self.browser.new_page()
            
            # Longer timeout and wait for network to be idle
            logger.info(f"🌐 Loading Workday: {board.board_url}")
            await page.goto(board.board_url, wait_until='networkidle', timeout=50000)
            
            # Wait for content to load
            await asyncio.sleep(10)
            
            # Aggressive scrolling to load all jobs
            for i in range(5):
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await asyncio.sleep(2)
            
            # Try MANY different selector strategies
            selector_strategies = [
                # Strategy 1: Workday automation IDs
                {
                    'name': 'automation-id',
                    'selector': '[data-automation-id="jobTitle"]',
                    'extract': 'text_and_href'
                },
                # Strategy 2: Composite containers
                {
                    'name': 'composite',
                    'selector': '[data-automation-id="compositeContainer"] a',
                    'extract': 'text_and_href'
                },
                # Strategy 3: Job links
                {
                    'name': 'job-links',
                    'selector': 'a[href*="/job/"]',
                    'extract': 'text_and_href'
                },
                # Strategy 4: Apply links
                {
                    'name': 'apply-links',
                    'selector': 'a[href*="apply"]',
                    'extract': 'text_and_href'
                },
                # Strategy 5: ARIA labels
                {
                    'name': 'aria',
                    'selector': 'a[aria-label*="job"]',
                    'extract': 'text_and_href'
                },
                # Strategy 6: List items
                {
                    'name': 'list-items',
                    'selector': 'li[role="listitem"] a',
                    'extract': 'text_and_href'
                },
                # Strategy 7: Generic containers
                {
                    'name': 'containers',
                    'selector': 'div[class*="job"] a, div[class*="Job"] a',
                    'extract': 'text_and_href'
                },
            ]
            
            for strategy in selector_strategies:
                try:
                    elements = await page.query_selector_all(strategy['selector'])
                    
                    if len(elements) > 5:
                        logger.info(f"✅ Workday strategy '{strategy['name']}' found {len(elements)} elements")
                        
                        for element in elements[:500]:
                            try:
                                title = await element.text_content()
                                if not title or len(title.strip()) < 3:
                                    continue
                                
                                title = title.strip()
                                
                                # Filter out non-job links
                                lower_title = title.lower()
                                skip_keywords = ['home', 'about', 'contact', 'privacy', 'terms', 'help', 'sign', 'log']
                                if any(kw in lower_title for kw in skip_keywords):
                                    continue
                                
                                href = await element.get_attribute('href')
                                if not href:
                                    continue
                                
                                # Must contain job/apply
                                if '/job/' not in href and '/apply/' not in href:
                                    continue
                                
                                job_url = urljoin(board.board_url, href)
                                job_id = job_url.split('/')[-1] or title.replace(' ', '-').lower()[:50]
                                
                                # Try to extract location from parent element
                                location = None
                                try:
                                    parent = await element.evaluate_handle('el => el.closest("li") || el.closest("div")')
                                    if parent:
                                        parent_text = await parent.evaluate('el => el.textContent')
                                        loc_match = re.search(r'(Remote|Hybrid|[A-Z][a-z]+,\s*[A-Z]{2,3})', parent_text)
                                        if loc_match:
                                            location = loc_match.group(1)
                                except:
                                    pass
                                
                                jobs.append(JobPosting(
                                    id=job_id,
                                    title=title,
                                    url=job_url,
                                    location=location
                                ))
                                
                            except Exception as e:
                                logger.debug(f"Error extracting job: {e}")
                                continue
                        
                        if len(jobs) > 0:
                            logger.info(f"✅ Workday: {len(jobs)} jobs extracted")
                            break  # Found jobs, stop trying strategies
                        
                except Exception as e:
                    logger.debug(f"Strategy {strategy['name']} failed: {e}")
                    continue
            
            if len(jobs) == 0:
                logger.warning(f"❌ Workday: No jobs found for {board.company_name}")
            
        except Exception as e:
            logger.error(f"Workday error: {e}")
        finally:
            if page:
                await page.close()
        
        return jobs

    @with_retries
    async def _scrape_ashby(self, board: JobBoard) -> List[JobPosting]:
        """COMPLETELY REWRITTEN - GraphQL + Better API + Playwright"""
        jobs = []
        client = await self._get_client()

        # Strategy 1: Try GraphQL endpoint (Ashby's new approach)
        try:
            graphql_url = board.board_url.rstrip('/') + '/api/non-user-graphql'
            query = {
                "operationName": "ApiJobBoardWithTeams",
                "variables": {},
                "query": """query ApiJobBoardWithTeams {
                    jobBoard {
                        jobs {
                            id
                            title
                            locationName
                            departmentName
                            employmentType
                            externalLink
                        }
                    }
                }"""
            }
            
            async with client.post(graphql_url, json=query, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    jobs_data = data.get('data', {}).get('jobBoard', {}).get('jobs', [])
                    
                    for job in jobs_data:
                        job_id = str(job.get('id', ''))
                        job_url = job.get('externalLink', '') or f"{board.board_url}/jobs/{job_id}"
                        
                        jobs.append(JobPosting(
                            id=job_id,
                            title=job.get('title', ''),
                            url=job_url,
                            location=job.get('locationName'),
                            department=job.get('departmentName'),
                            work_type=job.get('employmentType')
                        ))
                    
                    if jobs:
                        logger.info(f"✅ Ashby GraphQL: {len(jobs)} jobs")
                        return jobs
        except Exception as e:
            logger.debug(f"Ashby GraphQL failed: {e}")

        # Strategy 2: Try REST API endpoints
        api_patterns = ['/api/posting', '/api/postings', '/postings.json']
        
        for pattern in api_patterns:
            try:
                api_url = board.board_url.rstrip('/') + pattern
                async with client.get(api_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        
                        if not data:
                            continue
                        
                        job_list = []
                        if isinstance(data, list):
                            job_list = data
                        elif isinstance(data, dict):
                            job_list = data.get('postings', data.get('jobs', []))
                        
                        if not job_list:
                            continue
                        
                        for job in job_list:
                            if not isinstance(job, dict):
                                continue
                            
                            location = None
                            if job.get('locationNames'):
                                if isinstance(job['locationNames'], list):
                                    locs = [l.get('name') if isinstance(l, dict) else str(l) for l in job['locationNames']]
                                    location = ', '.join(filter(None, locs))
                            elif job.get('location'):
                                location = job['location'].get('name') if isinstance(job['location'], dict) else str(job['location'])
                            
                            job_url = job.get('url', '') or f"{board.board_url}/jobs/{job.get('id', '')}"
                            
                            jobs.append(JobPosting(
                                id=str(job.get('id', job.get('slug', ''))),
                                title=job.get('title', ''),
                                url=job_url,
                                location=location,
                                department=job.get('departmentName'),
                                metadata=job
                            ))
                        
                        if jobs:
                            logger.info(f"✅ Ashby REST: {len(jobs)} jobs")
                            return jobs
            except Exception as e:
                logger.debug(f"Ashby {pattern} failed: {e}")
                continue
        
        # Strategy 3: Playwright fallback with multiple selectors
        if self.browser and len(jobs) == 0:
            try:
                page = await self.browser.new_page()
                await page.goto(board.board_url, wait_until='networkidle', timeout=30000)
                await asyncio.sleep(5)
                
                # Scroll to load all jobs
                for _ in range(3):
                    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    await asyncio.sleep(1)
                
                selectors = [
                    'a[href*="/jobs/"]',
                    'a[href*="/postings/"]',
                    'div[class*="posting"] a',
                    '[data-testid*="job"] a',
                    'div[class*="job"] a',
                ]
                
                for selector in selectors:
                    try:
                        links = await page.query_selector_all(selector)
                        if len(links) > 3:
                            logger.info(f"✅ Ashby Playwright: selector '{selector}' found {len(links)} jobs")
                            
                            for link in links[:300]:
                                try:
                                    title = (await link.text_content()).strip()
                                    href = await link.get_attribute('href')
                                    
                                    if not title or not href or len(title) < 5:
                                        continue
                                    
                                    # Filter out navigation links
                                    if any(word in title.lower() for word in ['about', 'home', 'contact', 'blog']):
                                        continue
                                    
                                    job_url = urljoin(board.board_url, href)
                                    job_id = job_url.split('/')[-1]
                                    
                                    jobs.append(JobPosting(id=job_id, title=title, url=job_url))
                                except:
                                    continue
                            
                            if jobs:
                                break
                    except:
                        continue
                
                await page.close()
                
                if jobs:
                    logger.info(f"✅ Ashby Playwright: {len(jobs)} jobs")
                    
            except Exception as e:
                logger.error(f"Ashby Playwright error: {e}")
        
        return jobs

    @with_retries
    async def _scrape_generic(self, board: JobBoard) -> List[JobPosting]:
        """ENHANCED - Better selector strategies"""
        jobs = []
        
        if not self.browser:
            return []
        
        page = None
        try:
            page = await self.browser.new_page()
            await page.goto(board.board_url, wait_until='networkidle', timeout=30000)
            await asyncio.sleep(5)
            
            # Scroll to load dynamic content
            for _ in range(3):
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await asyncio.sleep(1)
            
            # Multiple selector strategies
            selector_strategies = [
                'a[href*="/job/"]',
                'a[href*="/jobs/"]',
                'a[href*="/careers/"]',
                'a[href*="/position"]',
                'a[href*="/apply"]',
                'a[href*="/opening"]',
                'div.job a',
                'div.position a',
                'li.job a',
                'article a[href*="job"]',
                '[class*="job"] a',
                '[class*="position"] a',
            ]
            
            for selector in selector_strategies:
                try:
                    elements = await page.query_selector_all(selector)
                    if len(elements) > 3:
                        logger.info(f"✅ Generic: selector '{selector}' found {len(elements)} jobs")
                        
                        for elem in elements[:300]:
                            try:
                                title = (await elem.text_content()).strip()
                                href = await elem.get_attribute('href')
                                
                                if not title or not href or len(title) < 5:
                                    continue
                                
                                # Filter navigation
                                skip_words = ['about', 'contact', 'home', 'blog', 'privacy', 'terms', 'login', 'sign']
                                if any(word in title.lower() for word in skip_words):
                                    continue
                                
                                job_url = urljoin(board.board_url, href)
                                job_id = job_url.split('/')[-1] or title.replace(' ', '-').lower()[:50]
                                
                                jobs.append(JobPosting(id=job_id, title=title, url=job_url))
                            except:
                                continue
                        
                        if jobs:
                            break
                except:
                    continue
            
            if jobs:
                logger.info(f"✅ Generic: {len(jobs)} jobs")
            else:
                logger.warning(f"❌ Generic: No jobs found")
                
        except Exception as e:
            logger.error(f"Generic error: {e}")
        finally:
            if page:
                await page.close()
        
        return jobs

    # Keep your existing scrapers for other ATS types
    @with_retries
    async def _scrape_bamboohr(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        client = await self._get_client()
        
        try:
            async with client.get(board.board_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
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
                        except:
                            continue
                    
                    if jobs:
                        logger.info(f"✅ BambooHR: {len(jobs)} jobs")
        except Exception as e:
            logger.debug(f"BambooHR error: {e}")
        
        return jobs

    @with_retries
    async def _scrape_smartrecruiters(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        client = await self._get_client()
        
        api_url = board.board_url.rstrip('/') + '/postings'
        
        try:
            async with client.get(api_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
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
                    
                    if jobs:
                        logger.info(f"✅ SmartRecruiters: {len(jobs)} jobs")
        except Exception as e:
            logger.debug(f"SmartRecruiters error: {e}")
        
        return jobs

    async def scrape_board(self, board: JobBoard) -> JobBoard:
        logger.info(f"🔍 Scraping {board.ats_type} for {board.company_name}")
        
        scraper_map = {
            'greenhouse': self._scrape_greenhouse,
            'lever': self._scrape_lever,
            'workday': self._scrape_workday,
            'ashby': self._scrape_ashby,
            'jobvite': self._scrape_generic,
            'bamboohr': self._scrape_bamboohr,
            'smartrecruiters': self._scrape_smartrecruiters,
            'generic': self._scrape_generic,
        }
        
        scraper = scraper_map.get(board.ats_type)
        if scraper:
            try:
                board.jobs = await scraper(board)
            except Exception as e:
                logger.error(f"Scraper failed: {e}")
                board.jobs = []
        else:
            logger.warning(f"No scraper for {board.ats_type}")
            board.jobs = []
        
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
                async with client.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
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
                logger.debug(f"External seed failed: {e}")

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
        logger.info(f"   Tested: {self.stats.total_tested}")
        logger.info(f"   Discovered: {self.stats.total_discovered}")
        logger.info(f"   Success rate: {(self.stats.total_discovered / max(self.stats.total_tested, 1) * 100):.1f}%")
        logger.info(f"   Companies with jobs: {self.stats.total_discovered - self.stats.companies_skipped_no_jobs}")
        logger.info(f"   Total jobs: {self.stats.total_jobs_collected}")
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
