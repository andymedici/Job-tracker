"""Job Intelligence Collector - PRODUCTION GRADE v2.0"""

import asyncio
import aiohttp
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json
import re
from urllib.parse import urljoin

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
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None

class JobIntelCollector:
    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_db()
        self.client: Optional[aiohttp.ClientSession] = None
        self.stats = CollectionStats()
        self._semaphore = asyncio.Semaphore(10)
        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
    
    async def initialize_playwright(self):
        """Initialize Playwright browser once"""
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
                logger.info("✅ Playwright browser initialized")
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
            self.client = aiohttp.ClientSession(
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=50, limit_per_host=10)
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
        
        pattern = r'\$(\d{1,3}(?:,\d{3})*|\d+)k?\s*-\s*\$?(\d{1,3}(?:,\d{3})*|\d+)k?'
        match = re.search(pattern, text, re.IGNORECASE)
        
        if match:
            min_sal = match.group(1).replace(',', '')
            max_sal = match.group(2).replace(',', '')
            
            if 'k' in match.group(0).lower():
                min_sal = int(min_sal.replace('k', '')) * 1000 if 'k' in min_sal.lower() else int(min_sal) * 1000
                max_sal = int(max_sal.replace('k', '')) * 1000 if 'k' in max_sal.lower() else int(max_sal) * 1000
            else:
                min_sal = int(min_sal)
                max_sal = int(max_sal)
                
            return {
                'salary_min': min_sal,
                'salary_max': max_sal,
                'salary_currency': 'USD'
            }
        
        return {}

    # ========================================================================
    # ATS TESTING
    # ========================================================================
    
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
                        if 'greenhouse' in text.lower() or 'job' in text.lower():
                            logger.info(f"✅ Found Greenhouse: {company_name}")
                            return JobBoard(company_name, 'greenhouse', url)
            except:
                pass
            await asyncio.sleep(0.5)
        return None
    
    async def _test_lever(self, company_name: str) -> Optional[JobBoard]:
        token = self.db._name_to_token(company_name)
        test_urls = [f"https://jobs.lever.co/{token}"]
        client = await self._get_client()
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        logger.info(f"✅ Found Lever: {company_name}")
                        return JobBoard(company_name, 'lever', url)
            except:
                pass
            await asyncio.sleep(0.5)
        return None
    
    async def _test_workday(self, company_name: str) -> Optional[JobBoard]:
        token = self.db._name_to_token(company_name)
        # Test multiple Workday variations
        test_patterns = [
            f"https://{token}.wd5.myworkdayjobs.com/{token}",
            f"https://{token}.wd1.myworkdayjobs.com/{token}",
            f"https://{token}.wd5.myworkdayjobs.com/External",
            f"https://{token}.wd5.myworkdayjobs.com/Careers",
        ]
        client = await self._get_client()
        for url in test_patterns:
            try:
                async with client.get(url, allow_redirects=True) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'myworkdayjobs.com' in str(resp.url) and ('job' in text.lower() or 'career' in text.lower()):
                            logger.info(f"✅ Found Workday: {company_name}")
                            return JobBoard(company_name, 'workday', str(resp.url))
            except:
                pass
            await asyncio.sleep(0.5)
        return None
    
    async def _test_ashby(self, company_name: str) -> Optional[JobBoard]:
        token = self.db._name_to_token(company_name)
        test_urls = [f"https://jobs.ashbyhq.com/{token}"]
        client = await self._get_client()
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        logger.info(f"✅ Found Ashby: {company_name}")
                        return JobBoard(company_name, 'ashby', url)
            except:
                pass
            await asyncio.sleep(0.5)
        return None

    async def _test_company(self, company_name: str, board_hint: str = None) -> Optional[JobBoard]:
        """Test company for ATS"""
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
        ]
        
        if board_hint:
            for ats_type, test_func in test_order:
                if ats_type == board_hint.lower():
                    board = await test_func(company_name)
                    if board:
                        try:
                            self.db.increment_seed_success(company_name)
                        except:
                            pass
                        return board
                    break
        
        for ats_type, test_func in test_order:
            if board_hint and ats_type == board_hint.lower():
                continue
            board = await test_func(company_name)
            if board:
                try:
                    self.db.increment_seed_success(company_name)
                except:
                    pass
                return board
            await asyncio.sleep(0.5)
        
        return None
    
    # ========================================================================
    # GREENHOUSE SCRAPER (WORKS WELL)
    # ========================================================================
    
    async def _scrape_greenhouse(self, board: JobBoard) -> List[JobPosting]:
        """Scrape Greenhouse - JSON API + HTML fallback"""
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
            logger.warning(f"Could not extract Greenhouse token from: {board.board_url}")
            return []
        
        # Try JSON API
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
                        
                        for job in job_list:
                            jobs.append(JobPosting(
                                id=str(job.get('id', '')),
                                title=job.get('title', ''),
                                url=job.get('absolute_url', ''),
                                location=job.get('location', {}).get('name') if isinstance(job.get('location'), dict) else str(job.get('location', '')),
                                department=job.get('departments', [{}])[0].get('name') if job.get('departments') else None,
                                metadata=job
                            ))
                        
                        if jobs:
                            logger.info(f"✅ Greenhouse JSON: {len(jobs)} jobs for {board.company_name}")
                            return jobs
            except Exception as e:
                logger.debug(f"Greenhouse API {api_url} failed: {e}")
        
        return jobs

    # ========================================================================
    # LEVER SCRAPER (WORKS WELL)
    # ========================================================================
    
    async def _scrape_lever(self, board: JobBoard) -> List[JobPosting]:
        """Scrape Lever - JSON API"""
        jobs = []
        client = await self._get_client()
        
        api_url = board.board_url.rstrip('/') + '/postings'
        try:
            async with client.get(api_url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for job in data:
                        jobs.append(JobPosting(
                            id=job.get('id', ''),
                            title=job.get('text', ''),
                            url=job.get('hostedUrl', ''),
                            location=job.get('categories', {}).get('location'),
                            department=job.get('categories', {}).get('team'),
                            work_type=job.get('categories', {}).get('commitment'),
                            posted_date=job.get('createdAt'),
                            metadata=job
                        ))
                    logger.info(f"✅ Lever JSON: {len(jobs)} jobs for {board.company_name}")
        except Exception as e:
            logger.error(f"Lever API failed for {board.company_name}: {e}")
        
        return jobs

    # ========================================================================
    # WORKDAY SCRAPER (COMPLETELY REWRITTEN)
    # ========================================================================
    
    async def _scrape_workday(self, board: JobBoard) -> List[JobPosting]:
        """Scrape Workday with ROBUST Playwright implementation"""
        if not self.browser:
            logger.warning(f"Playwright not available for {board.company_name}")
            return []

        jobs = []
        page = None
        
        try:
            page = await self.browser.new_page()
            
            # Set longer timeout
            page.set_default_timeout(60000)
            
            logger.info(f"🌐 Loading Workday board: {board.board_url}")
            await page.goto(board.board_url, wait_until='domcontentloaded', timeout=30000)
            
            # Wait for page to settle
            await asyncio.sleep(5)
            
            # Try multiple selector strategies
            job_selectors = [
                # Common Workday selectors (actual from real sites)
                'li[class*="css-"][role="listitem"]',
                'a[data-automation-id="jobTitle"]',
                'div[data-automation-id="compositeContainer"] a',
                'li.css-1q2dra3',
                'a[href*="/job/"]',
                'div[class*="job"]',
            ]
            
            job_elements = []
            working_selector = None
            
            for selector in job_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    if elements and len(elements) > 0:
                        job_elements = elements
                        working_selector = selector
                        logger.info(f"✅ Found {len(elements)} jobs with selector: {selector}")
                        break
                except Exception as e:
                    continue
            
            if not job_elements:
                # Last resort: get all links and filter
                all_links = await page.query_selector_all('a')
                job_elements = []
                for link in all_links:
                    try:
                        href = await link.get_attribute('href')
                        text = await link.text_content()
                        if href and '/job/' in href and text and len(text) > 5:
                            job_elements.append(link)
                    except:
                        continue
                
                if job_elements:
                    logger.info(f"✅ Found {len(job_elements)} jobs via link filtering")
                else:
                    logger.warning(f"No jobs found for {board.company_name} - page might not have loaded")
                    # Debug: save screenshot
                    try:
                        await page.screenshot(path=f'/tmp/workday_{board.company_name}.png')
                        logger.info(f"📸 Screenshot saved for debugging")
                    except:
                        pass
                    return []
            
            # Extract job data
            for element in job_elements[:200]:  # Limit to 200
                try:
                    # Get title
                    title = await element.text_content()
                    if not title or len(title.strip()) < 3:
                        continue
                    
                    title = title.strip()
                    
                    # Get URL
                    href = await element.get_attribute('href')
                    if not href:
                        continue
                    
                    job_url = urljoin(board.board_url, href) if not href.startswith('http') else href
                    
                    # Generate ID from URL
                    job_id = job_url.split('/')[-1] or title.replace(' ', '-').lower()
                    
                    # Try to find location
                    location = None
                    try:
                        # Look for sibling or parent elements with location
                        parent = await element.evaluate('el => el.closest("li") || el.closest("div")')
                        if parent:
                            parent_text = await page.evaluate('el => el.textContent', parent)
                            # Extract location patterns
                            location_match = re.search(r'(Remote|Hybrid|[A-Z][a-z]+,\s*[A-Z]{2}|[A-Z][a-z]+\s+[A-Z][a-z]+)', parent_text)
                            if location_match:
                                location = location_match.group(1)
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
            
            logger.info(f"✅ Workday scraped {len(jobs)} jobs for {board.company_name}")
            
        except PlaywrightTimeout:
            logger.error(f"⏱️ Timeout loading Workday for {board.company_name}")
        except Exception as e:
            logger.error(f"❌ Workday scrape failed for {board.company_name}: {e}")
        finally:
            if page:
                await page.close()
        
        return jobs

    # ========================================================================
    # ASHBY SCRAPER (COMPLETELY REWRITTEN)
    # ========================================================================
    
    async def _scrape_ashby(self, board: JobBoard) -> List[JobPosting]:
        """Scrape Ashby with JSON API + Playwright fallback"""
        jobs = []
        client = await self._get_client()

        # Try JSON API first
        api_url = board.board_url.rstrip('/') + '/api/postings'
        try:
            async with client.get(api_url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list):
                        for job in data:
                            location_list = [loc.get('name') for loc in job.get('locationNames', []) if loc.get('name')]
                            location = ', '.join(location_list) if location_list else None
                            
                            jobs.append(JobPosting(
                                id=job.get('id', ''),
                                title=job.get('title', ''),
                                url=job.get('url', ''),
                                location=location,
                                department=job.get('departmentName'),
                                metadata=job
                            ))
                        logger.info(f"✅ Ashby JSON: {len(jobs)} jobs for {board.company_name}")
                        return jobs
        except Exception as e:
            logger.debug(f"Ashby API failed: {e}")

        # Playwright fallback
        if not self.browser:
            return []

        page = None
        try:
            page = await self.browser.new_page()
            await page.goto(board.board_url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(3)
            
            # Ashby-specific selectors
            selectors = [
                'a[href*="/jobs/"]',
                'div[class*="JobList"] a',
                'a[class*="job"]',
            ]
            
            job_links = []
            for selector in selectors:
                try:
                    job_links = await page.query_selector_all(selector)
                    if job_links:
                        logger.info(f"Found {len(job_links)} jobs with {selector}")
                        break
                except:
                    continue
            
            for link in job_links[:200]:
                try:
                    title = await link.text_content()
                    href = await link.get_attribute('href')
                    
                    if not title or not href:
                        continue
                    
                    job_url = href if href.startswith('http') else urljoin(board.board_url, href)
                    job_id = job_url.split('/')[-1]
                    
                    jobs.append(JobPosting(
                        id=job_id,
                        title=title.strip(),
                        url=job_url
                    ))
                except:
                    continue
            
            logger.info(f"✅ Ashby Playwright: {len(jobs)} jobs for {board.company_name}")
            
        except Exception as e:
            logger.error(f"Ashby Playwright failed for {board.company_name}: {e}")
        finally:
            if page:
                await page.close()
        
        return jobs

    # ========================================================================
    # MAIN SCRAPING LOGIC
    # ========================================================================

    async def scrape_board(self, board: JobBoard) -> JobBoard:
        """Scrape jobs from board"""
        logger.info(f"🔍 Scraping {board.ats_type} for {board.company_name}")
        
        if board.ats_type == 'greenhouse':
            board.jobs = await self._scrape_greenhouse(board)
        elif board.ats_type == 'lever':
            board.jobs = await self._scrape_lever(board)
        elif board.ats_type == 'workday':
            board.jobs = await self._scrape_workday(board)
        elif board.ats_type == 'ashby':
            board.jobs = await self._scrape_ashby(board)
            
        logger.info(f"✅ Scraped {len(board.jobs)} jobs from {board.company_name}")
        self.stats.total_jobs_collected += len(board.jobs)
        return board
    
    async def _discover_and_scrape(self, company_name: str):
        """Discover and scrape a single company"""
        async with self._semaphore:
            try:
                board = await self._test_company(company_name)
                if board:
                    self.stats.total_discovered += 1
                    board = await self.scrape_board(board)
                    
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

    async def run_discovery(self, max_companies: int = 500) -> CollectionStats:
        """Run discovery on seed companies"""
        await self.initialize_playwright()
        logger.info(f"🔍 Starting discovery on {max_companies} seeds")
        
        seeds = self.db.get_seeds(limit=max_companies, prioritize_quality=True)
        logger.info(f"📋 Testing {len(seeds)} seeds")
        
        tasks = []
        for seed in seeds:
            task = self._discover_and_scrape(seed['company_name'])
            tasks.append(task)
        
        batch_size = 20
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            await asyncio.gather(*batch, return_exceptions=True)
            await asyncio.sleep(2)
        
        try:
            self.db.blacklist_poor_seeds(min_tests=3, max_success_rate=5.0)
        except:
            pass
        
        self.stats.end_time = datetime.now()
        logger.info(f"✅ Discovery complete: {self.stats.total_discovered} companies, {self.stats.total_jobs_collected} jobs")
        return self.stats

    async def run_refresh(self, hours_since_update: int = 6, max_companies: int = 500) -> CollectionStats:
        """Refresh existing companies"""
        await self.initialize_playwright()
        stats = CollectionStats()
        
        companies = self.db.get_companies_for_refresh(hours_since_update, max_companies)
        logger.info(f"🔄 Refreshing {len(companies)} companies")
        
        for company in companies:
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
                    }
                    for job in board.jobs
                ])
                
                stats.total_jobs_collected += len(board.jobs)
                stats.total_new_jobs += new
                stats.total_updated_jobs += updated
                stats.total_closed_jobs += closed
                
            except Exception as e:
                logger.error(f"Error refreshing {company['company_name']}: {e}")
            
            await asyncio.sleep(1)
        
        stats.end_time = datetime.now()
        logger.info(f"✅ Refresh complete: {stats.total_jobs_collected} jobs")
        return stats


# Entry points
async def run_collection(max_companies: int = 500) -> CollectionStats:
    collector = JobIntelCollector()
    try:
        return await collector.run_discovery(max_companies=max_companies)
    finally:
        await collector.close()

async def run_refresh(hours_since_update: int = 6, max_companies: int = 500) -> CollectionStats:
    collector = JobIntelCollector()
    try:
        return await collector.run_refresh(hours_since_update, max_companies)
    finally:
        await collector.close()
