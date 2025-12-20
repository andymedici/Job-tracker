"""Job Intelligence Collector"""

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
        if self.client and not self.client.closed:
            await self.client.close()
    
    def _extract_salary(self, text: str) -> Dict:
        """Extract salary information from text"""
        if not text:
            return {}
        
        # Look for patterns like "$100k-$150k", "$100,000 - $150,000"
        pattern = r'\$(\d{1,3}(?:,\d{3})*|\d+)k?\s*-\s*\$?(\d{1,3}(?:,\d{3})*|\d+)k?'
        match = re.search(pattern, text, re.IGNORECASE)
        
        if match:
            min_sal = match.group(1).replace(',', '')
            max_sal = match.group(2).replace(',', '')
            
            # Handle 'k' notation
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
    
    # --- ATS Testing Functions (No Change) ---
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
                        if 'greenhouse' in text.lower():
                            logger.info(f"✅ Found Greenhouse: {company_name}")
                            # Use the embed URL as it's closer to the API path
                            if 'embed' in url:
                                return JobBoard(company_name, 'greenhouse', url)
                            # Fallback to the main board if only that is found
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
        test_urls = [
            f"https://{token}.wd5.myworkdayjobs.com/{token}",
            f"https://{token}.wd1.myworkdayjobs.com/{token}",
        ]
        client = await self._get_client()
        for url in test_urls:
            try:
                async with client.get(url, allow_redirects=True) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'myworkdayjobs.com' in str(resp.url):
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
    # --- End ATS Testing Functions ---

    async def _test_company(self, company_name: str, board_hint: str = None) -> Optional[JobBoard]:
        """Test company - ONLY increment discovered if board found"""
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
    
    # --- SCRAPER FUNCTIONS WITH FIXES/ADDITIONS ---
    
    async def _scrape_greenhouse(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        client = await self._get_client()
        
        # 1. Try JSON API first (most reliable, your existing logic)
        try:
            # Change the board URL (which is often the embed HTML page) to the JSON endpoint
            api_url = board.board_url.replace('/embed/job_board?for=', '/embed/job_board/jobs?for=')
            
            # If the URL is the main board, try to infer the JSON API token
            if 'embed' not in board.board_url:
                token_match = re.search(r'greenhouse.io/(\w+)', board.board_url)
                if token_match:
                    token = token_match.group(1)
                    api_url = f"https://boards.greenhouse.io/embed/job_board/jobs?for={token}"

            async with client.get(api_url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, dict) and 'jobs' in data:
                        for job in data['jobs']:
                            salary_data = self._extract_salary(str(job))
                            jobs.append(JobPosting(
                                id=str(job.get('id', job.get('title', ''))),
                                title=job.get('title', ''),
                                url=job.get('absolute_url', ''),
                                location=job.get('location', {}).get('name') if isinstance(job.get('location'), dict) else job.get('location'),
                                department=job.get('departments', [{}])[0].get('name') if job.get('departments') else None,
                                salary_min=salary_data.get('salary_min'),
                                salary_max=salary_data.get('salary_max'),
                                salary_currency=salary_data.get('salary_currency'),
                                metadata=job
                            ))
                        if jobs:
                            logger.info(f"✅ Greenhouse JSON API Success for {board.company_name}")
                            return jobs
        except Exception as e:
            logger.warning(f"Greenhouse JSON API failed for {board.company_name}. Falling back to HTML scrape. Error: {e}")

        # 2. Fallback to HTML scrape (for non-embed/non-API access boards)
        try:
            async with client.get(board.board_url) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    
                    # Target selector for job listings on a standard Greenhouse HTML page
                    job_links = soup.select('div.posting, a.job-link')
                    
                    for link in job_links:
                        title = link.select_one('h4, a')
                        location = link.select_one('span.location')
                        
                        if title and 'href' in link.attrs:
                             # Ensure we extract the URL from the parent div if necessary
                            job_url = urljoin(board.board_url, link.attrs['href']) if link.name == 'a' else urljoin(board.board_url, link.select_one('a')['href'])
                            
                            jobs.append(JobPosting(
                                id=job_url.split('/')[-1], # Use a unique part of URL as ID fallback
                                title=title.get_text(strip=True),
                                url=job_url,
                                location=location.get_text(strip=True) if location else None,
                            ))
            logger.info(f"✅ Greenhouse HTML Scrape found {len(jobs)} jobs for {board.company_name}")
            return jobs

        except Exception as e:
            logger.error(f"Error scraping Greenhouse (HTML fallback) for {board.company_name}: {e}")
            return []

    async def _scrape_lever(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        client = await self._get_client()
        
        # 1. Try JSON API (your existing logic)
        try:
            api_url = board.board_url.rstrip('/') + '/postings'
            async with client.get(api_url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for job in data:
                        salary_data = self._extract_salary(str(job))
                        jobs.append(JobPosting(
                            id=job.get('id', ''),
                            title=job.get('text', ''),
                            url=job.get('hostedUrl', ''),
                            location=job.get('categories', {}).get('location'),
                            department=job.get('categories', {}).get('team'),
                            work_type=job.get('categories', {}).get('commitment'),
                            posted_date=job.get('createdAt'),
                            salary_min=salary_data.get('salary_min'),
                            salary_max=salary_data.get('salary_max'),
                            salary_currency=salary_data.get('salary_currency'),
                            metadata=job
                        ))
                    if jobs:
                        logger.info(f"✅ Lever JSON API Success for {board.company_name}")
                        return jobs
        except Exception as e:
            logger.warning(f"Lever JSON API failed for {board.company_name}. Falling back to HTML scrape. Error: {e}")

        # 2. Fallback to HTML scrape (for when the JSON API is restricted)
        try:
            async with client.get(board.board_url) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    
                    # Selector for job listings on a standard Lever HTML page
                    job_links = soup.select('.posting')
                    
                    for link in job_links:
                        title_el = link.select_one('h5')
                        location_el = link.select_one('.location')
                        
                        if title_el and 'data-qa-posting-url' in link.attrs:
                            job_url = link.attrs['data-qa-posting-url']
                            
                            jobs.append(JobPosting(
                                id=job_url.split('/')[-1],
                                title=title_el.get_text(strip=True),
                                url=job_url,
                                location=location_el.get_text(strip=True) if location_el else None,
                            ))
            logger.info(f"✅ Lever HTML Scrape found {len(jobs)} jobs for {board.company_name}")
            return jobs
        
        except Exception as e:
            logger.error(f"Error scraping Lever (HTML fallback) for {board.company_name}: {e}")
            return []

    async def _scrape_workday(self, board: JobBoard) -> List[JobPosting]:
        """
        Scrapes Workday using the most common hidden JSON API approach.
        This is a best-effort scrape without a full headless browser.
        """
        jobs = []
        client = await self._get_client()
        
        # Workday job board URL is often in the format: 
        # https://<token>.wd<N>.myworkdayjobs.com/<BoardName>/<Path>
        # The API request often goes to /jobs
        api_url = urljoin(board.board_url.split('/job/')[0] + '/', 'fs/search/results')
        
        # This is a common Workday POST payload to get the first page of jobs
        payload = {
            "appliedFacets": {},
            "limit": 20,
            "offset": 0,
            "searchText": ""
        }
        
        try:
            # Need to get the session cookie first, then POST to the search endpoint
            async with client.get(board.board_url, allow_redirects=True) as resp:
                if resp.status != 200:
                    logger.warning(f"Workday initial GET failed with status {resp.status} for {board.company_name}")
                    return jobs
                
            # Now, attempt the POST to the search API
            async with client.post(
                api_url, 
                json=payload, 
                headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if 'jobPostings' in data.get('body', {}).get('children', [{}])[0].get('children', [{}])[0]:
                        job_list = data['body']['children'][0]['children'][0]['jobPostings']
                        
                        for job_data in job_list:
                            job_id = job_data.get('title', {}).get('commandLink', '').split('/')[-1]
                            title = job_data.get('title', {}).get('title', '')
                            # Workday URLs are constructed from the board URL base
                            job_url_path = job_data.get('title', {}).get('commandLink', '')
                            if job_url_path:
                                # The URL typically uses the path from the base board URL
                                base_url = board.board_url.split('/job/')[0]
                                job_url = urljoin(base_url, job_url_path)
                            else:
                                job_url = ''
                            
                            jobs.append(JobPosting(
                                id=job_id,
                                title=title,
                                url=job_url,
                                location=job_data.get('locationsText', ''),
                                metadata=job_data
                            ))
                        logger.info(f"✅ Workday JSON API Success for {board.company_name}. Found {len(jobs)} jobs (Page 1).")
                        # Note: Paging logic (fetching limit/offset > 0) is omitted for simplicity/initial fix.
                        return jobs

        except Exception as e:
            logger.error(f"Error scraping Workday for {board.company_name}: {e}")
            return []

    async def _scrape_ashby(self, board: JobBoard) -> List[JobPosting]:
        """Scrapes Ashby using its standard JSON API endpoint."""
        jobs = []
        client = await self._get_client()
        
        # Ashby's JSON API is usually at /api/postings
        api_url = board.board_url.rstrip('/') + '/api/postings'
        
        try:
            async with client.get(api_url, headers={'Accept': 'application/json'}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    if isinstance(data, list):
                        for job in data:
                            # Extracting all details from the JSON payload
                            location_list = [loc.get('name') for loc in job.get('locationNames', []) if loc.get('name')]
                            location = ', '.join(location_list) if location_list else None
                            
                            work_type = 'Full-time' # Ashby generally full-time, but can check job details if needed
                            
                            jobs.append(JobPosting(
                                id=job.get('id', ''),
                                title=job.get('title', ''),
                                url=job.get('url', ''),
                                location=location,
                                department=job.get('departmentName', None),
                                work_type=work_type,
                                # salary data is rarely in this top-level JSON
                                metadata=job
                            ))
                        logger.info(f"✅ Ashby JSON API Success for {board.company_name}")
                        return jobs
        except Exception as e:
            logger.error(f"Error scraping Ashby for {board.company_name}: {e}")
            return []

    async def scrape_board(self, board: JobBoard) -> JobBoard:
        logger.info(f"🔍 Scraping {board.ats_type} for {board.company_name}")
        if board.ats_type == 'greenhouse':
            board.jobs = await self._scrape_greenhouse(board)
        elif board.ats_type == 'lever':
            board.jobs = await self._scrape_lever(board)
        elif board.ats_type == 'workday':
            board.jobs = await self._scrape_workday(board)
        elif board.ats_type == 'ashby':
            board.jobs = await self._scrape_ashby(board)
        # Add other ATS scrapers here
            
        logger.info(f"✅ Scraped {len(board.jobs)} jobs from {board.company_name}")
        self.stats.total_jobs_collected += len(board.jobs)
        return board
    
    # --- Remaining Logic (No Change) ---
    async def run_discovery(self, max_companies: int = 500) -> CollectionStats:
        """Run discovery - FIXED stats"""
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
    
    async def _discover_and_scrape(self, company_name: str):
        """Discover and scrape - ONLY count as discovered if board found"""
        async with self._semaphore:
            try:
                board = await self._test_company(company_name)
                if board:
                    self.stats.total_discovered += 1  # ONLY increment if found
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

async def run_collection(max_companies: int = 500) -> CollectionStats:
    collector = JobIntelCollector()
    try:
        return await collector.run_discovery(max_companies=max_companies)
    finally:
        await collector.close()

async def run_refresh(hours_since_update: int = 6, max_companies: int = 500) -> CollectionStats:
    collector = JobIntelCollector()
    stats = CollectionStats()
    try:
        companies = collector.db.get_companies_for_refresh(hours_since_update, max_companies)
        logger.info(f"🔄 Refreshing {len(companies)} companies")
        for company in companies:
            board = JobBoard(company['company_name'], company['ats_type'], company['board_url'])
            board = await collector.scrape_board(board)
            collector.db.update_company_job_count(company['id'], len(board.jobs))
            new, updated, closed = collector.db.archive_jobs(company['id'], [
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
        stats.end_time = datetime.now()
        logger.info(f"✅ Refresh complete: {stats.total_jobs_collected} jobs")
        return stats
    finally:
        await collector.close()
