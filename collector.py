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
                min_sal = int(min_sal) * 1000
                max_sal = int(max_sal) * 1000
            else:
                min_sal = int(min_sal)
                max_sal = int(max_sal)
            
            return {
                'salary_min': min_sal,
                'salary_max': max_sal,
                'salary_currency': 'USD'
            }
        
        return {}
    
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
                async with client.get(url) as resp:
                    if resp.status == 200:
                        logger.info(f"✅ Found Workday: {company_name}")
                        return JobBoard(company_name, 'workday', url)
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
    
    async def _scrape_greenhouse(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        try:
            client = await self._get_client()
            api_url = board.board_url.replace('/embed/job_board?for=', '/embed/job_board/jobs?for=')
            try:
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
                        return jobs
            except:
                pass
        except Exception as e:
            logger.error(f"Error scraping Greenhouse: {e}")
        return jobs
    
    async def _scrape_lever(self, board: JobBoard) -> List[JobPosting]:
        jobs = []
        try:
            client = await self._get_client()
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
        except Exception as e:
            logger.error(f"Error scraping Lever: {e}")
        return jobs
    
    async def scrape_board(self, board: JobBoard) -> JobBoard:
        logger.info(f"🔍 Scraping {board.ats_type} for {board.company_name}")
        if board.ats_type == 'greenhouse':
            board.jobs = await self._scrape_greenhouse(board)
        elif board.ats_type == 'lever':
            board.jobs = await self._scrape_lever(board)
        logger.info(f"✅ Scraped {len(board.jobs)} jobs from {board.company_name}")
        self.stats.total_jobs_collected += len(board.jobs)
        return board
    
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
