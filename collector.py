"""
Job Intelligence Collector
Discovers and scrapes job boards from various ATS platforms
"""

import asyncio
import aiohttp
import logging
from typing import List, Dict, Optional, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime
import json
import re
from urllib.parse import urljoin, quote

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from database import get_db, Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ua = UserAgent()

# ============================================================================
# Data Models
# ============================================================================

@dataclass
class JobPosting:
    """Individual job posting"""
    id: str
    title: str
    url: str
    location: Optional[str] = None
    department: Optional[str] = None
    work_type: Optional[str] = None
    posted_date: Optional[str] = None
    metadata: Dict = field(default_factory=dict)

@dataclass
class JobBoard:
    """Job board information"""
    company_name: str
    ats_type: str
    board_url: str
    jobs: List[JobPosting] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            'company_name': self.company_name,
            'ats_type': self.ats_type,
            'board_url': self.board_url,
            'job_count': len(self.jobs),
            'jobs': [
                {
                    'id': job.id,
                    'title': job.title,
                    'url': job.url,
                    'location': job.location,
                    'department': job.department,
                    'work_type': job.work_type,
                    'posted_date': job.posted_date,
                    'metadata': job.metadata
                }
                for job in self.jobs
            ]
        }

@dataclass
class CollectionStats:
    """Collection statistics"""
    total_tested: int = 0
    total_discovered: int = 0
    total_jobs_collected: int = 0
    total_new_jobs: int = 0
    total_updated_jobs: int = 0
    total_closed_jobs: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    
    def to_dict(self) -> Dict:
        return {
            'total_tested': self.total_tested,
            'total_discovered': self.total_discovered,
            'total_jobs_collected': self.total_jobs_collected,
            'total_new_jobs': self.total_new_jobs,
            'total_updated_jobs': self.total_updated_jobs,
            'total_closed_jobs': self.total_closed_jobs,
            'duration_seconds': (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        }

# ============================================================================
# Job Intelligence Collector
# ============================================================================

class JobIntelCollector:
    """Main collector for discovering companies and scraping job boards"""
    
    def __init__(self, db: Optional[Database] = None, progress_callback: Optional[Callable] = None):
        self.db = db or get_db()
        self.client: Optional[aiohttp.ClientSession] = None
        self.stats = CollectionStats()
        self.progress_callback = progress_callback
        self._semaphore = asyncio.Semaphore(10)
    
    async def _get_client(self) -> aiohttp.ClientSession:
        """Get or create aiohttp client session"""
        if self.client is None or self.client.closed:
            headers = {
                'User-Agent': ua.random,
                'Accept': 'text/html,application/json,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
            }
            self.client = aiohttp.ClientSession(
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=50, limit_per_host=10)
            )
        return self.client
    
    async def close(self):
        """Close client session"""
        if self.client and not self.client.closed:
            await self.client.close()
    
    def _update_progress(self, progress: float):
        """Update progress callback if provided"""
        if self.progress_callback:
            self.progress_callback(progress, self.stats.to_dict())
    
    # ========================================================================
    # ATS Detection Methods
    # ========================================================================
    
    async def _test_greenhouse(self, company_name: str) -> Optional[JobBoard]:
        """Test for Greenhouse ATS"""
        token = self.db._name_to_token(company_name)
        test_urls = [
            f"https://boards.greenhouse.io/{token}",
            f"https://boards.greenhouse.io/embed/job_board?for={token}",
            f"https://job-boards.greenhouse.io/{token}"
        ]
        
        client = await self._get_client()
        
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'greenhouse' in text.lower() or 'job_app' in text:
                            logger.info(f"✅ Found Greenhouse board: {company_name}")
                            return JobBoard(company_name, 'greenhouse', url)
            except Exception as e:
                logger.debug(f"Greenhouse test failed for {url}: {e}")
                continue
            
            await asyncio.sleep(0.5)
        
        return None
    
    async def _test_lever(self, company_name: str) -> Optional[JobBoard]:
        """Test for Lever ATS"""
        token = self.db._name_to_token(company_name)
        test_urls = [
            f"https://jobs.lever.co/{token}",
            f"https://{token}.lever.co"
        ]
        
        client = await self._get_client()
        
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'lever' in text.lower() or 'postings' in url:
                            logger.info(f"✅ Found Lever board: {company_name}")
                            return JobBoard(company_name, 'lever', url)
            except Exception as e:
                logger.debug(f"Lever test failed for {url}: {e}")
                continue
            
            await asyncio.sleep(0.5)
        
        return None
    
    async def _test_workday(self, company_name: str) -> Optional[JobBoard]:
        """Test for Workday ATS"""
        token = self.db._name_to_token(company_name)
        
        # Workday URLs are less predictable, try common patterns
        test_urls = [
            f"https://{token}.wd5.myworkdayjobs.com/{token}",
            f"https://{token}.wd1.myworkdayjobs.com/{token}",
            f"https://{token}.wd3.myworkdayjobs.com/{token}",
        ]
        
        client = await self._get_client()
        
        for url in test_urls:
            try:
                async with client.get(url, allow_redirects=True) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'workday' in text.lower() or 'myworkdayjobs' in str(resp.url):
                            logger.info(f"✅ Found Workday board: {company_name}")
                            return JobBoard(company_name, 'workday', str(resp.url))
            except Exception as e:
                logger.debug(f"Workday test failed for {url}: {e}")
                continue
            
            await asyncio.sleep(0.5)
        
        return None
    
    async def _test_ashby(self, company_name: str) -> Optional[JobBoard]:
        """Test for Ashby ATS"""
        token = self.db._name_to_token(company_name)
        test_urls = [
            f"https://jobs.ashbyhq.com/{token}",
            f"https://{token}.ashbyhq.com"
        ]
        
        client = await self._get_client()
        
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'ashby' in text.lower():
                            logger.info(f"✅ Found Ashby board: {company_name}")
                            return JobBoard(company_name, 'ashby', url)
            except Exception as e:
                logger.debug(f"Ashby test failed for {url}: {e}")
                continue
            
            await asyncio.sleep(0.5)
        
        return None
    
    async def _test_jobvite(self, company_name: str) -> Optional[JobBoard]:
        """Test for Jobvite ATS"""
        token = self.db._name_to_token(company_name)
        test_urls = [
            f"https://jobs.jobvite.com/{token}",
            f"https://{token}.jobvite.com"
        ]
        
        client = await self._get_client()
        
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'jobvite' in text.lower():
                            logger.info(f"✅ Found Jobvite board: {company_name}")
                            return JobBoard(company_name, 'jobvite', url)
            except Exception as e:
                logger.debug(f"Jobvite test failed for {url}: {e}")
                continue
            
            await asyncio.sleep(0.5)
        
        return None
    
    async def _test_smartrecruiters(self, company_name: str) -> Optional[JobBoard]:
        """Test for SmartRecruiters ATS"""
        token = self.db._name_to_token(company_name)
        test_urls = [
            f"https://jobs.smartrecruiters.com/{token}",
            f"https://{token}.smartrecruiters.com"
        ]
        
        client = await self._get_client()
        
        for url in test_urls:
            try:
                async with client.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        if 'smartrecruiters' in text.lower():
                            logger.info(f"✅ Found SmartRecruiters board: {company_name}")
                            return JobBoard(company_name, 'smartrecruiters', url)
            except Exception as e:
                logger.debug(f"SmartRecruiters test failed for {url}: {e}")
                continue
            
            await asyncio.sleep(0.5)
        
        return None
    
    async def _test_custom_board(self, company_name: str) -> Optional[JobBoard]:
        """Test for custom/unknown job boards (fallback)"""
        # This is a placeholder - could be expanded with more heuristics
        return None
    
    async def _test_company(self, company_name: str, board_hint: str = None) -> Optional[JobBoard]:
        """Test a single company for ATS detection - ALL PLATFORMS WITH SEED TRACKING"""
        self.stats.total_tested += 1
        
        # Update seed tracking - times_tested
        try:
            self.db.increment_seed_tested(company_name)
        except Exception as e:
            logger.debug(f"Could not update seed stats: {e}")
        
        # Test order - ALL PLATFORMS
        test_order = [
            ('greenhouse', self._test_greenhouse),
            ('lever', self._test_lever),
            ('workday', self._test_workday),
            ('ashby', self._test_ashby),
            ('jobvite', self._test_jobvite),
            ('smartrecruiters', self._test_smartrecruiters),
            ('custom', self._test_custom_board),
        ]
        
        # If hint provided, test that first
        if board_hint:
            for ats_type, test_func in test_order:
                if ats_type == board_hint.lower():
                    board = await test_func(company_name)
                    if board:
                        # Update seed tracking - success!
                        try:
                            self.db.increment_seed_success(company_name)
                        except Exception as e:
                            logger.debug(f"Could not update seed success: {e}")
                        return board
                    break  # If hint fails, try all others
        
        # Test all platforms
        for ats_type, test_func in test_order:
            if board_hint and ats_type == board_hint.lower():
                continue  # Already tested
            
            board = await test_func(company_name)
            if board:
                # Update seed tracking - success!
                try:
                    self.db.increment_seed_success(company_name)
                except Exception as e:
                    logger.debug(f"Could not update seed success: {e}")
                return board
            
            await asyncio.sleep(0.5)  # Small delay between tests
        
        return None
    
    # ========================================================================
    # Job Scraping Methods
    # ========================================================================
    
    async def _scrape_greenhouse(self, board: JobBoard) -> List[JobPosting]:
        """Scrape Greenhouse job board"""
        jobs = []
        
        try:
            client = await self._get_client()
            
            # Try JSON API first
            api_url = board.board_url.replace('/embed/job_board?for=', '/embed/job_board/jobs?for=')
            
            try:
                async with client.get(api_url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        
                        if isinstance(data, dict) and 'jobs' in data:
                            for job in data['jobs']:
                                jobs.append(JobPosting(
                                    id=str(job.get('id', job.get('title', ''))),
                                    title=job.get('title', ''),
                                    url=job.get('absolute_url', ''),
                                    location=job.get('location', {}).get('name') if isinstance(job.get('location'), dict) else job.get('location'),
                                    department=job.get('departments', [{}])[0].get('name') if job.get('departments') else None,
                                    metadata=job
                                ))
                        
                        return jobs
            except:
                pass  # Fall back to HTML scraping
            
            # HTML scraping fallback
            async with client.get(board.board_url) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    
                    for job_elem in soup.find_all(['div', 'a'], class_=re.compile(r'opening|job|position')):
                        title_elem = job_elem.find(['h3', 'h4', 'span', 'a'])
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                            url = job_elem.get('href', '') or job_elem.find('a', href=True)
                            if url:
                                url = urljoin(board.board_url, url.get('href') if hasattr(url, 'get') else url)
                            
                            jobs.append(JobPosting(
                                id=title,
                                title=title,
                                url=url
                            ))
        
        except Exception as e:
            logger.error(f"Error scraping Greenhouse board: {e}")
        
        return jobs
    
    async def _scrape_lever(self, board: JobBoard) -> List[JobPosting]:
        """Scrape Lever job board"""
        jobs = []
        
        try:
            client = await self._get_client()
            
            # Lever has JSON API
            api_url = board.board_url.rstrip('/') + '/postings'
            
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
        
        except Exception as e:
            logger.error(f"Error scraping Lever board: {e}")
        
        return jobs
    
    async def _scrape_workday(self, board: JobBoard) -> List[JobPosting]:
        """Scrape Workday job board"""
        jobs = []
        
        try:
            client = await self._get_client()
            
            async with client.get(board.board_url) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    
                    # Workday uses various structures
                    for job_elem in soup.find_all(['li', 'div'], class_=re.compile(r'job|position')):
                        title_elem = job_elem.find(['h3', 'a'])
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                            url = job_elem.find('a', href=True)
                            if url:
                                url = urljoin(board.board_url, url['href'])
                            
                            jobs.append(JobPosting(
                                id=title,
                                title=title,
                                url=url
                            ))
        
        except Exception as e:
            logger.error(f"Error scraping Workday board: {e}")
        
        return jobs
    
    async def _scrape_ashby(self, board: JobBoard) -> List[JobPosting]:
        """Scrape Ashby job board"""
        jobs = []
        
        try:
            client = await self._get_client()
            
            async with client.get(board.board_url) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    
                    for job_elem in soup.find_all(['div', 'a'], class_=re.compile(r'job|posting')):
                        title_elem = job_elem.find(['h3', 'h4', 'span'])
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                            url = job_elem.get('href', '') or job_elem.find('a', href=True)
                            if url:
                                url = urljoin(board.board_url, url.get('href') if hasattr(url, 'get') else url)
                            
                            jobs.append(JobPosting(
                                id=title,
                                title=title,
                                url=url
                            ))
        
        except Exception as e:
            logger.error(f"Error scraping Ashby board: {e}")
        
        return jobs
    
    async def _scrape_jobvite(self, board: JobBoard) -> List[JobPosting]:
        """Scrape Jobvite job board"""
        jobs = []
        
        try:
            client = await self._get_client()
            
            async with client.get(board.board_url) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    
                    for job_elem in soup.find_all(['tr', 'div'], class_=re.compile(r'job|position')):
                        title_elem = job_elem.find(['td', 'a', 'span'])
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                            url = job_elem.find('a', href=True)
                            if url:
                                url = urljoin(board.board_url, url['href'])
                            
                            jobs.append(JobPosting(
                                id=title,
                                title=title,
                                url=url
                            ))
        
        except Exception as e:
            logger.error(f"Error scraping Jobvite board: {e}")
        
        return jobs
    
    async def _scrape_smartrecruiters(self, board: JobBoard) -> List[JobPosting]:
        """Scrape SmartRecruiters job board"""
        jobs = []
        
        try:
            client = await self._get_client()
            
            async with client.get(board.board_url) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    
                    for job_elem in soup.find_all(['li', 'div'], class_=re.compile(r'job|opening')):
                        title_elem = job_elem.find(['h4', 'a', 'span'])
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                            url = job_elem.find('a', href=True)
                            if url:
                                url = urljoin(board.board_url, url['href'])
                            
                            jobs.append(JobPosting(
                                id=title,
                                title=title,
                                url=url
                            ))
        
        except Exception as e:
            logger.error(f"Error scraping SmartRecruiters board: {e}")
        
        return jobs
    
    async def scrape_board(self, board: JobBoard) -> JobBoard:
        """Scrape jobs from a detected board"""
        logger.info(f"🔍 Scraping {board.ats_type} board for {board.company_name}")
        
        # Route to appropriate scraper
        if board.ats_type == 'greenhouse':
            board.jobs = await self._scrape_greenhouse(board)
        elif board.ats_type == 'lever':
            board.jobs = await self._scrape_lever(board)
        elif board.ats_type == 'workday':
            board.jobs = await self._scrape_workday(board)
        elif board.ats_type == 'ashby':
            board.jobs = await self._scrape_ashby(board)
        elif board.ats_type == 'jobvite':
            board.jobs = await self._scrape_jobvite(board)
        elif board.ats_type == 'smartrecruiters':
            board.jobs = await self._scrape_smartrecruiters(board)
        
        logger.info(f"✅ Scraped {len(board.jobs)} jobs from {board.company_name}")
        self.stats.total_jobs_collected += len(board.jobs)
        
        return board
    
    # ========================================================================
    # Main Collection Workflows
    # ========================================================================
    
    async def test_single_company(self, company_name: str, website_url: str = None, ats_hint: str = None) -> Optional[JobBoard]:
        """Test and scrape a single company (for manual submissions)"""
        logger.info(f"Testing single company: {company_name}")
        
        board = await self._test_company(company_name, ats_hint)
        
        if board:
            board = await self.scrape_board(board)
            
            # Add to database
            company_id = self.db.add_company(
                company_name=board.company_name,
                ats_type=board.ats_type,
                board_url=board.board_url,
                job_count=len(board.jobs)
            )
            
            if company_id:
                # Archive jobs
                new, updated, closed = self.db.archive_jobs(company_id, [
                    {
                        'id': job.id,
                        'title': job.title,
                        'location': job.location,
                        'department': job.department,
                        'work_type': job.work_type,
                        'url': job.url,
                        'posted_date': job.posted_date,
                        'metadata': job.metadata
                    }
                    for job in board.jobs
                ])
                
                logger.info(f"✅ Added {company_name}: {new} new jobs, {updated} updated, {closed} closed")
            
            return board
        
        logger.info(f"❌ No ATS found for {company_name}")
        return None
    
    async def run_discovery(self, max_companies: int = 500) -> CollectionStats:
        """Run company discovery - PRIORITIZE QUALITY SEEDS"""
        logger.info(f"🔍 Starting discovery on up to {max_companies} seed companies")
        
        # Get seeds prioritized by success rate
        seeds = self.db.get_seeds(limit=max_companies, prioritize_quality=True)
        
        logger.info(f"📋 Testing {len(seeds)} seeds (prioritized by quality)")
        
        tasks = []
        for seed in seeds:
            task = self._discover_and_scrape(seed['company_name'])
            tasks.append(task)
        
        # Process in batches
        batch_size = 20
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            await asyncio.gather(*batch, return_exceptions=True)
            
            progress = min(100, (i + batch_size) / len(tasks) * 100)
            self._update_progress(progress)
            
            await asyncio.sleep(2)  # Rate limiting between batches
        
        # After testing, blacklist poor performers
        try:
            blacklisted = self.db.blacklist_poor_seeds(min_tests=3, max_success_rate=5.0)
            if blacklisted:
                logger.info(f"🚫 Blacklisted {blacklisted} poor-performing seeds")
        except Exception as e:
            logger.debug(f"Seed blacklisting not available: {e}")
        
        self.stats.end_time = datetime.now()
        logger.info(f"✅ Discovery complete: {self.stats.total_discovered} companies, {self.stats.total_jobs_collected} jobs")
        
        return self.stats
    
    async def _discover_and_scrape(self, company_name: str):
        """Test, discover, and scrape a company"""
        async with self._semaphore:
            try:
                board = await self._test_company(company_name)
                
                if board:
                    self.stats.total_discovered += 1
                    
                    # Scrape jobs
                    board = await self.scrape_board(board)
                    
                    # Add to database
                    company_id = self.db.add_company(
                        company_name=board.company_name,
                        ats_type=board.ats_type,
                        board_url=board.board_url,
                        job_count=len(board.jobs)
                    )
                    
                    if company_id:
                        # Archive jobs
                        new, updated, closed = self.db.archive_jobs(company_id, [
                            {
                                'id': job.id,
                                'title': job.title,
                                'location': job.location,
                                'department': job.department,
                                'work_type': job.work_type,
                                'url': job.url,
                                'posted_date': job.posted_date,
                                'metadata': job.metadata
                            }
                            for job in board.jobs
                        ])
                        
                        self.stats.total_new_jobs += new
                        self.stats.total_updated_jobs += updated
                        self.stats.total_closed_jobs += closed
            
            except Exception as e:
                logger.error(f"Error processing {company_name}: {e}")

# ============================================================================
# Convenience Functions
# ============================================================================

async def run_collection(max_companies: int = 500) -> CollectionStats:
    """Run company discovery collection"""
    collector = JobIntelCollector()
    try:
        return await collector.run_discovery(max_companies=max_companies)
    finally:
        await collector.close()

async def run_refresh(hours_since_update: int = 6, max_companies: int = 500) -> CollectionStats:
    """Refresh existing companies"""
    collector = JobIntelCollector()
    stats = CollectionStats()
    
    try:
        companies = collector.db.get_companies_for_refresh(hours_since_update, max_companies)
        logger.info(f"🔄 Refreshing {len(companies)} companies")
        
        for company in companies:
            board = JobBoard(
                company_name=company['company_name'],
                ats_type=company['ats_type'],
                board_url=company['board_url']
            )
            
            board = await collector.scrape_board(board)
            
            # Update database
            collector.db.update_company_job_count(company['id'], len(board.jobs))
            
            # Archive jobs
            new, updated, closed = collector.db.archive_jobs(company['id'], [
                {
                    'id': job.id,
                    'title': job.title,
                    'location': job.location,
                    'department': job.department,
                    'work_type': job.work_type,
                    'url': job.url,
                    'posted_date': job.posted_date,
                    'metadata': job.metadata
                }
                for job in board.jobs
            ])
            
            stats.total_jobs_collected += len(board.jobs)
            stats.total_new_jobs += new
            stats.total_updated_jobs += updated
            stats.total_closed_jobs += closed
        
        stats.end_time = datetime.now()
        logger.info(f"✅ Refresh complete: {stats.total_jobs_collected} jobs processed")
        
        return stats
    
    finally:
        await collector.close()
