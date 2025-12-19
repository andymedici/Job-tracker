"""
Ultimate Seed Expander - Comprehensive Company Discovery System
Version 2.0 - Production Grade

Sources (15+):
- Tier 1: Curated tech companies, YC, manually verified
- Tier 2: Public companies (S&P 500, Fortune, SEC)
- Tier 3: Startup ecosystems (Crunchbase samples, GitHub lists)
- Tier 4: Mass expansion (comprehensive GitHub repos)
"""

import asyncio
import aiohttp
import json
import re
import logging
import random
import time
from typing import List, Tuple, Dict, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import quote

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from database import get_db, Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ua = UserAgent()

# ============================================================================
# SOURCE CONFIGURATION
# ============================================================================

@dataclass
class SourceConfig:
    """Configuration for each data source"""
    name: str
    tier: int
    priority: int
    url: Optional[str] = None
    enabled: bool = True
    rate_limit: float = 3.0  # Seconds between requests
    max_retries: int = 3
    timeout: int = 60
    description: str = ""

# Comprehensive source definitions
SOURCES = {
    # ===== TIER 1: HIGH-QUALITY TECH COMPANIES (95-90) =====
    'manual_verified': SourceConfig(
        name='Manually Verified Companies',
        tier=1,
        priority=100,
        description='Hand-curated list of known Greenhouse/Lever users'
    ),
    
    'yc_directory': SourceConfig(
        name='Y Combinator Companies',
        tier=1,
        priority=95,
        url='https://www.ycombinator.com/companies',
        description='All YC-backed startups'
    ),
    
    'yc_top_companies': SourceConfig(
        name='YC Top Companies',
        tier=1,
        priority=94,
        url='https://www.ycombinator.com/topcompanies',
        description='YC unicorns and high-growth companies'
    ),
    
    'techcrunch_startups': SourceConfig(
        name='TechCrunch Startup Database',
        tier=1,
        priority=93,
        url='https://techcrunch.com/startups/',
        description='Featured tech startups'
    ),
    
    'builtin_best_places': SourceConfig(
        name='Built In Best Places to Work',
        tier=1,
        priority=92,
        url='https://builtin.com/awards',
        description='Award-winning tech employers'
    ),
    
    'ats_curated_faang_plus': SourceConfig(
        name='FAANG+ & Unicorns',
        tier=1,
        priority=91,
        description='Major tech companies known to use modern ATS'
    ),
    
    'deloitte_fast500': SourceConfig(
        name='Deloitte Technology Fast 500',
        tier=1,
        priority=90,
        url='https://www.deloitte.com/us/en/Industries/tmt/articles/fast500-winners.html',
        description='Fastest-growing tech companies'
    ),
    
    # ===== TIER 2: PUBLIC COMPANIES & ESTABLISHED FIRMS (89-80) =====
    'sec_tickers': SourceConfig(
        name='SEC Company Tickers',
        tier=2,
        priority=89,
        url='https://www.sec.gov/files/company_tickers.json',
        description='All SEC-registered companies'
    ),
    
    'wikipedia_sp500': SourceConfig(
        name='S&P 500 Companies',
        tier=2,
        priority=88,
        url='https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
        description='S&P 500 constituents'
    ),
    
    'fortune_500': SourceConfig(
        name='Fortune 500',
        tier=2,
        priority=87,
        url='https://fortune.com/ranking/fortune500/',
        description='Largest US companies by revenue'
    ),
    
    'inc_5000': SourceConfig(
        name='Inc 5000',
        tier=2,
        priority=86,
        url='https://www.inc.com/inc5000',
        description='Fastest-growing private companies'
    ),
    
    'forbes_global_2000': SourceConfig(
        name='Forbes Global 2000',
        tier=2,
        priority=85,
        url='https://www.forbes.com/lists/global2000/',
        description='World\'s largest public companies'
    ),
    
    'nasdaq_100': SourceConfig(
        name='NASDAQ-100',
        tier=2,
        priority=84,
        url='https://en.wikipedia.org/wiki/Nasdaq-100',
        description='Top 100 non-financial NASDAQ companies'
    ),
    
    # ===== TIER 3: STARTUP ECOSYSTEMS & CURATED LISTS (79-70) =====
    'crunchbase_samples': SourceConfig(
        name='Crunchbase Dataset Samples',
        tier=3,
        priority=79,
        url='https://raw.githubusercontent.com/luminati-io/Crunchbase-dataset-samples/main/crunchbase_companies_1001.json',
        description='Crunchbase company samples'
    ),
    
    'b2b_dataset_samples': SourceConfig(
        name='B2B Business Dataset',
        tier=3,
        priority=78,
        url='https://raw.githubusercontent.com/luminati-io/B2B-business-dataset-samples/main/b2b_business_500.json',
        description='B2B company samples'
    ),
    
    'more_than_faangm': SourceConfig(
        name='moreThanFAANGM List',
        tier=3,
        priority=77,
        url='https://raw.githubusercontent.com/Kaustubh-Natuskar/moreThanFAANGM/master/README.md',
        description='400+ product companies beyond FAANG'
    ),
    
    'github_ventures': SourceConfig(
        name='GitHub Ventures Companies',
        tier=3,
        priority=76,
        url='https://github.com/github/ventures',
        description='GitHub-backed companies'
    ),
    
    # ===== TIER 4: MASS EXPANSION (69-60) =====
    'github_massive_list': SourceConfig(
        name='GitHub Massive Company List',
        tier=4,
        priority=69,
        url='https://gist.githubusercontent.com/bojanbabic/f007ffd83ea20b1ac48812131325851e/raw/',
        description='Comprehensive company name list (10k+)',
        rate_limit=5.0
    ),
    
    'heavy_pint_business': SourceConfig(
        name='Heavy Pint Business Names',
        tier=4,
        priority=68,
        url='https://raw.githubusercontent.com/9b/heavy_pint/master/lists/business-names.txt',
        description='Business names dataset'
    ),
}

# ============================================================================
# MANUALLY VERIFIED COMPANIES (TIER 1)
# ============================================================================

MANUAL_VERIFIED_COMPANIES = [
    # User-provided known Greenhouse/Lever users
    'Zentai', 'Brightcove', 'Matic', 'Fletcher Jones Imports', 'CompanyCam',
    'IonQ', 'Wiz', 'PayStand', 'SlickDeals', 'Validation Cloud', 'Vega',
    
    # User-provided comprehensive list
    'SpaceX', 'Cloudflare', 'Cisco', 'DoorDash', 'DocuSign', 'Dropbox',
    'HubSpot', 'Stripe', 'Pinterest', 'Squarespace', 'Wayfair', 'GoDaddy',
    'Warby Parker', 'Lyft', 'Oscar Health', 'Tencent', 'PlayStation',
    'Canonical', 'Okta', 'CarGurus', 'Affirm', 'Betterment', 'TripAdvisor',
    'Vimeo', 'Instacart', 'Evernote', 'Foursquare', 'Notion', 'Postman',
    'Outlier AI', 'Unity Technologies', 'Anduril Industries', '10x Genomics',
    'Toyota Motor Corporation', 'Accenture', 'UPS', 'AB Global',
    'Earnest Operations', 'Ouihelp', 'Asset Living', 'TÜV Rheinland', 'Onica',
    
    # FAANG+
    'Google', 'Apple', 'Meta', 'Amazon', 'Netflix', 'Microsoft', 'Tesla',
    
    # Major unicorns & high-growth
    'Anthropic', 'OpenAI', 'Scale AI', 'Databricks', 'Canva', 'Figma',
    'Brex', 'Rippling', 'Plaid', 'Chime', 'Robinhood', 'Coinbase',
    'Cruise', 'Aurora', 'Zoox', 'Waymo', 'Nuro',
    
    # Major ATS-using companies
    'Airbnb', 'Reddit', 'Slack', 'Atlassian', 'Salesforce', 'Workday',
    'ServiceNow', 'Snowflake', 'MongoDB', 'Elastic', 'HashiCorp',
    'GitLab', 'GitHub', 'Vercel', 'Netlify', 'Heroku', 'Render',
    
    # Enterprise software
    'Asana', 'Monday.com', 'ClickUp', 'Airtable', 'Notion',
    'Zapier', 'Retool', 'Webflow', 'Bubble', 'Glide',
    
    # Fintech
    'Square', 'PayPal', 'Venmo', 'Cash App', 'Adyen', 'Marqeta',
    'Checkout.com', 'Sezzle', 'Klarna', 'Afterpay',
    
    # Cloud/DevOps
    'Datadog', 'New Relic', 'PagerDuty', 'LaunchDarkly', 'Temporal',
    'Chronosphere', 'Observe', 'Honeycomb', 'Lightstep',
    
    # Security
    'CrowdStrike', 'Okta', 'Auth0', 'OneLogin', 'Duo Security',
    'Snyk', 'Lacework', 'Wiz', 'Orca Security', 'Aqua Security',
    
    # AI/ML
    'Hugging Face', 'Cohere', 'Stability AI', 'Midjourney', 'Runway',
    'Character AI', 'Jasper', 'Copy.ai', 'Descript', 'Synthesia',
    
    # E-commerce/Marketplaces
    'Shopify', 'BigCommerce', 'WooCommerce', 'Etsy', 'Poshmark',
    'StockX', 'GOAT', 'Faire', 'Ankorstore', 'Modalyst',
    
    # Healthcare tech
    'Oscar Health', 'Devoted Health', 'Ro', 'Hims & Hers', 'Nurx',
    'Omada Health', 'Livongo', 'Teladoc', 'Amwell', 'MDLive',
    
    # Real estate tech
    'Zillow', 'Redfin', 'Opendoor', 'Offerpad', 'Compass',
    'Better.com', 'Divvy Homes', 'Arrived', 'Fundrise',
    
    # EdTech
    'Coursera', 'Udemy', 'Udacity', 'Duolingo', 'Chegg',
    'Course Hero', 'Quizlet', 'Khan Academy', 'Skillshare',
    
    # Gaming
    'Roblox', 'Epic Games', 'Riot Games', 'Supercell', 'King',
    'Zynga', 'Glu Mobile', 'Scopely', 'Machine Zone',
    
    # Social/Content
    'Discord', 'Twitch', 'Substack', 'Patreon', 'OnlyFans',
    'Medium', 'Ghost', 'Beehiiv', 'ConvertKit',
    
    # Climate/Sustainability
    'Rivian', 'Lucid Motors', 'Proterra', 'ChargePoint', 'Sunrun',
    'Sunnova', 'Enphase', 'SolarEdge', 'Stem', 'Fluence',
]

# ============================================================================
# QUALITY FILTERS & UTILITIES
# ============================================================================

# Companies to exclude (known bad actors, defunct, etc.)
COMPANY_BLACKLIST = {
    'example', 'test', 'demo', 'sample', 'placeholder', 'acme',
    'company', 'corp', 'inc', 'llc', 'ltd', 'gmbh', 'sa', 'ag',
    'null', 'none', 'n/a', 'tbd', 'tba', 'unknown', 'unnamed',
    'private', 'confidential', 'stealth', 'startup',
}

# Minimum quality thresholds
MIN_COMPANY_NAME_LENGTH = 2
MAX_COMPANY_NAME_LENGTH = 100
MIN_WORD_COUNT = 1
MAX_WORD_COUNT = 8

def is_valid_company_name(name: str) -> bool:
    """Comprehensive company name validation"""
    if not name:
        return False
    
    # Length checks
    if len(name) < MIN_COMPANY_NAME_LENGTH or len(name) > MAX_COMPANY_NAME_LENGTH:
        return False
    
    # Word count
    words = name.split()
    if len(words) < MIN_WORD_COUNT or len(words) > MAX_WORD_COUNT:
        return False
    
    # Must contain at least one letter
    if not re.search(r'[a-zA-Z]', name):
        return False
    
    # Check blacklist
    name_lower = name.lower()
    if name_lower in COMPANY_BLACKLIST:
        return False
    
    # Check if only common suffixes
    words_lower = set(w.lower() for w in words)
    banned_words = {'inc', 'llc', 'corp', 'ltd', 'plc', 'gmbh', 'sa', 'ag', 'group', 'holdings', 'the'}
    if words_lower.issubset(banned_words):
        return False
    
    # Reject obvious garbage
    if re.match(r'^[\d\s\-_.]+$', name):  # Only numbers/punctuation
        return False
    
    if re.search(r'(test|example|sample|demo|placeholder)', name_lower):
        return False
    
    # Reject URLs
    if re.search(r'(https?://|www\.)', name_lower):
        return False
    
    return True

def normalize_company_name(name: str) -> str:
    """Normalize company name for consistency"""
    # Remove extra whitespace
    name = ' '.join(name.split())
    
    # Title case
    name = name.title()
    
    # Fix common acronyms
    acronyms = ['AI', 'ML', 'API', 'AWS', 'GCP', 'IBM', 'HP', 'IT', 'VR', 'AR', 'XR', 'IoT', 'SaaS', 'B2B', 'B2C']
    for acronym in acronyms:
        name = re.sub(rf'\b{acronym.lower()}\b', acronym, name, flags=re.IGNORECASE)
    
    return name.strip()

def name_to_token(name: str) -> str:
    """Convert company name to URL-friendly token"""
    token = name.lower()
    # Remove common suffixes
    token = re.sub(r'\s+(inc|llc|ltd|co|corp|corporation|gmbh|sa|ag|plc)\.?$', '', token, flags=re.IGNORECASE)
    # Remove special characters
    token = re.sub(r'[^a-z0-9\s-]', '', token)
    # Convert spaces to hyphens
    token = re.sub(r'[\s-]+', '-', token).strip('-')
    return token

# ============================================================================
# MAIN SEED EXPANDER CLASS
# ============================================================================

@dataclass
class ExpansionStats:
    """Track expansion statistics"""
    total_raw: int = 0
    total_processed: int = 0
    total_unique: int = 0
    total_inserted: int = 0
    sources_processed: int = 0
    sources_failed: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    
    def to_dict(self) -> Dict:
        return {
            'total_raw': self.total_raw,
            'total_processed': self.total_processed,
            'total_unique': self.total_unique,
            'total_inserted': self.total_inserted,
            'sources_processed': self.sources_processed,
            'sources_failed': self.sources_failed,
            'duration_seconds': (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        }

class UltimateSeedExpander:
    """Comprehensive seed expansion system"""
    
    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_db()
        self.client: Optional[aiohttp.ClientSession] = None
        self.stats = ExpansionStats()
        self.seen_names: Set[str] = set()
        self._semaphore = asyncio.Semaphore(5)  # Max concurrent requests
    
    async def _get_client(self) -> aiohttp.ClientSession:
        """Get or create aiohttp client session"""
        if self.client is None or self.client.closed:
            self.client = aiohttp.ClientSession(
                headers={'User-Agent': ua.random},
                timeout=aiohttp.ClientTimeout(total=60),
                connector=aiohttp.TCPConnector(limit=10, limit_per_host=5)
            )
        return self.client
    
    async def close(self):
        """Close client session"""
        if self.client and not self.client.closed:
            await self.client.close()
    
    async def _fetch_text(self, url: str, retries: int = 3) -> Optional[str]:
        """Fetch URL with retries and rate limiting"""
        client = await self._get_client()
        
        for attempt in range(retries):
            try:
                async with self._semaphore:
                    async with client.get(url, allow_redirects=True) as resp:
                        if resp.status == 200:
                            return await resp.text()
                        elif resp.status == 429:  # Rate limited
                            wait = int(resp.headers.get('Retry-After', 60))
                            logger.warning(f"Rate limited, waiting {wait}s")
                            await asyncio.sleep(wait)
                        else:
                            logger.warning(f"HTTP {resp.status} for {url}")
            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching {url} (attempt {attempt + 1}/{retries})")
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
            
            if attempt < retries - 1:
                await asyncio.sleep(random.uniform(2, 5))
        
        return None
    
    async def _fetch_json(self, url: str) -> Optional[Dict]:
        """Fetch JSON data"""
        text = await self._fetch_text(url)
        if text:
            try:
                return json.loads(text)
            except json.JSONDecodeError as e:
                logger.error(f"JSON parse error for {url}: {e}")
        return None
    
    def _process_names(self, raw_names: List[str], config: SourceConfig) -> List[Tuple[str, str, str, int]]:
        """Process and validate company names"""
        processed = []
        
        for name in raw_names:
            # Clean and validate
            clean = normalize_company_name(name)
            
            if not is_valid_company_name(clean):
                continue
            
            # Check for duplicates (case-insensitive)
            name_key = clean.lower()
            if name_key in self.seen_names:
                continue
            
            self.seen_names.add(name_key)
            
            # Generate token
            token = name_to_token(clean)
            
            # Add to results
            processed.append((clean, token, config.name, config.tier))
        
        return processed
    
    def _batch_insert(self, seeds: List[Tuple[str, str, str, int]], batch_size: int = 1000):
        """Insert seeds in batches"""
        for i in range(0, len(seeds), batch_size):
            batch = seeds[i:i + batch_size]
            inserted = self.db.insert_seeds(batch)
            self.stats.total_inserted += inserted
            logger.info(f"Inserted batch: {inserted} seeds")
    
    # ========================================================================
    # TIER 1 SOURCES
    # ========================================================================
    
    async def _expand_manual_verified(self, config: SourceConfig) -> List[str]:
        """Manual verified companies"""
        logger.info(f"✅ Loading {len(MANUAL_VERIFIED_COMPANIES)} manually verified companies")
        return MANUAL_VERIFIED_COMPANIES
    
    async def _expand_yc_directory(self, config: SourceConfig) -> List[str]:
        """Y Combinator companies directory"""
        logger.info(f"🚀 Expanding YC companies from {config.url}")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        # YC page structure: look for company links
        for a in soup.find_all('a', href=re.compile(r'/companies/')):
            text = a.get_text(strip=True)
            if text and len(text) > 2:
                companies.add(text)
        
        # Also look for company names in specific divs
        for div in soup.find_all('div', class_=re.compile(r'company')):
            text = div.get_text(strip=True)
            if text and len(text) > 2:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(2, 5))
        return list(companies)
    
    async def _expand_yc_top_companies(self, config: SourceConfig) -> List[str]:
        """YC top companies"""
        logger.info(f"🦄 Expanding YC top companies from {config.url}")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for h3 in soup.find_all('h3'):
            text = h3.get_text(strip=True)
            if text:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(2, 5))
        return list(companies)
    
    async def _expand_techcrunch_startups(self, config: SourceConfig) -> List[str]:
        """TechCrunch startup database"""
        logger.info("📰 Expanding TechCrunch startups")
        # TechCrunch requires more sophisticated scraping
        # This is a placeholder - you may need their API or specific scraping
        return []
    
    async def _expand_builtin_best_places(self, config: SourceConfig) -> List[str]:
        """Built In best places to work"""
        logger.info("🏆 Expanding Built In awards")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for a in soup.find_all('a', href=re.compile(r'/company/')):
            text = a.get_text(strip=True)
            if text:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_ats_curated_faang_plus(self, config: SourceConfig) -> List[str]:
        """FAANG+ and major tech companies"""
        logger.info("🌟 Loading FAANG+ companies")
        # These are already in MANUAL_VERIFIED_COMPANIES
        return []
    
    async def _expand_deloitte_fast500(self, config: SourceConfig) -> List[str]:
        """Deloitte Fast 500"""
        logger.info(f"⚡ Expanding Deloitte Fast 500 from {config.url}")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        # Look for table data
        for td in soup.find_all('td'):
            text = td.get_text(strip=True)
            # Skip rank numbers
            if text and not text.isdigit():
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    # ========================================================================
    # TIER 2 SOURCES
    # ========================================================================
    
    async def _expand_sec_tickers(self, config: SourceConfig) -> List[str]:
        """SEC company tickers"""
        logger.info("📋 Expanding SEC tickers")
        data = await self._fetch_json(config.url)
        if not data:
            return []
        
        companies = []
        for item in data.values():
            if isinstance(item, dict) and 'title' in item:
                companies.append(item['title'])
        
        await asyncio.sleep(random.uniform(2, 4))
        return companies
    
    async def _expand_wikipedia_sp500(self, config: SourceConfig) -> List[str]:
        """S&P 500 from Wikipedia"""
        logger.info("📊 Expanding S&P 500")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = []
        
        # Find the constituents table
        table = soup.find('table', {'id': 'constituents'})
        if table:
            for row in table.find_all('tr')[1:]:  # Skip header
                cells = row.find_all('td')
                if cells and len(cells) > 1:
                    # Company name is usually in second column
                    name = cells[1].get_text(strip=True)
                    if name:
                        companies.append(name)
        
        await asyncio.sleep(random.uniform(2, 4))
        return companies
    
    async def _expand_fortune_500(self, config: SourceConfig) -> List[str]:
        """Fortune 500 companies"""
        logger.info("💼 Expanding Fortune 500")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        # Fortune uses various structures
        for h3 in soup.find_all('h3'):
            text = h3.get_text(strip=True)
            if text:
                companies.add(text)
        
        for a in soup.find_all('a', href=re.compile(r'/company/')):
            text = a.get_text(strip=True)
            if text:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_inc_5000(self, config: SourceConfig) -> List[str]:
        """Inc 5000 companies"""
        logger.info("📈 Expanding Inc 5000")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for div in soup.find_all('div', class_=re.compile(r'company')):
            text = div.get_text(strip=True)
            if text:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_forbes_global_2000(self, config: SourceConfig) -> List[str]:
        """Forbes Global 2000"""
        logger.info("🌍 Expanding Forbes Global 2000")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for td in soup.find_all('td'):
            text = td.get_text(strip=True)
            if text and not text.isdigit():
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_nasdaq_100(self, config: SourceConfig) -> List[str]:
        """NASDAQ-100 companies"""
        logger.info("📱 Expanding NASDAQ-100")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = []
        
        # Find the components table
        for table in soup.find_all('table'):
            if 'component' in str(table).lower():
                for row in table.find_all('tr')[1:]:
                    cells = row.find_all('td')
                    if cells and len(cells) > 1:
                        name = cells[1].get_text(strip=True)
                        if name:
                            companies.append(name)
        
        await asyncio.sleep(random.uniform(2, 4))
        return companies
    
    # ========================================================================
    # TIER 3 SOURCES
    # ========================================================================
    
    async def _expand_crunchbase_samples(self, config: SourceConfig) -> List[str]:
        """Crunchbase dataset samples"""
        logger.info("💎 Expanding Crunchbase samples")
        data = await self._fetch_json(config.url)
        if not data:
            return []
        
        companies = []
        for item in data:
            if isinstance(item, dict) and 'company_name' in item:
                companies.append(item['company_name'])
            elif isinstance(item, dict) and 'name' in item:
                companies.append(item['name'])
        
        await asyncio.sleep(random.uniform(2, 4))
        return companies
    
    async def _expand_b2b_dataset_samples(self, config: SourceConfig) -> List[str]:
        """B2B business dataset"""
        logger.info("🏢 Expanding B2B dataset")
        data = await self._fetch_json(config.url)
        if not data:
            return []
        
        companies = []
        for item in data:
            if isinstance(item, dict) and 'company_name' in item:
                companies.append(item['company_name'])
        
        await asyncio.sleep(random.uniform(2, 4))
        return companies
    
    async def _expand_more_than_faangm(self, config: SourceConfig) -> List[str]:
        """moreThanFAANGM list"""
        logger.info("🚀 Expanding moreThanFAANGM")
        text = await self._fetch_text(config.url)
        if not text:
            return []
        
        companies = set()
        
        # Parse markdown - look for company names
        # Usually in format: - [Company Name](link) or - Company Name
        for line in text.split('\n'):
            # Match markdown links
            match = re.search(r'\[([^\]]+)\]', line)
            if match:
                companies.add(match.group(1))
            # Match bullet points
            elif line.strip().startswith('-'):
                name = line.strip('- ').strip()
                if name and not name.startswith('['):
                    companies.add(name)
        
        await asyncio.sleep(random.uniform(2, 4))
        return list(companies)
    
    async def _expand_github_ventures(self, config: SourceConfig) -> List[str]:
        """GitHub ventures companies"""
        logger.info("🐙 Expanding GitHub ventures")
        # This may require GitHub API access
        return []
    
    # ========================================================================
    # TIER 4 SOURCES (MASS EXPANSION)
    # ========================================================================
    
    async def _expand_github_massive_list(self, config: SourceConfig) -> List[str]:
        """GitHub massive company list (10k+)"""
        logger.info("📚 Expanding massive GitHub company list (this may take a while...)")
        text = await self._fetch_text(config.url)
        if not text:
            return []
        
        companies = []
        for line in text.split('\n'):
            line = line.strip()
            if line and len(line) > 2:
                companies.append(line)
        
        logger.info(f"📚 Loaded {len(companies)} companies from massive list")
        await asyncio.sleep(random.uniform(5, 10))
        return companies
    
    async def _expand_heavy_pint_business(self, config: SourceConfig) -> List[str]:
        """Heavy Pint business names"""
        logger.info("📄 Expanding Heavy Pint business names")
        text = await self._fetch_text(config.url)
        if not text:
            return []
        
        companies = []
        for line in text.split('\n'):
            line = line.strip()
            if line:
                companies.append(line)
        
        await asyncio.sleep(random.uniform(3, 6))
        return companies
    
    # ========================================================================
    # MAIN EXPANSION LOGIC
    # ========================================================================
    
    async def _run_source(self, source_name: str, config: SourceConfig) -> int:
        """Run a single source expansion"""
        logger.info("=" * 80)
        logger.info(f"Processing: {config.name} (Tier {config.tier}, Priority {config.priority})")
        logger.info(f"Description: {config.description}")
        
        # Find expansion method
        method_name = f'_expand_{source_name}'
        method = getattr(self, method_name, None)
        
        if not method:
            logger.warning(f"⚠️ No expansion method for {source_name}")
            return 0
        
        try:
            # Fetch raw names
            raw_names = await method(config)
            self.stats.total_raw += len(raw_names)
            
            if not raw_names:
                logger.warning(f"⚠️ No data retrieved from {config.name}")
                return 0
            
            logger.info(f"📥 Retrieved {len(raw_names)} raw company names")
            
            # Process and validate
            processed = self._process_names(raw_names, config)
            self.stats.total_processed += len(processed)
            
            if not processed:
                logger.warning(f"⚠️ No valid companies after processing {config.name}")
                return 0
            
            logger.info(f"✅ Validated {len(processed)} unique companies")
            
            # Insert to database
            self._batch_insert(processed)
            
            logger.info(f"💾 Inserted {len(processed)} companies from {config.name}")
            self.stats.sources_processed += 1
            
            # Rate limiting
            await asyncio.sleep(config.rate_limit)
            
            return len(processed)
            
        except Exception as e:
            logger.error(f"❌ Error processing {config.name}: {e}", exc_info=True)
            self.stats.sources_failed += 1
            return 0
    
    async def _run_expansion(self, *tiers: int, max_sources: Optional[int] = None) -> Dict[str, int]:
        """Run expansion for specified tiers"""
        logger.info("=" * 80)
        logger.info("🚀 STARTING SEED EXPANSION")
        logger.info("=" * 80)
        logger.info(f"Tiers: {tiers}")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info("=" * 80)
        
        # Filter and sort sources
        active_sources = [
            (config.priority, name, config)
            for name, config in SOURCES.items()
            if config.enabled and config.tier in tiers
        ]
        active_sources.sort(key=lambda x: x[0], reverse=True)
        
        if max_sources:
            active_sources = active_sources[:max_sources]
        
        logger.info(f"📋 Processing {len(active_sources)} sources")
        
        # Process sources
        results = {}
        for priority, name, config in active_sources:
            count = await self._run_source(name, config)
            results[name] = count
        
        # Finalize stats
        self.stats.end_time = datetime.now()
        self.stats.total_unique = len(self.seen_names)
        
        logger.info("=" * 80)
        logger.info("✅ EXPANSION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Raw names retrieved: {self.stats.total_raw}")
        logger.info(f"Valid names processed: {self.stats.total_processed}")
        logger.info(f"Unique names: {self.stats.total_unique}")
        logger.info(f"Inserted to database: {self.stats.total_inserted}")
        logger.info(f"Sources processed: {self.stats.sources_processed}")
        logger.info(f"Sources failed: {self.stats.sources_failed}")
        logger.info(f"Duration: {(self.stats.end_time - self.stats.start_time).total_seconds():.1f}s")
        logger.info("=" * 80)
        
        return results
    
    # ========================================================================
    # PUBLIC API
    # ========================================================================
    
    async def expand_tier1(self, max_sources: Optional[int] = None):
        """Expand Tier 1: High-quality tech companies"""
        return await self._run_expansion(1, max_sources=max_sources)
    
    async def expand_tier2(self, max_sources: Optional[int] = None):
        """Expand Tier 2: Public companies"""
        return await self._run_expansion(2, max_sources=max_sources)
    
    async def expand_tier3(self, max_sources: Optional[int] = None):
        """Expand Tier 3: Startup ecosystems"""
        return await self._run_expansion(3, max_sources=max_sources)
    
    async def expand_tier4(self, max_sources: Optional[int] = None):
        """Expand Tier 4: Mass expansion"""
        return await self._run_expansion(4, max_sources=max_sources)
    
    async def expand_tiers_1_2(self):
        """Expand Tiers 1-2: Quality companies only"""
        return await self._run_expansion(1, 2)
    
    async def expand_all(self):
        """Expand all tiers (comprehensive)"""
        return await self._run_expansion(1, 2, 3, 4)
    
    async def expand_smart(self):
        """Smart expansion: Tier 1-3 (excludes mass tier 4)"""
        return await self._run_expansion(1, 2, 3)

# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

async def run_tier1_expansion():
    """Run Tier 1 expansion"""
    expander = UltimateSeedExpander()
    try:
        return await expander.expand_tier1()
    finally:
        await expander.close()

async def run_tier2_expansion():
    """Run Tier 2 expansion"""
    expander = UltimateSeedExpander()
    try:
        return await expander.expand_tier2()
    finally:
        await expander.close()

async def run_full_expansion():
    """Run full expansion (all tiers)"""
    expander = UltimateSeedExpander()
    try:
        return await expander.expand_all()
    finally:
        await expander.close()

async def run_smart_expansion():
    """Run smart expansion (Tiers 1-3, skip mass tier 4)"""
    expander = UltimateSeedExpander()
    try:
        return await expander.expand_smart()
    finally:
        await expander.close()

# ============================================================================
# CLI INTERFACE
# ============================================================================

if __name__ == "__main__":
    import sys
    
    mode = sys.argv[1] if len(sys.argv) > 1 else "smart"
    
    expander = UltimateSeedExpander()
    
    try:
        if mode == "tier1":
            results = asyncio.run(expander.expand_tier1())
        elif mode == "tier2":
            results = asyncio.run(expander.expand_tier2())
        elif mode == "tier3":
            results = asyncio.run(expander.expand_tier3())
        elif mode == "tier4":
            results = asyncio.run(expander.expand_tier4())
        elif mode == "all":
            results = asyncio.run(expander.expand_all())
        elif mode == "smart":
            results = asyncio.run(expander.expand_smart())
        else:
            print(f"Unknown mode: {mode}")
            print("Usage: python seed_expander.py [tier1|tier2|tier3|tier4|all|smart]")
            sys.exit(1)
        
        print("\n" + "=" * 80)
        print("FINAL STATISTICS")
        print("=" * 80)
        print(json.dumps(expander.stats.to_dict(), indent=2))
        print("=" * 80)
        
    finally:
        asyncio.run(expander.close())
