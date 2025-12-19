"""
Ultimate Seed Expander v3.0 - Enhanced with 25+ Sources
Fixes all errors + adds major new data sources
"""

import asyncio
import aiohttp
import json
import re
import logging
import random
from typing import List, Tuple, Dict, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import quote
import time

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from database import get_db, Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ua = UserAgent()

@dataclass
class SourceConfig:
    """Configuration for each data source"""
    name: str
    tier: int
    priority: int
    url: Optional[str] = None
    enabled: bool = True
    rate_limit: float = 3.0
    max_retries: int = 3
    timeout: int = 60
    description: str = ""
    requires_special_handling: bool = False

# ============================================================================
# COMPREHENSIVE SOURCE DEFINITIONS (25+ SOURCES)
# ============================================================================

SOURCES = {
    # ===== TIER 1: PREMIUM TECH COMPANIES (100-90) =====
    'manual_verified': SourceConfig(
        name='Manually Verified Companies',
        tier=1,
        priority=100,
        description='Hand-curated list of known Greenhouse/Lever users'
    ),
    
    'greenhouse_customers': SourceConfig(
        name='Greenhouse Public Customers',
        tier=1,
        priority=99,
        url='https://www.greenhouse.com/customers',
        description='Companies using Greenhouse ATS'
    ),
    
    'lever_customers': SourceConfig(
        name='Lever Public Customers',
        tier=1,
        priority=98,
        url='https://www.lever.co/customers',
        description='Companies using Lever ATS'
    ),
    
    'yc_directory': SourceConfig(
        name='Y Combinator Companies',
        tier=1,
        priority=97,
        url='https://www.ycombinator.com/companies',
        description='All YC-backed startups',
        rate_limit=5.0
    ),
    
    'yc_top_companies': SourceConfig(
        name='YC Top Companies',
        tier=1,
        priority=96,
        url='https://www.ycombinator.com/topcompanies',
        description='YC unicorns and high-growth companies'
    ),
    
    'yc_work_at_startup': SourceConfig(
        name='YC Work at a Startup',
        tier=1,
        priority=95,
        url='https://www.workatastartup.com/companies',
        description='YC companies actively hiring'
    ),
    
    'techstars_portfolio': SourceConfig(
        name='Techstars Portfolio',
        tier=1,
        priority=94,
        url='https://www.techstars.com/portfolio',
        description='Techstars-backed companies'
    ),
    
    'a16z_portfolio': SourceConfig(
        name='Andreessen Horowitz Portfolio',
        tier=1,
        priority=93,
        url='https://a16z.com/portfolio/',
        description='a16z portfolio companies'
    ),
    
    'sequoia_portfolio': SourceConfig(
        name='Sequoia Capital Portfolio',
        tier=1,
        priority=92,
        url='https://www.sequoiacap.com/companies/',
        description='Sequoia portfolio companies'
    ),
    
    'product_hunt_trending': SourceConfig(
        name='Product Hunt Companies',
        tier=1,
        priority=91,
        url='https://www.producthunt.com/search?q=hiring',
        description='Product Hunt companies hiring'
    ),
    
    'deloitte_fast500': SourceConfig(
        name='Deloitte Technology Fast 500',
        tier=1,
        priority=90,
        url='https://www2.deloitte.com/us/en/pages/technology-media-and-telecommunications/articles/fast500-winners.html',
        description='Fastest-growing tech companies'
    ),
    
    # ===== TIER 2: PUBLIC COMPANIES & ESTABLISHED FIRMS (89-75) =====
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
    
    'nasdaq_100': SourceConfig(
        name='NASDAQ-100',
        tier=2,
        priority=87,
        url='https://en.wikipedia.org/wiki/Nasdaq-100',
        description='Top 100 non-financial NASDAQ companies'
    ),
    
    'fortune_500_wikipedia': SourceConfig(
        name='Fortune 500 (Wikipedia)',
        tier=2,
        priority=86,
        url='https://en.wikipedia.org/wiki/Fortune_500',
        description='Fortune 500 from Wikipedia'
    ),
    
    'inc_5000_wikipedia': SourceConfig(
        name='Inc 5000 (Wikipedia)',
        tier=2,
        priority=85,
        url='https://en.wikipedia.org/wiki/Inc._500',
        description='Inc 5000 from Wikipedia'
    ),
    
    'forbes_cloud_100': SourceConfig(
        name='Forbes Cloud 100',
        tier=2,
        priority=84,
        url='https://www.forbes.com/cloud100/',
        description='Top private cloud companies'
    ),
    
    'forbes_ai_50': SourceConfig(
        name='Forbes AI 50',
        tier=2,
        priority=83,
        url='https://www.forbes.com/lists/ai50/',
        description='Most promising AI companies'
    ),
    
    'crunchbase_unicorns': SourceConfig(
        name='Crunchbase Unicorn List',
        tier=2,
        priority=82,
        url='https://www.crunchbase.com/lists/unicorn-companies/97b0bf33-75c5-4eb0-9d3b-f04e0cfc8703/organization.companies',
        description='Unicorn companies ($1B+ valuation)'
    ),
    
    # ===== TIER 3: STARTUP ECOSYSTEMS & REMOTE COMPANIES (74-60) =====
    'angellist_trending': SourceConfig(
        name='AngelList/Wellfound Trending',
        tier=3,
        priority=74,
        url='https://wellfound.com/jobs',
        description='Trending startups on Wellfound'
    ),
    
    'we_work_remotely': SourceConfig(
        name='We Work Remotely Companies',
        tier=3,
        priority=73,
        url='https://weworkremotely.com/companies',
        description='Remote-first companies'
    ),
    
    'remote_co': SourceConfig(
        name='Remote.co Companies',
        tier=3,
        priority=72,
        url='https://remote.co/companies/',
        description='Companies hiring remotely'
    ),
    
    'flexjobs_companies': SourceConfig(
        name='FlexJobs Top Companies',
        tier=3,
        priority=71,
        url='https://www.flexjobs.com/blog/post/companies-hiring-remote-workers/',
        description='Top remote hiring companies'
    ),
    
    'builtin_companies': SourceConfig(
        name='Built In Companies',
        tier=3,
        priority=70,
        url='https://builtin.com/companies',
        description='Tech companies by location'
    ),
    
    'otta_companies': SourceConfig(
        name='Otta Companies',
        tier=3,
        priority=69,
        url='https://otta.com/companies',
        description='Startup jobs platform companies'
    ),
    
    'more_than_faangm': SourceConfig(
        name='moreThanFAANGM List',
        tier=3,
        priority=68,
        url='https://raw.githubusercontent.com/Kaustubh-Natuskar/moreThanFAANGM/master/README.md',
        description='400+ product companies beyond FAANG'
    ),
    
    'github_trending_orgs': SourceConfig(
        name='GitHub Trending Organizations',
        tier=3,
        priority=67,
        url='https://github.com/trending',
        description='Trending GitHub organizations'
    ),
    
    'stackshare_trending': SourceConfig(
        name='StackShare Companies',
        tier=3,
        priority=66,
        url='https://stackshare.io/trending/tools',
        description='Companies on StackShare'
    ),
    
    # ===== TIER 4: MASS EXPANSION (65-50) =====
    'github_massive_list': SourceConfig(
        name='GitHub Massive Company List',
        tier=4,
        priority=65,
        url='https://gist.githubusercontent.com/bojanbabic/f007ffd83ea20b1ac48812131325851e/raw/',
        description='Comprehensive company name list (10k+)',
        rate_limit=5.0
    ),
    
    'heavy_pint_business': SourceConfig(
        name='Heavy Pint Business Names',
        tier=4,
        priority=64,
        url='https://raw.githubusercontent.com/9b/heavy_pint/master/lists/business-names.txt',
        description='Business names dataset',
        requires_special_handling=True
    ),
    
    'wikipedia_tech_companies': SourceConfig(
        name='Wikipedia Tech Companies',
        tier=4,
        priority=63,
        url='https://en.wikipedia.org/wiki/List_of_technology_companies',
        description='Comprehensive tech company list'
    ),
    
    'wikipedia_unicorns': SourceConfig(
        name='Wikipedia Unicorn List',
        tier=4,
        priority=62,
        url='https://en.wikipedia.org/wiki/List_of_unicorn_startup_companies',
        description='List of unicorn startups'
    ),
}

# ============================================================================
# MANUALLY VERIFIED COMPANIES (Expanded)
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
    'Alphabet', 'Facebook', 'Instagram', 'WhatsApp', 'YouTube',
    
    # Major unicorns & high-growth (expanded)
    'Anthropic', 'OpenAI', 'Scale AI', 'Databricks', 'Canva', 'Figma',
    'Brex', 'Rippling', 'Plaid', 'Chime', 'Robinhood', 'Coinbase',
    'Cruise', 'Aurora', 'Zoox', 'Waymo', 'Nuro', 'TuSimple',
    'Celonis', 'UiPath', 'Automation Anywhere', 'Blue Prism',
    
    # Major ATS-using companies
    'Airbnb', 'Reddit', 'Slack', 'Atlassian', 'Salesforce', 'Workday',
    'ServiceNow', 'Snowflake', 'MongoDB', 'Elastic', 'HashiCorp',
    'GitLab', 'GitHub', 'Vercel', 'Netlify', 'Heroku', 'Render',
    'Supabase', 'PlanetScale', 'Neon', 'Railway',
    
    # Enterprise software
    'Asana', 'Monday.com', 'ClickUp', 'Airtable', 'Notion',
    'Zapier', 'Retool', 'Webflow', 'Bubble', 'Glide',
    'Linear', 'Height', 'Coda', 'Superhuman', 'Front',
    
    # Fintech (expanded)
    'Square', 'PayPal', 'Venmo', 'Cash App', 'Adyen', 'Marqeta',
    'Checkout.com', 'Sezzle', 'Klarna', 'Afterpay', 'Zip',
    'Revolut', 'N26', 'Monzo', 'Starling', 'Wise', 'Remitly',
    
    # Cloud/DevOps
    'Datadog', 'New Relic', 'PagerDuty', 'LaunchDarkly', 'Temporal',
    'Chronosphere', 'Observe', 'Honeycomb', 'Lightstep',
    'CircleCI', 'Travis CI', 'Jenkins', 'Harness', 'Spinnaker',
    
    # Security
    'CrowdStrike', 'Okta', 'Auth0', 'OneLogin', 'Duo Security',
    'Snyk', 'Lacework', 'Wiz', 'Orca Security', 'Aqua Security',
    'Palo Alto Networks', 'Fortinet', 'Check Point', 'Zscaler',
    
    # AI/ML (expanded)
    'Hugging Face', 'Cohere', 'Stability AI', 'Midjourney', 'Runway',
    'Character AI', 'Jasper', 'Copy.ai', 'Descript', 'Synthesia',
    'Replicate', 'Weights & Biases', 'Roboflow', 'Landing AI',
    
    # E-commerce/Marketplaces
    'Shopify', 'BigCommerce', 'WooCommerce', 'Etsy', 'Poshmark',
    'StockX', 'GOAT', 'Faire', 'Ankorstore', 'Modalyst',
    'Mercari', 'OfferUp', 'Depop', 'Grailed', 'Vinted',
    
    # Healthcare tech
    'Oscar Health', 'Devoted Health', 'Ro', 'Hims & Hers', 'Nurx',
    'Omada Health', 'Livongo', 'Teladoc', 'Amwell', 'MDLive',
    'One Medical', 'Carbon Health', 'Forward', 'Thirty Madison',
    
    # Real estate tech
    'Zillow', 'Redfin', 'Opendoor', 'Offerpad', 'Compass',
    'Better.com', 'Divvy Homes', 'Arrived', 'Fundrise',
    'Homelight', 'Knock', 'Flyhomes', 'Orchard',
    
    # EdTech
    'Coursera', 'Udemy', 'Udacity', 'Duolingo', 'Chegg',
    'Course Hero', 'Quizlet', 'Khan Academy', 'Skillshare',
    'Masterclass', 'Pluralsight', 'DataCamp', 'Codecademy',
    
    # Gaming
    'Roblox', 'Epic Games', 'Riot Games', 'Supercell', 'King',
    'Zynga', 'Glu Mobile', 'Scopely', 'Machine Zone',
    'Unity', 'Unreal', 'Roblox Corporation', 'Niantic',
    
    # Social/Content
    'Discord', 'Twitch', 'Substack', 'Patreon', 'OnlyFans',
    'Medium', 'Ghost', 'Beehiiv', 'ConvertKit',
    'Clubhouse', 'Geneva', 'Slack', 'Telegram',
    
    # Climate/Sustainability
    'Rivian', 'Lucid Motors', 'Proterra', 'ChargePoint', 'Sunrun',
    'Sunnova', 'Enphase', 'SolarEdge', 'Stem', 'Fluence',
    'Redwood Materials', 'Northvolt', 'QuantumScape',
    
    # Web3/Crypto
    'Coinbase', 'Kraken', 'Gemini', 'BlockFi', 'Celsius',
    'Alchemy', 'Infura', 'Chainlink', 'OpenSea', 'Magic Eden',
    'Uniswap', 'Aave', 'Compound', 'MakerDAO',
]

# ============================================================================
# QUALITY FILTERS
# ============================================================================

COMPANY_BLACKLIST = {
    'example', 'test', 'demo', 'sample', 'placeholder', 'acme',
    'company', 'corp', 'inc', 'llc', 'ltd', 'gmbh', 'sa', 'ag',
    'null', 'none', 'n/a', 'tbd', 'tba', 'unknown', 'unnamed',
    'private', 'confidential', 'stealth', 'startup',
    'untitled', 'new company', 'my company', 'your company',
}

MIN_COMPANY_NAME_LENGTH = 2
MAX_COMPANY_NAME_LENGTH = 100
MIN_WORD_COUNT = 1
MAX_WORD_COUNT = 8

def is_valid_company_name(name: str) -> bool:
    """Comprehensive company name validation"""
    if not name:
        return False
    
    if len(name) < MIN_COMPANY_NAME_LENGTH or len(name) > MAX_COMPANY_NAME_LENGTH:
        return False
    
    words = name.split()
    if len(words) < MIN_WORD_COUNT or len(words) > MAX_WORD_COUNT:
        return False
    
    if not re.search(r'[a-zA-Z]', name):
        return False
    
    name_lower = name.lower()
    if name_lower in COMPANY_BLACKLIST:
        return False
    
    words_lower = set(w.lower() for w in words)
    banned_words = {'inc', 'llc', 'corp', 'ltd', 'plc', 'gmbh', 'sa', 'ag', 'group', 'holdings', 'the'}
    if words_lower.issubset(banned_words):
        return False
    
    if re.match(r'^[\d\s\-_.]+$', name):
        return False
    
    if re.search(r'(test|example|sample|demo|placeholder)', name_lower):
        return False
    
    if re.search(r'(https?://|www\.)', name_lower):
        return False
    
    return True

def normalize_company_name(name: str) -> str:
    """Normalize company name"""
    name = ' '.join(name.split())
    name = name.title()
    
    acronyms = ['AI', 'ML', 'API', 'AWS', 'GCP', 'IBM', 'HP', 'IT', 'VR', 'AR', 'XR', 'IoT', 'SaaS', 'B2B', 'B2C', 'CEO', 'CTO', 'CFO']
    for acronym in acronyms:
        name = re.sub(rf'\b{acronym.lower()}\b', acronym, name, flags=re.IGNORECASE)
    
    return name.strip()

def name_to_token(name: str) -> str:
    """Convert company name to URL-friendly token"""
    token = name.lower()
    token = re.sub(r'\s+(inc|llc|ltd|co|corp|corporation|gmbh|sa|ag|plc)\.?$', '', token, flags=re.IGNORECASE)
    token = re.sub(r'[^a-z0-9\s-]', '', token)
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
    """Comprehensive seed expansion system v3.0"""
    
    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_db()
        self.client: Optional[aiohttp.ClientSession] = None
        self.stats = ExpansionStats()
        self.seen_names: Set[str] = set()
        self._semaphore = asyncio.Semaphore(5)
    
    async def _get_client(self) -> aiohttp.ClientSession:
        """Get or create aiohttp client session with better headers"""
        if self.client is None or self.client.closed:
            # Rotate user agents for better success rate
            headers = {
                'User-Agent': ua.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            self.client = aiohttp.ClientSession(
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=60),
                connector=aiohttp.TCPConnector(limit=10, limit_per_host=5)
            )
        return self.client
    
    async def close(self):
        """Close client session"""
        if self.client and not self.client.closed:
            await self.client.close()
    
    async def _fetch_text(self, url: str, retries: int = 3, encoding: str = 'utf-8') -> Optional[str]:
        """Fetch URL with retries, rate limiting, and encoding handling"""
        client = await self._get_client()
        
        for attempt in range(retries):
            try:
                async with self._semaphore:
                    async with client.get(url, allow_redirects=True) as resp:
                        if resp.status == 200:
                            # Handle different encodings
                            try:
                                return await resp.text(encoding=encoding)
                            except UnicodeDecodeError:
                                # Try latin-1 as fallback
                                return await resp.text(encoding='latin-1')
                        elif resp.status == 429:
                            wait = int(resp.headers.get('Retry-After', 60))
                            logger.warning(f"Rate limited, waiting {wait}s")
                            await asyncio.sleep(wait)
                        elif resp.status == 403:
                            logger.warning(f"HTTP 403 (Forbidden) for {url} - May need authentication")
                            return None
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
            clean = normalize_company_name(name)
            
            if not is_valid_company_name(clean):
                continue
            
            name_key = clean.lower()
            if name_key in self.seen_names:
                continue
            
            self.seen_names.add(name_key)
            token = name_to_token(clean)
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
    # TIER 1 SOURCES (NEW & FIXED)
    # ========================================================================
    
    async def _expand_manual_verified(self, config: SourceConfig) -> List[str]:
        """Manual verified companies"""
        logger.info(f"✅ Loading {len(MANUAL_VERIFIED_COMPANIES)} manually verified companies")
        return MANUAL_VERIFIED_COMPANIES
    
    async def _expand_greenhouse_customers(self, config: SourceConfig) -> List[str]:
        """Greenhouse public customer list"""
        logger.info(f"🌿 Expanding Greenhouse customers from {config.url}")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        # Look for customer logos and names
        for img in soup.find_all('img', alt=True):
            if img['alt'] and 'logo' not in img['alt'].lower():
                companies.add(img['alt'])
        
        # Look for company mentions in text
        for div in soup.find_all('div', class_=re.compile(r'customer|client|company')):
            text = div.get_text(strip=True)
            if text and len(text) < 50:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_lever_customers(self, config: SourceConfig) -> List[str]:
        """Lever public customer list"""
        logger.info(f"⚙️ Expanding Lever customers from {config.url}")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for img in soup.find_all('img', alt=True):
            if img['alt']:
                companies.add(img['alt'])
        
        for h3 in soup.find_all('h3'):
            text = h3.get_text(strip=True)
            if text and len(text) < 50:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_yc_directory(self, config: SourceConfig) -> List[str]:
        """Y Combinator companies directory - IMPROVED"""
        logger.info(f"🚀 Expanding YC companies from {config.url}")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        # Multiple strategies for YC page
        for a in soup.find_all('a', href=re.compile(r'/companies/')):
            text = a.get_text(strip=True)
            if text and len(text) > 2 and len(text) < 50:
                companies.add(text)
        
        for div in soup.find_all('div', class_=re.compile(r'company')):
            text = div.get_text(strip=True)
            if text and len(text) > 2 and len(text) < 50:
                companies.add(text)
        
        for h3 in soup.find_all(['h3', 'h4']):
            text = h3.get_text(strip=True)
            if text and len(text) > 2 and len(text) < 50:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(4, 8))
        return list(companies)
    
    async def _expand_yc_top_companies(self, config: SourceConfig) -> List[str]:
        """YC top companies"""
        logger.info(f"🦄 Expanding YC top companies from {config.url}")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for h3 in soup.find_all(['h2', 'h3', 'h4']):
            text = h3.get_text(strip=True)
            if text:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_yc_work_at_startup(self, config: SourceConfig) -> List[str]:
        """YC Work at a Startup companies"""
        logger.info(f"💼 Expanding YC Work at a Startup")
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
    
    async def _expand_techstars_portfolio(self, config: SourceConfig) -> List[str]:
        """Techstars portfolio companies"""
        logger.info(f"⭐ Expanding Techstars portfolio")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for div in soup.find_all('div', class_=re.compile(r'company|portfolio')):
            for h in div.find_all(['h2', 'h3', 'h4']):
                text = h.get_text(strip=True)
                if text:
                    companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_a16z_portfolio(self, config: SourceConfig) -> List[str]:
        """a16z portfolio companies"""
        logger.info(f"💰 Expanding a16z portfolio")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for a in soup.find_all('a'):
            text = a.get_text(strip=True)
            if text and len(text) > 2 and len(text) < 50:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_sequoia_portfolio(self, config: SourceConfig) -> List[str]:
        """Sequoia portfolio companies"""
        logger.info(f"🌲 Expanding Sequoia portfolio")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for div in soup.find_all('div', class_=re.compile(r'portfolio|company')):
            text = div.get_text(strip=True)
            if text and len(text) < 50:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_product_hunt_trending(self, config: SourceConfig) -> List[str]:
        """Product Hunt companies"""
        logger.info(f"🔍 Expanding Product Hunt companies")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for a in soup.find_all('a', href=re.compile(r'/posts/')):
            text = a.get_text(strip=True)
            if text:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_deloitte_fast500(self, config: SourceConfig) -> List[str]:
        """Deloitte Fast 500 - FIXED URL"""
        logger.info(f"⚡ Expanding Deloitte Fast 500 from {config.url}")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for td in soup.find_all('td'):
            text = td.get_text(strip=True)
            if text and not text.isdigit() and len(text) > 2:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    # ========================================================================
    # TIER 2 SOURCES (FIXED & NEW)
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
        
        table = soup.find('table', {'id': 'constituents'})
        if table:
            for row in table.find_all('tr')[1:]:
                cells = row.find_all('td')
                if cells and len(cells) > 1:
                    name = cells[1].get_text(strip=True)
                    if name:
                        companies.append(name)
        
        await asyncio.sleep(random.uniform(2, 4))
        return companies
    
    async def _expand_nasdaq_100(self, config: SourceConfig) -> List[str]:
        """NASDAQ-100 companies"""
        logger.info("📱 Expanding NASDAQ-100")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = []
        
        for table in soup.find_all('table', class_='wikitable'):
            for row in table.find_all('tr')[1:]:
                cells = row.find_all('td')
                if cells and len(cells) > 1:
                    name = cells[1].get_text(strip=True)
                    if name:
                        companies.append(name)
        
        await asyncio.sleep(random.uniform(2, 4))
        return companies
    
    async def _expand_fortune_500_wikipedia(self, config: SourceConfig) -> List[str]:
        """Fortune 500 from Wikipedia - ALTERNATIVE SOURCE"""
        logger.info("💼 Expanding Fortune 500 (Wikipedia)")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = []
        
        for table in soup.find_all('table', class_='wikitable'):
            for row in table.find_all('tr')[1:]:
                cells = row.find_all('td')
                if cells and len(cells) > 1:
                    name = cells[1].get_text(strip=True)
                    if name:
                        companies.append(name)
        
        await asyncio.sleep(random.uniform(2, 4))
        return companies
    
    async def _expand_inc_5000_wikipedia(self, config: SourceConfig) -> List[str]:
        """Inc 5000 from Wikipedia - ALTERNATIVE SOURCE"""
        logger.info("📈 Expanding Inc 5000 (Wikipedia)")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for a in soup.find_all('a'):
            text = a.get_text(strip=True)
            if text and len(text) > 2 and len(text) < 50:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(2, 4))
        return list(companies)
    
    async def _expand_forbes_cloud_100(self, config: SourceConfig) -> List[str]:
        """Forbes Cloud 100"""
        logger.info("☁️ Expanding Forbes Cloud 100")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for div in soup.find_all('div', class_=re.compile(r'company|org')):
            text = div.get_text(strip=True)
            if text and len(text) < 50:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_forbes_ai_50(self, config: SourceConfig) -> List[str]:
        """Forbes AI 50"""
        logger.info("🤖 Expanding Forbes AI 50")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for h3 in soup.find_all(['h2', 'h3', 'h4']):
            text = h3.get_text(strip=True)
            if text:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_crunchbase_unicorns(self, config: SourceConfig) -> List[str]:
        """Crunchbase unicorn list"""
        logger.info("🦄 Expanding Crunchbase unicorns")
        # This would require Crunchbase API access
        # Return empty for now
        return []
    
    # ========================================================================
    # TIER 3 SOURCES (NEW)
    # ========================================================================
    
    async def _expand_angellist_trending(self, config: SourceConfig) -> List[str]:
        """AngelList/Wellfound trending"""
        logger.info("👼 Expanding Wellfound companies")
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
    
    async def _expand_we_work_remotely(self, config: SourceConfig) -> List[str]:
        """We Work Remotely companies"""
        logger.info("🌎 Expanding We Work Remotely")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for a in soup.find_all('a'):
            text = a.get_text(strip=True)
            if text and len(text) < 50:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_remote_co(self, config: SourceConfig) -> List[str]:
        """Remote.co companies"""
        logger.info("💻 Expanding Remote.co")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for div in soup.find_all('div', class_=re.compile(r'company')):
            text = div.get_text(strip=True)
            if text and len(text) < 50:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_flexjobs_companies(self, config: SourceConfig) -> List[str]:
        """FlexJobs companies"""
        logger.info("🏠 Expanding FlexJobs")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for li in soup.find_all('li'):
            text = li.get_text(strip=True)
            # Company names are often in list items
            if text and len(text) < 50 and not text.startswith(('The', 'A ', 'An ')):
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_builtin_companies(self, config: SourceConfig) -> List[str]:
        """Built In companies"""
        logger.info("🏗️ Expanding Built In")
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
    
    async def _expand_otta_companies(self, config: SourceConfig) -> List[str]:
        """Otta companies"""
        logger.info("🎯 Expanding Otta")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for h3 in soup.find_all(['h2', 'h3']):
            text = h3.get_text(strip=True)
            if text:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_more_than_faangm(self, config: SourceConfig) -> List[str]:
        """moreThanFAANGM list - FIXED PARSING"""
        logger.info("🚀 Expanding moreThanFAANGM")
        text = await self._fetch_text(config.url)
        if not text:
            return []
        
        companies = set()
        
        # Parse markdown - improved regex
        for line in text.split('\n'):
            # Match markdown links: [Company Name](url)
            matches = re.findall(r'\[([^\]]+)\]\([^\)]+\)', line)
            for match in matches:
                if match and len(match) > 2 and len(match) < 50:
                    companies.add(match)
            
            # Match bullet points with company names
            if line.strip().startswith(('-', '*', '+')):
                # Remove bullet and links
                clean_line = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', line)
                clean_line = clean_line.strip('- *+').strip()
                if clean_line and len(clean_line) > 2 and len(clean_line) < 50:
                    companies.add(clean_line)
        
        await asyncio.sleep(random.uniform(2, 4))
        return list(companies)
    
    async def _expand_github_trending_orgs(self, config: SourceConfig) -> List[str]:
        """GitHub trending organizations"""
        logger.info("🐙 Expanding GitHub trending orgs")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for a in soup.find_all('a', href=re.compile(r'^/[^/]+$')):
            text = a.get_text(strip=True)
            if text and len(text) > 2:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    async def _expand_stackshare_trending(self, config: SourceConfig) -> List[str]:
        """StackShare companies"""
        logger.info("📚 Expanding StackShare")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for div in soup.find_all('div', class_=re.compile(r'company')):
            text = div.get_text(strip=True)
            if text and len(text) < 50:
                companies.add(text)
        
        await asyncio.sleep(random.uniform(3, 6))
        return list(companies)
    
    # ========================================================================
    # TIER 4 SOURCES (FIXED)
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
        """Heavy Pint business names - FIXED ENCODING"""
        logger.info("📄 Expanding Heavy Pint business names")
        # Try latin-1 encoding instead of utf-8
        text = await self._fetch_text(config.url, encoding='latin-1')
        if not text:
            return []
        
        companies = []
        for line in text.split('\n'):
            line = line.strip()
            if line:
                companies.append(line)
        
        await asyncio.sleep(random.uniform(3, 6))
        return companies
    
    async def _expand_wikipedia_tech_companies(self, config: SourceConfig) -> List[str]:
        """Wikipedia tech companies list"""
        logger.info("🖥️ Expanding Wikipedia tech companies")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = set()
        
        for table in soup.find_all('table', class_='wikitable'):
            for row in table.find_all('tr')[1:]:
                cells = row.find_all('td')
                if cells:
                    name = cells[0].get_text(strip=True)
                    if name:
                        companies.add(name)
        
        await asyncio.sleep(random.uniform(2, 4))
        return list(companies)
    
    async def _expand_wikipedia_unicorns(self, config: SourceConfig) -> List[str]:
        """Wikipedia unicorn startups"""
        logger.info("🦄 Expanding Wikipedia unicorn list")
        html = await self._fetch_text(config.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        companies = []
        
        for table in soup.find_all('table', class_='wikitable'):
            for row in table.find_all('tr')[1:]:
                cells = row.find_all('td')
                if cells and len(cells) > 1:
                    name = cells[0].get_text(strip=True)
                    if name:
                        companies.append(name)
        
        await asyncio.sleep(random.uniform(2, 4))
        return companies
    
    # ========================================================================
    # MAIN EXPANSION LOGIC
    # ========================================================================
    
    async def _run_source(self, source_name: str, config: SourceConfig) -> int:
        """Run a single source expansion"""
        logger.info("=" * 80)
        logger.info(f"Processing: {config.name} (Tier {config.tier}, Priority {config.priority})")
        logger.info(f"Description: {config.description}")
        
        method_name = f'_expand_{source_name}'
        method = getattr(self, method_name, None)
        
        if not method:
            logger.warning(f"⚠️ No expansion method for {source_name}")
            return 0
        
        try:
            raw_names = await method(config)
            self.stats.total_raw += len(raw_names)
            
            if not raw_names:
                logger.warning(f"⚠️ No data retrieved from {config.name}")
                return 0
            
            logger.info(f"📥 Retrieved {len(raw_names)} raw company names")
            
            processed = self._process_names(raw_names, config)
            self.stats.total_processed += len(processed)
            
            if not processed:
                logger.warning(f"⚠️ No valid companies after processing {config.name}")
                return 0
            
            logger.info(f"✅ Validated {len(processed)} unique companies")
            
            self._batch_insert(processed)
            
            logger.info(f"💾 Inserted {len(processed)} companies from {config.name}")
            self.stats.sources_processed += 1
            
            await asyncio.sleep(config.rate_limit)
            
            return len(processed)
            
        except Exception as e:
            logger.error(f"❌ Error processing {config.name}: {e}", exc_info=True)
            self.stats.sources_failed += 1
            return 0
    
    async def _run_expansion(self, *tiers: int, max_sources: Optional[int] = None) -> Dict[str, int]:
        """Run expansion for specified tiers"""
        logger.info("=" * 80)
        logger.info("🚀 STARTING SEED EXPANSION v3.0")
        logger.info("=" * 80)
        logger.info(f"Tiers: {tiers}")
        logger.info(f"Timestamp: {datetime.now().isoformat()}")
        logger.info("=" * 80)
        
        active_sources = [
            (config.priority, name, config)
            for name, config in SOURCES.items()
            if config.enabled and config.tier in tiers
        ]
        active_sources.sort(key=lambda x: x[0], reverse=True)
        
        if max_sources:
            active_sources = active_sources[:max_sources]
        
        logger.info(f"📋 Processing {len(active_sources)} sources")
        
        results = {}
        for priority, name, config in active_sources:
            count = await self._run_source(name, config)
            results[name] = count
        
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
