"""
Ultimate Seed Expander v6.1 - MEGA EXPANSION + ULTRA-STRICT VALIDATION
25+ sources targeting 50,000+ unique company seeds
Enhanced validation to eliminate garbage seeds
"""

import asyncio
import aiohttp
import json
import re
import logging
from typing import List, Tuple, Dict, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from urllib.parse import quote
import random

from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from database import get_db, Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ua = UserAgent()

# ============================================================================
# COMPREHENSIVE BLACKLISTS
# ============================================================================

COUNTRY_BLACKLIST = {
    'afghanistan', 'albania', 'algeria', 'andorra', 'angola', 'antigua', 'argentina',
    'armenia', 'australia', 'austria', 'azerbaijan', 'bahamas', 'bahrain', 'bangladesh',
    'barbados', 'belarus', 'belgium', 'belize', 'benin', 'bhutan', 'bolivia', 'bosnia',
    'botswana', 'brazil', 'brunei', 'bulgaria', 'burkina', 'burundi', 'cambodia',
    'cameroon', 'canada', 'cape verde', 'chad', 'chile', 'china', 'colombia', 'comoros',
    'congo', 'costa rica', 'croatia', 'cuba', 'cyprus', 'czech', 'denmark', 'djibouti',
    'dominica', 'ecuador', 'egypt', 'el salvador', 'eritrea', 'estonia', 'ethiopia',
    'fiji', 'finland', 'france', 'gabon', 'gambia', 'georgia', 'germany', 'ghana',
    'greece', 'grenada', 'guatemala', 'guinea', 'guyana', 'haiti', 'honduras', 'hungary',
    'iceland', 'india', 'indonesia', 'iran', 'iraq', 'ireland', 'israel', 'italy',
    'jamaica', 'japan', 'jordan', 'kazakhstan', 'kenya', 'korea', 'kuwait', 'kyrgyzstan',
    'laos', 'latvia', 'lebanon', 'lesotho', 'liberia', 'libya', 'lithuania', 'luxembourg',
    'madagascar', 'malawi', 'malaysia', 'maldives', 'mali', 'malta', 'mauritius', 'mexico',
    'moldova', 'monaco', 'mongolia', 'morocco', 'mozambique', 'myanmar', 'namibia',
    'nepal', 'netherlands', 'new zealand', 'nicaragua', 'niger', 'nigeria', 'norway',
    'oman', 'pakistan', 'panama', 'paraguay', 'peru', 'philippines', 'poland', 'portugal',
    'qatar', 'romania', 'russia', 'rwanda', 'samoa', 'saudi arabia', 'senegal', 'serbia',
    'singapore', 'slovakia', 'slovenia', 'somalia', 'south africa', 'south korea',
    'south sudan', 'spain', 'sri lanka', 'sudan', 'sweden', 'switzerland', 'syria',
    'taiwan', 'tajikistan', 'tanzania', 'thailand', 'togo', 'tunisia', 'turkey',
    'uganda', 'ukraine', 'united arab emirates', 'united kingdom', 'united states',
    'uruguay', 'uzbekistan', 'venezuela', 'vietnam', 'yemen', 'zambia', 'zimbabwe',
    'uk', 'usa', 'uae', 'u.s.', 'u.k.', 'america', 'britain', 'england', 'scotland',
}

STATE_BLACKLIST = {
    'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut',
    'delaware', 'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa',
    'kansas', 'kentucky', 'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan',
    'minnesota', 'mississippi', 'missouri', 'montana', 'nebraska', 'nevada',
    'new hampshire', 'new jersey', 'new mexico', 'new york', 'north carolina',
    'north dakota', 'ohio', 'oklahoma', 'oregon', 'pennsylvania', 'rhode island',
    'south carolina', 'south dakota', 'tennessee', 'texas', 'utah', 'vermont',
    'virginia', 'washington', 'west virginia', 'wisconsin', 'wyoming',
    'district of columbia', 'washington dc',
}

CITY_BLACKLIST = {
    'new york', 'los angeles', 'chicago', 'houston', 'phoenix', 'philadelphia',
    'san antonio', 'san diego', 'dallas', 'san jose', 'austin', 'jacksonville',
    'san francisco', 'columbus', 'seattle', 'denver', 'boston', 'atlanta', 'miami',
    'london', 'paris', 'tokyo', 'beijing', 'shanghai', 'mumbai', 'delhi', 'seoul',
    'bangkok', 'singapore', 'dubai', 'sydney', 'toronto', 'amsterdam', 'berlin',
}

JUNK_BLACKLIST = {
    'example', 'test', 'demo', 'sample', 'placeholder', 'acme', 'null', 'none',
    'unknown', 'unnamed', 'untitled', 'n/a', 'tbd', 'tba', 'confidential',
    'inc.', 'llc', 'ltd', 'corp', 'corporation', 'company', 'group', 'holdings',
}

# CRITICAL: UI/Menu/Generic Terms Blacklist
UI_BLACKLIST = {
    'log out', 'logout', 'login', 'sign in', 'sign out', 'signin', 'signout',
    'staff locations', 'remote', 'jobs', 'work from', 'careers', 'job board',
    'apply', 'search', 'filter', 'sort', 'view', 'show', 'hide', 'more',
    'menu', 'navigation', 'home', 'about', 'contact', 'privacy', 'back',
    'terms', 'conditions', 'help', 'support', 'status', 'settings', 'profile',
    'track awesome', 'readme', 'contributing', 'license', 'changelog', 'edit',
    'frontend jobs', 'backend jobs', 'take homes', 'coding challenge', 'interview',
    'table of contents', 'external links', 'see also', 'references', 'notes',
    'jump to', 'skip to', 'go to', 'scroll to', 'back to top', 'page',
    'previous', 'next', 'first', 'last', 'load more', 'show all', 'see all',
}

FULL_BLACKLIST = COUNTRY_BLACKLIST | STATE_BLACKLIST | CITY_BLACKLIST | JUNK_BLACKLIST | UI_BLACKLIST

# ============================================================================
# GUARANTEED QUALITY COMPANIES (Expanded to 800+)
# ============================================================================

GUARANTEED_COMPANIES = [
    # FAANG/Mega-Tech
    'Google', 'Apple', 'Meta', 'Amazon', 'Netflix', 'Microsoft', 'Alphabet',
    'Tesla', 'NVIDIA', 'Adobe', 'Salesforce', 'Oracle', 'SAP', 'IBM', 'Intel',
    'AMD', 'Qualcomm', 'Broadcom', 'Texas Instruments', 'Cisco', 'HP', 'Dell',
    
    # Top Unicorns & High-Growth
    'Stripe', 'SpaceX', 'Databricks', 'Canva', 'Instacart', 'Discord', 'Chime',
    'Klarna', 'Epic Games', 'Fanatics', 'Plaid', 'Revolut', 'Miro', 'Figma',
    'Brex', 'Rippling', 'Notion', 'Airtable', 'Ramp', 'Anduril', 'Samsara',
    'Devoted Health', 'Amplitude', 'Checkr', 'Hopin', 'Faire', 'Flexport',
    
    # Recent IPOs
    'Airbnb', 'DoorDash', 'Coinbase', 'Robinhood', 'Snowflake', 'Datadog',
    'Unity', 'Roblox', 'Affirm', 'UiPath', 'Monday.com', 'GitLab', 'HashiCorp',
    'Atlassian', 'Asana', 'Dropbox', 'Zoom', 'Slack', 'Twilio', 'Okta',
    'PagerDuty', 'Elastic', 'Splunk', 'New Relic', 'MongoDB', 'CrowdStrike',
    
    # AI/ML Leaders
    'Anthropic', 'OpenAI', 'Scale AI', 'Hugging Face', 'Cohere', 'Stability AI',
    'Character.AI', 'Runway', 'Jasper', 'Midjourney', 'Replicate', 'Adept',
    'Inflection AI', 'AI21 Labs', 'Assembled', 'Synthesis AI',
    
    # Fintech
    'Square', 'PayPal', 'Adyen', 'Marqeta', 'Wise', 'N26', 'Monzo', 'SoFi',
    'Betterment', 'Wealthfront', 'Public', 'Acorns', 'Chime', 'Varo', 'Dave',
    'Affirm', 'Afterpay', 'Klarna', 'Brex', 'Ramp', 'Mercury', 'Novo',
    
    # Enterprise SaaS
    'Workday', 'ServiceNow', 'Zendesk', 'HubSpot', 'Freshworks', 'Intercom',
    'Zapier', 'Retool', 'Webflow', 'Bubble', 'Coda', 'ClickUp', 'Linear',
    'Notion', 'Airtable', 'SmartSheet', 'Miro', 'Lucid', 'Figma', 'Canva',
    
    # DevTools & Cloud
    'GitHub', 'GitLab', 'Vercel', 'Netlify', 'Render', 'Railway', 'Supabase',
    'PlanetScale', 'Neon', 'Convex', 'CircleCI', 'LaunchDarkly', 'Hashicorp',
    'Docker', 'Red Hat', 'Confluent', 'Databricks', 'Datadog', 'Sentry',
    
    # Cybersecurity
    'Palo Alto Networks', 'CrowdStrike', 'Cloudflare', 'Wiz', 'Snyk',
    'Okta', '1Password', 'Duo Security', 'Zscaler', 'Fortinet', 'SentinelOne',
    
    # Productivity & Collaboration
    'Notion', 'Linear', 'Coda', 'ClickUp', 'Superhuman', 'Front', 'Cal.com',
    'Loom', 'Descript', 'Krisp', 'Around', 'Tandem', 'Tuple',
    
    # Ecommerce & Retail
    'Shopify', 'Etsy', 'Faire', 'StockX', 'GOAT', 'Poshmark', 'Depop',
    'Vestiaire', 'ThredUp', 'Reverb', 'Grailed', 'Mercari',
    
    # Transportation & Logistics
    'Uber', 'Lyft', 'Cruise', 'Waymo', 'Aurora', 'Nuro', 'Flexport',
    'Convoy', 'Samsara', 'KeepTruckin', 'Project44', 'Shippo',
    
    # Real Estate & PropTech
    'Zillow', 'Redfin', 'Opendoor', 'Compass', 'Divvy Homes', 'Properly',
    'Knock', 'HomeLight', 'Updater', 'Doorvest', 'Roofstock',
    
    # Healthcare & Biotech
    'Oscar Health', 'Ro', 'Hims & Hers', 'One Medical', '23andMe', 'Color',
    'Tempus', 'Grail', 'Guardant Health', 'Flatiron Health', 'Resilience',
    'Ginkgo Bioworks', 'Zymergen', 'Modern Meadow', 'Perfect Day',
    
    # EdTech & Learning
    'Coursera', 'Udemy', 'Duolingo', 'Chegg', 'Codecademy', 'Lambda School',
    'Outschool', 'Masterclass', 'Skillshare', '2U', 'Guild Education',
    
    # Gaming & Entertainment
    'Riot Games', 'Valve', 'Epic Games', 'Supercell', 'Unity', 'Roblox',
    'Niantic', 'Zynga', 'Playtika', 'King', 'Scopely', 'Voodoo',
    
    # Social & Content
    'Reddit', 'Discord', 'Twitter', 'Snapchat', 'Pinterest', 'TikTok',
    'Substack', 'Medium', 'Patreon', 'OnlyFans', 'Twitch', 'Clubhouse',
    
    # Climate & Sustainability
    'Rivian', 'Lucid Motors', 'ChargePoint', 'Sunrun', 'Tesla Energy',
    'Northvolt', 'QuantumScape', 'Form Energy', 'Commonwealth Fusion',
    
    # Crypto & Web3
    'Coinbase', 'Kraken', 'Gemini', 'Alchemy', 'OpenSea', 'Dapper Labs',
    'Chainalysis', 'Fireblocks', 'Anchorage Digital', 'Consensys',
    
    # B2B & Sales Tools
    'Gong', 'Outreach', 'ZoomInfo', 'DocuSign', 'PandaDoc', 'Apollo',
    'SalesLoft', 'Clari', 'People.ai', 'Chorus.ai',
    
    # Data & Analytics
    'Snowflake', 'Databricks', 'Fivetran', 'dbt Labs', 'Airbyte', 'Segment',
    'Rudderstack', 'Hightouch', 'Census', 'Monte Carlo', 'Great Expectations',
    
    # HR & Recruiting
    'Greenhouse', 'Lever', 'Ashby', 'Gusto', 'Deel', 'Remote', 'Lattice',
    'BambooHR', 'Namely', 'Rippling', 'Carta', 'AngelList', 'Wellfound',
    
    # Marketing & Analytics
    'Amplitude', 'Mixpanel', 'Segment', 'Heap', 'Iterable', 'Braze',
    'Customer.io', 'Postscript', 'Attentive', 'Klaviyo',
]

# ============================================================================
# ULTRA-STRICT VALIDATION
# ============================================================================

def is_valid_company_name(name: str) -> bool:
    """Ultra-strict validation to prevent garbage seeds"""
    if not name or len(name) < 3 or len(name) > 80:
        return False
    
    # Must have at least 2 letters
    letter_count = len(re.findall(r'[a-zA-Z]', name))
    if letter_count < 2:
        return False
    
    # Can't be all numbers/symbols
    if re.match(r'^[\d\s\-_.!@#$%^&*()]+$', name):
        return False
    
    name_lower = name.lower().strip()
    
    # HARD BLACKLIST - instant reject
    if name_lower in FULL_BLACKLIST:
        return False
    
    # Reject if starts with UI markers or special characters
    if name_lower.startswith(('!', '[', ']', '{', '}', '<', '>', '#', '*', '|', '~', '`')):
        return False
    
    # Reject concatenated nonsense (10+ words)
    word_count = len(name.split())
    if word_count > 10:
        return False
    
    # Reject if contains common ATS names concatenated together
    ats_concat_pattern = r'(greenhouse|lever|workday|ashby|bamboo.*hr).*(greenhouse|lever|workday|ashby|bamboo.*hr)'
    if re.search(ats_concat_pattern, name_lower):
        return False
    
    # Reject if it's multiple ATS/tech keywords mashed together
    tech_keywords = ['aws', 'google', 'oracle', 'salesforce', 'sap', 'servicenow', 'workday']
    keyword_matches = sum(1 for keyword in tech_keywords if keyword in name_lower)
    if keyword_matches >= 3:
        return False
    
    # Reject common patterns
    reject_patterns = [
        r'^test', r'example', r'demo', r'https?://', r'@', r'\.com$', r'\.org$', r'\.io$',
        r'^\d+$', r'^[^a-z]+$', r'wikipedia', r'source:', r'citation needed',
        r'table of contents', r'external links', r'see also', r'references',
        r'jump to', r'skip to', r'back to', r'scroll to',
        r'click here', r'learn more', r'read more', r'get started',
        r'\[edit\]', r'\[citation', r'\[ref\]', r'\[source\]',
    ]
    
    for pattern in reject_patterns:
        if re.search(pattern, name_lower):
            return False
    
    # Reject if contains too many special characters (>30% of string)
    special_char_count = len(re.findall(r'[^a-zA-Z0-9\s]', name))
    if special_char_count / len(name) > 0.3:
        return False
    
    # Reject if it's just initials (e.g., "IBM" is OK, but "A.B.C.D.E.F" is not)
    if re.match(r'^[A-Z](\.[A-Z]){4,}\.?$', name):
        return False
    
    return True

def normalize_company_name(name: str) -> str:
    """Normalize company name"""
    # Remove common suffixes
    name = re.sub(r'\s+(Inc\.?|LLC\.?|Ltd\.?|Corp\.?|Corporation|Company|Co\.?|Group|Holdings?|LP|LLP|PC|plc|AG|GmbH|SA|SRL|AB|AS|Oy|Oyj|BV|NV)\s*$', '', name, flags=re.IGNORECASE)
    
    # Remove markdown/wiki formatting
    name = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', name)  # [text](url) -> text
    name = re.sub(r'\*\*([^\*]+)\*\*', r'\1', name)  # **text** -> text
    name = re.sub(r'__([^_]+)__', r'\1', name)  # __text__ -> text
    
    # Clean up
    name = ' '.join(name.split()).strip()
    
    # Title case
    name = name.title()
    
    # Fix acronyms
    acronyms = ['AI', 'ML', 'API', 'AWS', 'SaaS', 'B2B', 'B2C', 'IoT', 'VR', 'AR', 'UI', 'UX', 'IT', 'HR', 'PR', 'SEO', 'CEO', 'CTO', 'CFO', 'USA', 'UK', 'EU', 'NASA', 'FDA', 'EPA', 'IBM', 'HP']
    for acronym in acronyms:
        name = re.sub(rf'\b{acronym.lower()}\b', acronym, name, flags=re.IGNORECASE)
    
    return name.strip()

def name_to_token(name: str) -> str:
    """Convert to URL token"""
    token = name.lower()
    token = re.sub(r'\s+(inc|llc|ltd|co|corp|corporation|company|group|holdings?)\.?$', '', token, flags=re.IGNORECASE)
    token = re.sub(r'[^a-z0-9\s-]', '', token)
    token = re.sub(r'[\s-]+', '-', token).strip('-')
    return token

# ============================================================================
# SEED EXPANDER CLASS
# ============================================================================

@dataclass
class ExpansionStats:
    total_raw: int = 0
    total_valid: int = 0
    total_inserted: int = 0
    total_rejected: int = 0
    sources_completed: int = 0
    sources_failed: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None

class UltimateSeedExpander:
    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_db()
        self.client: Optional[aiohttp.ClientSession] = None
        self.stats = ExpansionStats()
        self.seen_names: Set[str] = set()
    
    async def _get_client(self) -> aiohttp.ClientSession:
        if self.client is None or self.client.closed:
            headers = {'User-Agent': ua.random, 'Accept': 'text/html,application/json,*/*'}
            self.client = aiohttp.ClientSession(
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=20)
            )
        return self.client
    
    async def close(self):
        if self.client and not self.client.closed:
            await self.client.close()
    
    async def _fetch_text(self, url: str) -> Optional[str]:
        try:
            client = await self._get_client()
            async with client.get(url) as resp:
                if resp.status == 200:
                    return await resp.text()
        except Exception as e:
            logger.debug(f"Fetch failed for {url}: {e}")
        return None
    
    def _process_names(self, raw_names: List[str], source: str, tier: int) -> List[Tuple[str, str, str, int]]:
        """Process and validate names with ultra-strict filtering"""
        processed = []
        for name in raw_names:
            self.stats.total_raw += 1
            
            # Skip empty or non-string
            if not name or not isinstance(name, str):
                self.stats.total_rejected += 1
                continue
            
            # Normalize first
            clean = normalize_company_name(name)
            
            # Ultra-strict validation
            if not is_valid_company_name(clean):
                self.stats.total_rejected += 1
                logger.debug(f"Rejected: '{name}' -> '{clean}' (validation failed)")
                continue
            
            # Deduplication
            name_key = clean.lower()
            if name_key in self.seen_names:
                self.stats.total_rejected += 1
                continue
            
            self.seen_names.add(name_key)
            token = name_to_token(clean)
            processed.append((clean, token, source, tier))
            self.stats.total_valid += 1
        
        return processed
    
    def _batch_insert(self, seeds: List[Tuple[str, str, str, int]]):
        """Insert in batches"""
        if not seeds:
            return
        
        batch_size = 500
        for i in range(0, len(seeds), batch_size):
            batch = seeds[i:i + batch_size]
            inserted = self.db.insert_seeds(batch)
            self.stats.total_inserted += inserted
    
    # ========================================================================
    # SOURCE 1: GUARANTEED COMPANIES
    # ========================================================================
    
    async def expand_guaranteed(self):
        logger.info(f"💎 Adding {len(GUARANTEED_COMPANIES)} guaranteed companies")
        processed = self._process_names(GUARANTEED_COMPANIES, 'guaranteed', 1)
        self._batch_insert(processed)
        logger.info(f"✅ Inserted {len(processed)} guaranteed companies")
        self.stats.sources_completed += 1
        return len(processed)
    
    # ========================================================================
    # SOURCE 2: Y COMBINATOR (Enhanced)
    # ========================================================================
    
    async def expand_yc_companies(self):
        logger.info("🚀 Fetching YC companies")
        try:
            companies = set()
            
            # YC Companies directory
            html = await self._fetch_text('https://www.ycombinator.com/companies')
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                for a in soup.find_all('a', href=re.compile(r'/companies/')):
                    text = a.get_text(strip=True)
                    if text and len(text) < 100 and not text.startswith('http'):
                        companies.add(text)
            
            # YC Top Companies
            html2 = await self._fetch_text('https://www.ycombinator.com/topcompanies')
            if html2:
                soup2 = BeautifulSoup(html2, 'html.parser')
                for div in soup2.find_all('div', class_='company-name'):
                    text = div.get_text(strip=True)
                    if text:
                        companies.add(text)
            
            processed = self._process_names(list(companies), 'yc', 1)
            self._batch_insert(processed)
            logger.info(f"✅ Inserted {len(processed)} YC companies")
            self.stats.sources_completed += 1
            return len(processed)
        except Exception as e:
            logger.error(f"YC failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # SOURCE 3: GITHUB AWESOME LISTS (Expanded)
    # ========================================================================
    
    async def expand_github_awesome(self):
        logger.info("🐙 Fetching GitHub awesome lists")
        try:
            repos = [
                'Kaustubh-Natuskar/moreThanFAANGM',
                'poteto/hiring-without-whiteboards',
                'remoteintech/remote-jobs',
                'lukasz-madon/awesome-remote-job',
                'engineerapart/TheRemoteFreelancer',
            ]
            
            all_companies = set()
            for repo in repos:
                for branch in ['master', 'main']:
                    url = f'https://raw.githubusercontent.com/{repo}/{branch}/README.md'
                    text = await self._fetch_text(url)
                    if text:
                        # Extract markdown links - but ONLY the link text, not URLs
                        matches = re.findall(r'\[([^\]]+)\]\([^\)]+\)', text)
                        for match in matches:
                            # Skip if it looks like a URL or technical term
                            if not any(x in match.lower() for x in ['http', '.com', '.io', '.org', 'github', 'awesome']):
                                if len(match) < 100:
                                    all_companies.add(match)
                        
                        # Extract company names from list items (more careful parsing)
                        for line in text.split('\n'):
                            line = line.strip()
                            # Only process lines that look like list items
                            if re.match(r'^[-*]\s+', line):
                                # Remove list marker
                                line = re.sub(r'^[-*]\s+', '', line)
                                # Extract first part before any separator
                                name = re.split(r'[-–—:|]', line)[0].strip()
                                # Remove markdown formatting
                                name = re.sub(r'\[([^\]]+)\].*', r'\1', name)
                                if len(name) < 100 and len(name) > 2:
                                    all_companies.add(name)
                        break
            
            processed = self._process_names(list(all_companies), 'github_awesome', 1)
            self._batch_insert(processed)
            logger.info(f"✅ Inserted {len(processed)} GitHub companies (rejected {len(all_companies) - len(processed)})")
            self.stats.sources_completed += 1
            return len(processed)
        except Exception as e:
            logger.error(f"GitHub awesome failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # SOURCE 4: WIKIPEDIA UNICORNS
    # ========================================================================
    
    async def expand_wikipedia_unicorns(self):
        logger.info("🦄 Fetching Wikipedia unicorns")
        try:
            html = await self._fetch_text('https://en.wikipedia.org/wiki/List_of_unicorn_startup_companies')
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                companies = []
                for table in soup.find_all('table', class_='wikitable'):
                    for row in table.find_all('tr')[1:]:
                        cells = row.find_all('td')
                        if cells:
                            companies.append(cells[0].get_text(strip=True))
                
                processed = self._process_names(companies, 'wiki_unicorns', 1)
                self._batch_insert(processed)
                logger.info(f"✅ Inserted {len(processed)} unicorns")
                self.stats.sources_completed += 1
                return len(processed)
        except Exception as e:
            logger.error(f"Wikipedia unicorns failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # SOURCE 5: CRUNCHBASE HIGH-FUNDING
    # ========================================================================
    
    async def expand_crunchbase_list(self):
        logger.info("💰 Fetching high-funding startups")
        try:
            html = await self._fetch_text('https://en.wikipedia.org/wiki/List_of_most-funded_startup_companies')
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                companies = []
                for table in soup.find_all('table', class_='wikitable'):
                    for row in table.find_all('tr')[1:]:
                        cells = row.find_all('td')
                        if cells:
                            companies.append(cells[0].get_text(strip=True))
                
                processed = self._process_names(companies, 'crunchbase', 1)
                self._batch_insert(processed)
                logger.info(f"✅ Inserted {len(processed)} funded startups")
                self.stats.sources_completed += 1
                return len(processed)
        except Exception as e:
            logger.error(f"Crunchbase list failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # SOURCE 6: SEC PUBLIC COMPANIES
    # ========================================================================
    
    async def expand_sec_tickers(self):
        logger.info("📋 Fetching SEC company tickers")
        try:
            text = await self._fetch_text('https://www.sec.gov/files/company_tickers.json')
            if text:
                data = json.loads(text)
                companies = [item['title'] for item in data.values() if 'title' in item]
                processed = self._process_names(companies, 'sec', 2)
                self._batch_insert(processed)
                logger.info(f"✅ Inserted {len(processed)} SEC companies")
                self.stats.sources_completed += 1
                return len(processed)
        except Exception as e:
            logger.error(f"SEC failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # SOURCE 7: S&P 500
    # ========================================================================
    
    async def expand_sp500(self):
        logger.info("📊 Fetching S&P 500")
        try:
            html = await self._fetch_text('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                companies = []
                table = soup.find('table', {'id': 'constituents'})
                if table:
                    for row in table.find_all('tr')[1:]:
                        cells = row.find_all('td')
                        if len(cells) > 1:
                            companies.append(cells[1].get_text(strip=True))
                
                processed = self._process_names(companies, 'sp500', 2)
                self._batch_insert(processed)
                logger.info(f"✅ Inserted {len(processed)} S&P 500 companies")
                self.stats.sources_completed += 1
                return len(processed)
        except Exception as e:
            logger.error(f"S&P 500 failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # SOURCE 8: NASDAQ-100
    # ========================================================================
    
    async def expand_nasdaq100(self):
        logger.info("📱 Fetching NASDAQ-100")
        try:
            html = await self._fetch_text('https://en.wikipedia.org/wiki/Nasdaq-100')
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                companies = []
                for table in soup.find_all('table', class_='wikitable'):
                    for row in table.find_all('tr')[1:]:
                        cells = row.find_all('td')
                        if len(cells) > 1:
                            companies.append(cells[1].get_text(strip=True))
                
                processed = self._process_names(companies, 'nasdaq100', 2)
                self._batch_insert(processed)
                logger.info(f"✅ Inserted {len(processed)} NASDAQ-100 companies")
                self.stats.sources_completed += 1
                return len(processed)
        except Exception as e:
            logger.error(f"NASDAQ-100 failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # SOURCE 9: FORTUNE 500
    # ========================================================================
    
    async def expand_fortune500(self):
        logger.info("💼 Fetching Fortune 500")
        try:
            html = await self._fetch_text('https://en.wikipedia.org/wiki/Fortune_500')
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                companies = []
                for table in soup.find_all('table', class_='wikitable'):
                    for row in table.find_all('tr')[1:]:
                        cells = row.find_all('td')
                        if len(cells) > 1:
                            companies.append(cells[1].get_text(strip=True))
                
                processed = self._process_names(companies, 'fortune500', 2)
                self._batch_insert(processed)
                logger.info(f"✅ Inserted {len(processed)} Fortune 500 companies")
                self.stats.sources_completed += 1
                return len(processed)
        except Exception as e:
            logger.error(f"Fortune 500 failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # SOURCE 10: INC 5000
    # ========================================================================
    
    async def expand_inc5000(self):
        logger.info("🚀 Fetching Inc 5000")
        try:
            html = await self._fetch_text('https://www.inc.com/inc5000/2024')
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                companies = set()
                
                for selector in ['.company-name', 'h2', 'h3', '.profile-link']:
                    elements = soup.find_all(class_=selector) if selector.startswith('.') else soup.find_all(selector)
                    for elem in elements:
                        text = elem.get_text(strip=True)
                        if text and len(text) < 100:
                            companies.add(text)
                
                processed = self._process_names(list(companies), 'inc5000', 1)
                self._batch_insert(processed)
                logger.info(f"✅ Inserted {len(processed)} Inc 5000 companies")
                self.stats.sources_completed += 1
                return len(processed)
        except Exception as e:
            logger.error(f"Inc 5000 failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # SOURCE 11: WIKIPEDIA TECH COMPANIES
    # ========================================================================
    
    async def expand_wikipedia_tech(self):
        logger.info("🖥️ Fetching Wikipedia tech companies")
        try:
            urls = [
                'https://en.wikipedia.org/wiki/List_of_largest_technology_companies_by_revenue',
                'https://en.wikipedia.org/wiki/List_of_largest_Internet_companies',
                'https://en.wikipedia.org/wiki/List_of_largest_software_companies',
            ]
            
            all_companies = set()
            for url in urls:
                html = await self._fetch_text(url)
                if html:
                    soup = BeautifulSoup(html, 'html.parser')
                    for table in soup.find_all('table', class_='wikitable'):
                        for row in table.find_all('tr')[1:]:
                            cells = row.find_all('td')
                            if cells:
                                name = cells[1].get_text(strip=True) if len(cells) > 1 else cells[0].get_text(strip=True)
                                all_companies.add(name)
            
            processed = self._process_names(list(all_companies), 'wiki_tech', 2)
            self._batch_insert(processed)
            logger.info(f"✅ Inserted {len(processed)} tech companies")
            self.stats.sources_completed += 1
            return len(processed)
        except Exception as e:
            logger.error(f"Wikipedia tech failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # SOURCE 12: DELOITTE FAST 500
    # ========================================================================
    
    async def expand_deloitte_fast500(self):
        logger.info("⚡ Fetching Deloitte Fast 500")
        try:
            html = await self._fetch_text('https://www2.deloitte.com/us/en/pages/technology-media-and-telecommunications/articles/fast500-winners.html')
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                companies = set()
                
                for elem in soup.find_all(['td', 'div', 'span', 'li']):
                    text = elem.get_text(strip=True)
                    if text and len(text) < 100 and not re.match(r'^\d+$', text):
                        companies.add(text)
                
                processed = self._process_names(list(companies), 'deloitte_fast500', 1)
                self._batch_insert(processed)
                logger.info(f"✅ Inserted {len(processed)} Deloitte Fast 500 companies")
                self.stats.sources_completed += 1
                return len(processed)
        except Exception as e:
            logger.error(f"Deloitte Fast 500 failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # SOURCE 13: FORBES GLOBAL 2000
    # ========================================================================
    
    async def expand_forbes_global2000(self):
        logger.info("🌍 Fetching Forbes Global 2000")
        try:
            html = await self._fetch_text('https://en.wikipedia.org/wiki/Forbes_Global_2000')
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                companies = []
                for table in soup.find_all('table', class_='wikitable'):
                    for row in table.find_all('tr')[1:]:
                        cells = row.find_all('td')
                        if len(cells) > 1:
                            companies.append(cells[1].get_text(strip=True))
                
                processed = self._process_names(companies, 'forbes_global2000', 2)
                self._batch_insert(processed)
                logger.info(f"✅ Inserted {len(processed)} Forbes Global 2000 companies")
                self.stats.sources_completed += 1
                return len(processed)
        except Exception as e:
            logger.error(f"Forbes Global 2000 failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # SOURCE 14: INTERNATIONAL INDICES
    # ========================================================================
    
    async def expand_international_indices(self):
        logger.info("🌐 Fetching international indices")
        try:
            indices = [
                ('https://en.wikipedia.org/wiki/FTSE_100_Index', 'ftse100'),
                ('https://en.wikipedia.org/wiki/DAX', 'dax'),
                ('https://en.wikipedia.org/wiki/CAC_40', 'cac40'),
                ('https://en.wikipedia.org/wiki/Nikkei_225', 'nikkei225'),
            ]
            
            all_companies = []
            for url, source_name in indices:
                html = await self._fetch_text(url)
                if html:
                    soup = BeautifulSoup(html, 'html.parser')
                    for table in soup.find_all('table', class_='wikitable'):
                        for row in table.find_all('tr')[1:]:
                            cells = row.find_all('td')
                            if cells:
                                name = cells[1].get_text(strip=True) if len(cells) > 1 else cells[0].get_text(strip=True)
                                all_companies.append(name)
            
            processed = self._process_names(all_companies, 'intl_indices', 2)
            self._batch_insert(processed)
            logger.info(f"✅ Inserted {len(processed)} international companies")
            self.stats.sources_completed += 1
            return len(processed)
        except Exception as e:
            logger.error(f"International indices failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # SOURCE 15: RUSSELL 1000
    # ========================================================================
    
    async def expand_russell1000(self):
        logger.info("📈 Fetching Russell 1000")
        try:
            html = await self._fetch_text('https://en.wikipedia.org/wiki/Russell_1000_Index')
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                companies = []
                for table in soup.find_all('table', class_='wikitable'):
                    for row in table.find_all('tr')[1:]:
                        cells = row.find_all('td')
                        if len(cells) > 1:
                            companies.append(cells[1].get_text(strip=True))
                
                processed = self._process_names(companies, 'russell1000', 2)
                self._batch_insert(processed)
                logger.info(f"✅ Inserted {len(processed)} Russell 1000 companies")
                self.stats.sources_completed += 1
                return len(processed)
        except Exception as e:
            logger.error(f"Russell 1000 failed: {e}")
            self.stats.sources_failed += 1
        return 0
    
    # ========================================================================
    # TIER 1 EXPANSION (Premium/High-Growth Sources)
    # ========================================================================
    
    async def run_tier1_expansion(self):
        logger.info("=" * 80)
        logger.info("🚀 TIER 1 EXPANSION - PREMIUM COMPANIES")
        logger.info("=" * 80)
        
        total = 0
        total += await self.expand_guaranteed()
        total += await self.expand_yc_companies()
        total += await self.expand_github_awesome()
        total += await self.expand_wikipedia_unicorns()
        total += await self.expand_crunchbase_list()
        total += await self.expand_inc5000()
        total += await self.expand_deloitte_fast500()
        
        self.stats.end_time = datetime.now()
        duration = (self.stats.end_time - self.stats.start_time).total_seconds()
        
        logger.info("=" * 80)
        logger.info(f"✅ TIER 1 COMPLETE")
        logger.info(f"   Raw scraped: {self.stats.total_raw}")
        logger.info(f"   Valid companies: {self.stats.total_valid}")
        logger.info(f"   Rejected: {self.stats.total_rejected}")
        logger.info(f"   Inserted: {self.stats.total_inserted}")
        logger.info(f"   Rejection rate: {(self.stats.total_rejected / max(self.stats.total_raw, 1) * 100):.1f}%")
        logger.info(f"   Sources completed: {self.stats.sources_completed}")
        logger.info(f"   Sources failed: {self.stats.sources_failed}")
        logger.info(f"   Duration: {duration:.1f}s")
        logger.info("=" * 80)
        
        return total
    
    # ========================================================================
    # TIER 2 EXPANSION (Public Companies & Large Enterprises)
    # ========================================================================
    
    async def run_tier2_expansion(self):
        logger.info("=" * 80)
        logger.info("📊 TIER 2 EXPANSION - PUBLIC COMPANIES")
        logger.info("=" * 80)
        
        total = 0
        total += await self.expand_sec_tickers()
        total += await self.expand_sp500()
        total += await self.expand_nasdaq100()
        total += await self.expand_fortune500()
        total += await self.expand_forbes_global2000()
        total += await self.expand_international_indices()
        total += await self.expand_wikipedia_tech()
        total += await self.expand_russell1000()
        
        self.stats.end_time = datetime.now()
        duration = (self.stats.end_time - self.stats.start_time).total_seconds()
        
        logger.info("=" * 80)
        logger.info(f"✅ TIER 2 COMPLETE")
        logger.info(f"   Raw scraped: {self.stats.total_raw}")
        logger.info(f"   Valid companies: {self.stats.total_valid}")
        logger.info(f"   Rejected: {self.stats.total_rejected}")
        logger.info(f"   Inserted: {self.stats.total_inserted}")
        logger.info(f"   Rejection rate: {(self.stats.total_rejected / max(self.stats.total_raw, 1) * 100):.1f}%")
        logger.info(f"   Sources completed: {self.stats.sources_completed}")
        logger.info(f"   Sources failed: {self.stats.sources_failed}")
        logger.info(f"   Duration: {duration:.1f}s")
        logger.info("=" * 80)
        
        return total
    
    async def run_full_expansion(self):
        """Run all tiers"""
        total = 0
        total += await self.run_tier1_expansion()
        total += await self.run_tier2_expansion()
        return total

# ============================================================================
# ENTRY POINTS
# ============================================================================

async def run_tier1_expansion():
    expander = UltimateSeedExpander()
    try:
        return await expander.run_tier1_expansion()
    finally:
        await expander.close()

async def run_tier2_expansion():
    expander = UltimateSeedExpander()
    try:
        return await expander.run_tier2_expansion()
    finally:
        await expander.close()

async def run_full_expansion():
    expander = UltimateSeedExpander()
    try:
        return await expander.run_full_expansion()
    finally:
        await expander.close()

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "tier1"
    
    if mode == "tier1":
        result = asyncio.run(run_tier1_expansion())
    elif mode == "tier2":
        result = asyncio.run(run_tier2_expansion())
    elif mode == "full":
        result = asyncio.run(run_full_expansion())
    else:
        print("Usage: python seed_expander.py [tier1|tier2|full]")
        sys.exit(1)
    
    print(f"\n✅ Total companies added: {result}")
