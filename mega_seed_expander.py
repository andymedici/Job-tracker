"""
Mega Seed Expander - 20+ Sources for 50,000+ Companies
=======================================================
Sources:
- Tier 1 (Premium): YC, VC Portfolios, Inc 5000, Forbes Lists, Guaranteed List
- Tier 2 (Good): Wikipedia, SEC EDGAR, Built In, Indeed, Glassdoor
- Tier 3 (Supplemental): Product Hunt, GitHub Awesome Lists
"""

import asyncio
import aiohttp
import json
import re
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple
from bs4 import BeautifulSoup
import hashlib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# VALIDATION AND BLACKLISTS
# =============================================================================

# UI/Navigation terms to filter out
UI_BLACKLIST = {
    'login', 'logout', 'sign in', 'sign up', 'register', 'subscribe', 'menu',
    'home', 'about', 'contact', 'help', 'support', 'faq', 'terms', 'privacy',
    'cookie', 'settings', 'profile', 'account', 'dashboard', 'admin', 'search',
    'filter', 'sort', 'next', 'previous', 'back', 'forward', 'close', 'open',
    'expand', 'collapse', 'show', 'hide', 'more', 'less', 'all', 'none',
    'select', 'choose', 'apply', 'cancel', 'submit', 'save', 'edit', 'delete',
    'download', 'upload', 'share', 'print', 'export', 'import', 'copy', 'paste',
    'view all', 'see more', 'load more', 'read more', 'learn more', 'click here',
}

# Geographic terms
GEO_BLACKLIST = {
    # Countries
    'united states', 'united kingdom', 'canada', 'australia', 'germany', 'france',
    'japan', 'china', 'india', 'brazil', 'mexico', 'spain', 'italy', 'netherlands',
    'sweden', 'norway', 'denmark', 'finland', 'switzerland', 'austria', 'belgium',
    'ireland', 'portugal', 'poland', 'russia', 'south korea', 'singapore', 'israel',
    'usa', 'uk', 'eu', 'uae',
    # US States
    'california', 'texas', 'new york', 'florida', 'illinois', 'pennsylvania',
    'ohio', 'georgia', 'michigan', 'north carolina', 'washington', 'arizona',
    'massachusetts', 'tennessee', 'indiana', 'missouri', 'maryland', 'wisconsin',
    'colorado', 'minnesota', 'south carolina', 'alabama', 'louisiana', 'kentucky',
    'oregon', 'oklahoma', 'connecticut', 'utah', 'iowa', 'nevada', 'arkansas',
    'mississippi', 'kansas', 'new mexico', 'nebraska', 'west virginia', 'idaho',
    'hawaii', 'new hampshire', 'maine', 'montana', 'rhode island', 'delaware',
    'south dakota', 'north dakota', 'alaska', 'vermont', 'wyoming', 'virginia',
    # Major Cities
    'new york city', 'los angeles', 'chicago', 'houston', 'phoenix', 'philadelphia',
    'san antonio', 'san diego', 'dallas', 'san jose', 'austin', 'jacksonville',
    'san francisco', 'seattle', 'denver', 'boston', 'nashville', 'baltimore',
    'portland', 'las vegas', 'miami', 'atlanta', 'oakland', 'minneapolis',
    'london', 'paris', 'berlin', 'tokyo', 'beijing', 'shanghai', 'mumbai',
    'toronto', 'vancouver', 'sydney', 'melbourne', 'amsterdam', 'dublin',
    'nyc', 'sf', 'la', 'bay area', 'silicon valley',
}

# Junk/Generic terms
JUNK_BLACKLIST = {
    'test', 'demo', 'example', 'sample', 'placeholder', 'template', 'default',
    'unknown', 'undefined', 'null', 'none', 'n/a', 'tbd', 'coming soon',
    'lorem ipsum', 'foo', 'bar', 'baz', 'qux', 'hello world', 'untitled',
    'your company', 'company name', 'acme', 'abc company', 'xyz corp',
    'startup', 'new company', 'my company', 'our company', 'the company',
    'industry', 'technology', 'software', 'services', 'solutions', 'systems',
    'global', 'international', 'worldwide', 'national', 'regional', 'local',
    'digital', 'analytics', 'consulting', 'management', 'partners', 'group',
    'inc', 'llc', 'ltd', 'corp', 'corporation', 'company', 'enterprises',
    'holdings', 'ventures', 'capital', 'investments', 'fund', 'portfolio',
    'various', 'multiple', 'several', 'many', 'other', 'misc', 'miscellaneous',
    'featured', 'popular', 'trending', 'top', 'best', 'leading', 'premier',
}

# Combined blacklist
ALL_BLACKLISTS = UI_BLACKLIST | GEO_BLACKLIST | JUNK_BLACKLIST


@dataclass
class SeedCompany:
    """Validated seed company"""
    name: str
    source: str
    tier: int  # 1 = premium, 2 = good, 3 = supplemental
    confidence: float = 1.0
    url: Optional[str] = None
    metadata: Dict = field(default_factory=dict)


class SeedValidator:
    """Ultra-strict validation for seed companies"""
    
    @staticmethod
    def validate(name: str) -> bool:
        """Validate a potential company name"""
        if not name:
            return False
        
        name = name.strip()
        name_lower = name.lower()
        
        # Length checks
        if len(name) < 2 or len(name) > 100:
            return False
        
        # Must have at least 2 letters
        letter_count = sum(1 for c in name if c.isalpha())
        if letter_count < 2:
            return False
        
        # Special character limit (<30% of string)
        special_count = sum(1 for c in name if not c.isalnum() and c != ' ')
        if special_count / len(name) > 0.3:
            return False
        
        # Word count limit
        words = name.split()
        if len(words) > 8:
            return False
        
        # Blacklist check
        if name_lower in ALL_BLACKLISTS:
            return False
        
        # Check if any blacklist term is a significant part
        for term in ALL_BLACKLISTS:
            if len(term) > 3 and term in name_lower:
                # Allow if blacklist term is small part of larger name
                if len(term) / len(name_lower) > 0.7:
                    return False
        
        # Reject if starts with generic terms
        generic_starters = ['the ', 'a ', 'an ', 'this ', 'that ', 'your ', 'our ', 'my ']
        for starter in generic_starters:
            if name_lower.startswith(starter) and len(name_lower) < 15:
                return False
        
        # Reject URLs
        if 'http' in name_lower or 'www.' in name_lower or '.com' in name_lower:
            return False
        
        # Reject email addresses
        if '@' in name:
            return False
        
        # Reject pure numbers
        if name.replace(' ', '').replace('-', '').isnumeric():
            return False
        
        return True
    
    @staticmethod
    def normalize(name: str) -> str:
        """Normalize company name for deduplication"""
        name = name.strip()
        # Remove common suffixes
        suffixes = [', Inc.', ', Inc', ' Inc.', ' Inc', ', LLC', ' LLC', 
                   ', Ltd.', ' Ltd.', ' Ltd', ', Corp.', ' Corp.', ' Corp',
                   ', Co.', ' Co.', ' Co', ' Corporation', ' Company']
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
        return name.strip()
    
    @staticmethod
    def generate_token(name: str) -> str:
        """Generate a dedup token from company name"""
        normalized = SeedValidator.normalize(name).lower()
        # Remove all non-alphanumeric
        token = re.sub(r'[^a-z0-9]', '', normalized)
        return token


# =============================================================================
# GUARANTEED SEED LIST (Curated high-quality)
# =============================================================================

GUARANTEED_SEEDS = {
    # FAANG+
    'Apple', 'Google', 'Amazon', 'Meta', 'Microsoft', 'Netflix', 'Nvidia',
    
    # AI Leaders
    'Anthropic', 'OpenAI', 'Cohere', 'Hugging Face', 'Stability AI', 'Midjourney',
    'Inflection AI', 'Character AI', 'Mistral AI', 'Perplexity', 'Runway',
    'Scale AI', 'Weights & Biases', 'Anyscale', 'Modal', 'Replicate',
    
    # Unicorns (2023-2024)
    'Stripe', 'SpaceX', 'Databricks', 'Canva', 'Checkout.com', 'Revolut',
    'Klarna', 'Figma', 'Discord', 'Notion', 'Airtable', 'Miro', 'Webflow',
    'Vercel', 'Supabase', 'PlanetScale', 'Railway', 'Render', 'Fly.io',
    
    # Developer Tools
    'GitHub', 'GitLab', 'Atlassian', 'JetBrains', 'Postman', 'Snyk',
    'HashiCorp', 'Datadog', 'New Relic', 'Sentry', 'LaunchDarkly', 'Split',
    'CircleCI', 'Buildkite', 'Sourcegraph', 'Retool', 'Temporal', 'Temporal',
    
    # Fintech
    'Plaid', 'Ramp', 'Brex', 'Mercury', 'Carta', 'Gusto', 'Rippling',
    'Navan', 'Airwallex', 'Wise', 'Marqeta', 'Affirm', 'Chime', 'Current',
    'Dave', 'SoFi', 'Robinhood', 'Coinbase', 'Kraken', 'BlockFi',
    
    # Healthcare/Biotech
    'Tempus', 'Color Health', 'Ro', 'Hims', 'Thirty Madison', 'Cerebral',
    'Modern Health', 'Lyra Health', 'Spring Health', 'Headspace', 'Calm',
    'Noom', 'Oura', 'Whoop', 'Eight Sleep', 'Levels', 'Carrot Fertility',
    
    # E-commerce/Retail Tech
    'Shopify', 'BigCommerce', 'Bolt', 'Fast', 'Faire', 'Whatnot', 'Poshmark',
    'ThredUp', 'StockX', 'Goat', 'Fanatics', 'Flexport', 'Shippo', 'Shipbob',
    
    # Security
    'CrowdStrike', 'SentinelOne', 'Lacework', 'Wiz', 'Orca Security', 'Snyk',
    '1Password', 'Bitwarden', 'Okta', 'Auth0', 'Ping Identity', 'ForgeRock',
    
    # Data/Analytics
    'Snowflake', 'Databricks', 'dbt Labs', 'Fivetran', 'Airbyte', 'Monte Carlo',
    'Hex', 'Observable', 'Mode', 'Metabase', 'Looker', 'ThoughtSpot', 'Sigma',
    
    # Infrastructure
    'Cloudflare', 'Fastly', 'Akamai', 'DigitalOcean', 'Linode', 'Vultr',
    'MongoDB', 'Cockroach Labs', 'SingleStore', 'Timescale', 'ClickHouse',
    'Redis Labs', 'Elastic', 'Confluent', 'Starburst', 'Trino',
    
    # Productivity/Collaboration
    'Slack', 'Zoom', 'Loom', 'Calendly', 'Doodle', 'Typeform', 'Coda',
    'ClickUp', 'Monday.com', 'Asana', 'Linear', 'Height', 'Shortcut', 'Jira',
    
    # Sales/Marketing Tech
    'Salesforce', 'HubSpot', 'Marketo', 'Mailchimp', 'Klaviyo', 'Attentive',
    'Gong', 'Outreach', 'Salesloft', 'Apollo', 'ZoomInfo', 'Clearbit', '6sense',
    
    # HR Tech
    'Workday', 'Lattice', 'Culture Amp', 'Leapsome', '15Five', 'Deel', 'Remote',
    'Oyster', 'Papaya Global', 'Velocity Global', 'Greenhouse', 'Lever', 'Ashby',
    
    # Real Estate/PropTech
    'Zillow', 'Redfin', 'Opendoor', 'Compass', 'Divvy Homes', 'Homeward',
    'Ribbon', 'Pacaso', 'Arrived Homes', 'Roofstock', 'Lessen', 'Rhino',
    
    # Transportation/Logistics
    'Uber', 'Lyft', 'DoorDash', 'Instacart', 'Gopuff', 'Getir', 'Gorillas',
    'Convoy', 'Flexport', 'Project44', 'FourKites', 'Samsara', 'Motive',
    
    # Gaming
    'Roblox', 'Epic Games', 'Unity', 'Niantic', 'Scopely', 'AppLovin',
    'ironSource', 'Playrix', 'Supercell', 'MiHoYo', 'Dream Games',
    
    # Climate/Clean Tech
    'Tesla', 'Rivian', 'Lucid', 'ChargePoint', 'EVgo', 'Electrify America',
    'Redwood Materials', 'Form Energy', 'Commonwealth Fusion', 'Twelve',
    
    # Recent Notable IPOs (2023-2024)
    'Arm', 'Instacart', 'Klaviyo', 'Reddit', 'Astera Labs', 'Rubrik',
    
    # Fortune 500 Tech
    'IBM', 'Oracle', 'SAP', 'Adobe', 'Salesforce', 'VMware', 'Dell',
    'HP', 'Intel', 'AMD', 'Qualcomm', 'Broadcom', 'Texas Instruments',
    'Cisco', 'Juniper Networks', 'Arista Networks', 'Palo Alto Networks',
    
    # Consulting/Professional Services
    'McKinsey', 'BCG', 'Bain', 'Deloitte', 'KPMG', 'PwC', 'EY', 'Accenture',
}


# =============================================================================
# VC PORTFOLIOS (40 VCs = ~10,000+ companies)
# =============================================================================

VC_PORTFOLIOS = {
    'a]16z': {
        'url': 'https://a16z.com/portfolio/',
        'pattern': r'<a[^>]*href="https://a16z\.com/portfolio/([^"]+)"[^>]*>([^<]+)</a>',
    },
    'sequoia': {
        'url': 'https://www.sequoiacap.com/our-companies/',
        'backup_api': 'https://api.sequoiacap.com/companies',
    },
    'accel': {
        'url': 'https://www.accel.com/portfolio',
    },
    'benchmark': {
        'url': 'https://www.benchmark.com/portfolio',
    },
    'greylock': {
        'url': 'https://greylock.com/portfolio/',
    },
    'index': {
        'url': 'https://www.indexventures.com/companies',
    },
    'lightspeed': {
        'url': 'https://lsvp.com/portfolio/',
    },
    'general_catalyst': {
        'url': 'https://www.generalcatalyst.com/portfolio',
    },
    'bessemer': {
        'url': 'https://www.bvp.com/portfolio',
    },
    'insight': {
        'url': 'https://www.insightpartners.com/portfolio/',
    },
    'tiger_global': {
        'url': 'https://www.tigerglobal.com/portfolio',
    },
    'coatue': {
        'url': 'https://www.coatue.com/portfolio',
    },
    'addition': {
        'url': 'https://www.addition.com/portfolio',
    },
    'founders_fund': {
        'url': 'https://foundersfund.com/portfolio/',
    },
    'nea': {
        'url': 'https://www.nea.com/portfolio',
    },
    'kleiner_perkins': {
        'url': 'https://www.kleinerperkins.com/portfolio',
    },
    'redpoint': {
        'url': 'https://www.redpoint.com/companies/',
    },
    'ggv': {
        'url': 'https://www.ggvc.com/portfolio/',
    },
    'spark': {
        'url': 'https://www.sparkcapital.com/portfolio',
    },
    'usv': {
        'url': 'https://www.usv.com/portfolio',
    },
    'first_round': {
        'url': 'https://firstround.com/portfolio/',
    },
    'thrive': {
        'url': 'https://www.thrivecap.com/portfolio',
    },
    'ribbit': {
        'url': 'https://ribbitcap.com/companies/',
    },
    'khosla': {
        'url': 'https://www.khoslaventures.com/portfolio',
    },
    'craft': {
        'url': 'https://www.craftventures.com/portfolio',
    },
    'felicis': {
        'url': 'https://www.felicis.com/portfolio',
    },
    '8vc': {
        'url': 'https://www.8vc.com/portfolio',
    },
    'menlo': {
        'url': 'https://www.menlovc.com/portfolio',
    },
    'battery': {
        'url': 'https://www.battery.com/portfolio/',
    },
    'ivp': {
        'url': 'https://www.ivp.com/portfolio/',
    },
    'canaan': {
        'url': 'https://www.canaan.com/portfolio',
    },
    'scale_vp': {
        'url': 'https://www.scalevp.com/portfolio',
    },
    'emergence': {
        'url': 'https://www.emcap.com/portfolio/',
    },
    'wing': {
        'url': 'https://wing.vc/portfolio/',
    },
    'initialized': {
        'url': 'https://initialized.com/portfolio/',
    },
    'social_capital': {
        'url': 'https://www.socialcapital.com/portfolio',
    },
    'fifth_wall': {
        'url': 'https://fifthwall.com/portfolio',
    },
    'lux': {
        'url': 'https://www.luxcapital.com/companies',
    },
    'mayfield': {
        'url': 'https://www.mayfield.com/portfolio/',
    },
    'yc': {
        'url': 'https://www.ycombinator.com/companies',
        'special': 'yc',
    },
}


# =============================================================================
# SEED EXPANSION SOURCES
# =============================================================================

class SeedExpander:
    """Expand seeds from 20+ sources"""
    
    def __init__(self, db_path: str = 'job_intel.db'):
        self.db_path = db_path
        self.validator = SeedValidator()
        self.seen_tokens: Set[str] = set()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        }
    
    async def expand_all(self, tiers: List[int] = [1, 2, 3]) -> Dict[str, List[SeedCompany]]:
        """Run all expansion sources"""
        results = {}
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            # Tier 1: Premium sources
            if 1 in tiers:
                logger.info("Expanding Tier 1 (Premium) sources...")
                results['guaranteed'] = self._expand_guaranteed()
                results['yc'] = await self._expand_yc(session)
                results['github_awesome'] = await self._expand_github_awesome(session)
                results['vc_portfolios'] = await self._expand_vc_portfolios(session)
                results['inc_5000'] = await self._expand_inc_5000(session)
                results['forbes'] = await self._expand_forbes_lists(session)
            
            # Tier 2: Good sources
            if 2 in tiers:
                logger.info("Expanding Tier 2 (Good) sources...")
                results['wikipedia'] = await self._expand_wikipedia(session)
                results['sec_edgar'] = await self._expand_sec_edgar(session)
                results['builtin'] = await self._expand_builtin(session)
                results['indeed'] = await self._expand_indeed(session)
                results['glassdoor'] = await self._expand_glassdoor(session)
            
            # Tier 3: Supplemental
            if 3 in tiers:
                logger.info("Expanding Tier 3 (Supplemental) sources...")
                results['producthunt'] = await self._expand_producthunt(session)
                results['wellfound'] = await self._expand_wellfound(session)
        
        return results
    
    def _expand_guaranteed(self) -> List[SeedCompany]:
        """Return guaranteed high-quality seeds"""
        seeds = []
        for name in GUARANTEED_SEEDS:
            if self._is_new(name):
                seeds.append(SeedCompany(
                    name=name,
                    source='guaranteed',
                    tier=1,
                    confidence=1.0,
                ))
        logger.info(f"Guaranteed list: {len(seeds)} seeds")
        return seeds
    
    async def _expand_yc(self, session: aiohttp.ClientSession) -> List[SeedCompany]:
        """Expand from Y Combinator directory"""
        seeds = []
        
        # YC has an Algolia search API
        try:
            # First try the companies page
            url = "https://www.ycombinator.com/companies"
            async with session.get(url, timeout=30) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Find company names in various patterns
                    for link in soup.find_all('a', href=True):
                        href = link.get('href', '')
                        if '/companies/' in href and href.count('/') == 2:
                            company_slug = href.split('/companies/')[-1].strip('/')
                            name = company_slug.replace('-', ' ').title()
                            if self._is_new(name) and self.validator.validate(name):
                                seeds.append(SeedCompany(
                                    name=name,
                                    source='yc',
                                    tier=1,
                                    confidence=0.95,
                                    url=f"https://www.ycombinator.com{href}",
                                ))
            
            # Also try Algolia API
            algolia_url = "https://45bwzj1sgc-dsn.algolia.net/1/indexes/*/queries"
            payload = {
                "requests": [{
                    "indexName": "YCCompany_production",
                    "params": "hitsPerPage=1000&page=0"
                }]
            }
            headers = {
                **self.headers,
                'x-algolia-api-key': 'ZGNjNzRlODdmOWVjMWFhYjZlZDA0YjA2YTRlNjc3NTA0ODQ4MDViM2VlZGYzNjc1NjY2N2M5NjYxOTg0ZjMwMG1pbGlzZWNvbmRzVW50aWw9MTY5NTM5NzIwMA==',
                'x-algolia-application-id': '45BWZJ1SGC',
            }
            async with session.post(algolia_url, json=payload, headers=headers, timeout=30) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for hit in data.get('results', [{}])[0].get('hits', []):
                        name = hit.get('name', '')
                        if self._is_new(name) and self.validator.validate(name):
                            seeds.append(SeedCompany(
                                name=name,
                                source='yc',
                                tier=1,
                                confidence=0.95,
                                metadata={'batch': hit.get('batch', '')},
                            ))
        except Exception as e:
            logger.warning(f"YC expansion error: {e}")
        
        logger.info(f"YC: {len(seeds)} seeds")
        return seeds
    
    async def _expand_github_awesome(self, session: aiohttp.ClientSession) -> List[SeedCompany]:
        """Expand from GitHub awesome lists"""
        seeds = []
        
        awesome_lists = [
            'https://raw.githubusercontent.com/jnv/lists/master/data/companies.json',
            'https://raw.githubusercontent.com/Kristories/awesome-startup-credits-for-startups/main/README.md',
            'https://raw.githubusercontent.com/kuchin/awesome-startup-credits/main/README.md',
            'https://raw.githubusercontent.com/kdeldycke/awesome-engineering-team-management/main/readme.md',
            'https://raw.githubusercontent.com/goabstract/Awesome-Design-Tools/master/README.md',
            'https://raw.githubusercontent.com/trimstray/the-book-of-secret-knowledge/master/README.md',
        ]
        
        company_pattern = r'\[([A-Z][A-Za-z0-9\s&\.\-]+)\]\(https?://[^\)]+\)'
        
        for url in awesome_lists:
            try:
                async with session.get(url, timeout=15) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        
                        if url.endswith('.json'):
                            try:
                                data = json.loads(text)
                                for item in data:
                                    if isinstance(item, str):
                                        name = item
                                    elif isinstance(item, dict):
                                        name = item.get('name', item.get('company', ''))
                                    else:
                                        continue
                                    if self._is_new(name) and self.validator.validate(name):
                                        seeds.append(SeedCompany(
                                            name=name,
                                            source='github_awesome',
                                            tier=1,
                                            confidence=0.8,
                                        ))
                            except json.JSONDecodeError:
                                pass
                        else:
                            # Parse markdown
                            matches = re.findall(company_pattern, text)
                            for name in matches:
                                name = name.strip()
                                if self._is_new(name) and self.validator.validate(name):
                                    seeds.append(SeedCompany(
                                        name=name,
                                        source='github_awesome',
                                        tier=1,
                                        confidence=0.8,
                                    ))
            except Exception as e:
                logger.debug(f"Error fetching {url}: {e}")
        
        logger.info(f"GitHub Awesome: {len(seeds)} seeds")
        return seeds
    
    async def _expand_vc_portfolios(self, session: aiohttp.ClientSession) -> List[SeedCompany]:
        """Expand from VC portfolio pages"""
        seeds = []
        
        for vc_name, config in VC_PORTFOLIOS.items():
            try:
                url = config.get('url', '')
                if not url:
                    continue
                
                async with session.get(url, timeout=20) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Look for company names in common patterns
                        # 1. Links with company names
                        for link in soup.find_all('a', href=True):
                            text = link.get_text(strip=True)
                            if text and 3 <= len(text) <= 50:
                                if self._is_new(text) and self.validator.validate(text):
                                    seeds.append(SeedCompany(
                                        name=text,
                                        source=f'vc_{vc_name}',
                                        tier=1,
                                        confidence=0.85,
                                    ))
                        
                        # 2. Company cards/divs
                        for card in soup.find_all(['div', 'article', 'li'], class_=re.compile(r'company|portfolio|card', re.I)):
                            # Look for h2, h3, or strong tags
                            title = card.find(['h2', 'h3', 'h4', 'strong'])
                            if title:
                                name = title.get_text(strip=True)
                                if self._is_new(name) and self.validator.validate(name):
                                    seeds.append(SeedCompany(
                                        name=name,
                                        source=f'vc_{vc_name}',
                                        tier=1,
                                        confidence=0.85,
                                    ))
                
            except Exception as e:
                logger.debug(f"Error fetching VC {vc_name}: {e}")
        
        logger.info(f"VC Portfolios: {len(seeds)} seeds")
        return seeds
    
    async def _expand_wikipedia(self, session: aiohttp.ClientSession) -> List[SeedCompany]:
        """Expand from Wikipedia company lists"""
        seeds = []
        
        wiki_pages = [
            'https://en.wikipedia.org/wiki/List_of_unicorn_startup_companies',
            'https://en.wikipedia.org/wiki/List_of_largest_technology_companies_by_revenue',
            'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
            'https://en.wikipedia.org/wiki/List_of_Fortune_500_companies',
            'https://en.wikipedia.org/wiki/List_of_largest_Internet_companies',
            'https://en.wikipedia.org/wiki/List_of_mergers_and_acquisitions_by_Alphabet',
            'https://en.wikipedia.org/wiki/List_of_mergers_and_acquisitions_by_Microsoft',
            'https://en.wikipedia.org/wiki/List_of_mergers_and_acquisitions_by_Apple',
            'https://en.wikipedia.org/wiki/List_of_mergers_and_acquisitions_by_Amazon',
            'https://en.wikipedia.org/wiki/List_of_largest_companies_by_revenue',
        ]
        
        for url in wiki_pages:
            try:
                async with session.get(url, timeout=20) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Find tables
                        for table in soup.find_all('table', class_='wikitable'):
                            for row in table.find_all('tr'):
                                cells = row.find_all(['td', 'th'])
                                if cells:
                                    # Usually company name is in first or second cell
                                    for cell in cells[:2]:
                                        link = cell.find('a')
                                        if link:
                                            name = link.get_text(strip=True)
                                            if self._is_new(name) and self.validator.validate(name):
                                                seeds.append(SeedCompany(
                                                    name=name,
                                                    source='wikipedia',
                                                    tier=2,
                                                    confidence=0.9,
                                                ))
                                                break
            except Exception as e:
                logger.debug(f"Error fetching Wikipedia {url}: {e}")
        
        logger.info(f"Wikipedia: {len(seeds)} seeds")
        return seeds
    
    async def _expand_sec_edgar(self, session: aiohttp.ClientSession) -> List[SeedCompany]:
        """Expand from SEC EDGAR company tickers"""
        seeds = []
        
        try:
            url = "https://www.sec.gov/files/company_tickers.json"
            async with session.get(url, timeout=30) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    for key, company in data.items():
                        name = company.get('title', '')
                        if self._is_new(name) and self.validator.validate(name):
                            seeds.append(SeedCompany(
                                name=name,
                                source='sec_edgar',
                                tier=2,
                                confidence=0.95,
                                metadata={'ticker': company.get('ticker', '')},
                            ))
        except Exception as e:
            logger.warning(f"SEC EDGAR error: {e}")
        
        logger.info(f"SEC EDGAR: {len(seeds)} seeds")
        return seeds
    
    async def _expand_inc_5000(self, session: aiohttp.ClientSession) -> List[SeedCompany]:
        """Expand from Inc 5000 list"""
        seeds = []
        
        # Inc 5000 typically requires JavaScript, try API or cached data
        years = [2024, 2023, 2022, 2021, 2020]
        
        for year in years:
            try:
                url = f"https://www.inc.com/inc5000/{year}"
                async with session.get(url, timeout=20) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Look for company names
                        for item in soup.find_all(['h2', 'h3', 'a'], class_=re.compile(r'company|title|name', re.I)):
                            name = item.get_text(strip=True)
                            if self._is_new(name) and self.validator.validate(name):
                                seeds.append(SeedCompany(
                                    name=name,
                                    source=f'inc_5000_{year}',
                                    tier=1,
                                    confidence=0.9,
                                ))
            except Exception as e:
                logger.debug(f"Inc 5000 {year} error: {e}")
        
        logger.info(f"Inc 5000: {len(seeds)} seeds")
        return seeds
    
    async def _expand_forbes_lists(self, session: aiohttp.ClientSession) -> List[SeedCompany]:
        """Expand from Forbes lists (Cloud 100, AI 50, etc.)"""
        seeds = []
        
        forbes_lists = [
            'https://www.forbes.com/lists/cloud100/',
            'https://www.forbes.com/lists/ai50/',
            'https://www.forbes.com/lists/fintech50/',
        ]
        
        for url in forbes_lists:
            try:
                async with session.get(url, timeout=20) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        for item in soup.find_all(['h2', 'h3', 'a', 'div'], class_=re.compile(r'name|company|title', re.I)):
                            name = item.get_text(strip=True)
                            if self._is_new(name) and self.validator.validate(name):
                                seeds.append(SeedCompany(
                                    name=name,
                                    source='forbes',
                                    tier=1,
                                    confidence=0.9,
                                ))
            except Exception as e:
                logger.debug(f"Forbes error {url}: {e}")
        
        logger.info(f"Forbes: {len(seeds)} seeds")
        return seeds
    
    async def _expand_builtin(self, session: aiohttp.ClientSession) -> List[SeedCompany]:
        """Expand from Built In tech hub lists"""
        seeds = []
        
        cities = ['', 'nyc', 'seattle', 'chicago', 'boston', 'austin', 'la', 'colorado', 'atlanta', 'sf']
        
        for city in cities:
            try:
                base = f"https://builtin{city}.com" if city else "https://builtin.com"
                url = f"{base}/companies"
                
                async with session.get(url, timeout=20) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        for link in soup.find_all('a', href=True):
                            href = link.get('href', '')
                            if '/company/' in href or '/companies/' in href:
                                name = link.get_text(strip=True)
                                if self._is_new(name) and self.validator.validate(name):
                                    seeds.append(SeedCompany(
                                        name=name,
                                        source=f'builtin_{city or "main"}',
                                        tier=2,
                                        confidence=0.8,
                                    ))
            except Exception as e:
                logger.debug(f"Built In {city} error: {e}")
        
        logger.info(f"Built In: {len(seeds)} seeds")
        return seeds
    
    async def _expand_indeed(self, session: aiohttp.ClientSession) -> List[SeedCompany]:
        """Expand from Indeed best places to work"""
        seeds = []
        
        try:
            url = "https://www.indeed.com/cmp/top-employers"
            async with session.get(url, timeout=20) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    for item in soup.find_all(['a', 'h2', 'h3'], class_=re.compile(r'company|employer', re.I)):
                        name = item.get_text(strip=True)
                        if self._is_new(name) and self.validator.validate(name):
                            seeds.append(SeedCompany(
                                name=name,
                                source='indeed',
                                tier=2,
                                confidence=0.85,
                            ))
        except Exception as e:
            logger.debug(f"Indeed error: {e}")
        
        logger.info(f"Indeed: {len(seeds)} seeds")
        return seeds
    
    async def _expand_glassdoor(self, session: aiohttp.ClientSession) -> List[SeedCompany]:
        """Expand from Glassdoor best places to work"""
        seeds = []
        
        try:
            url = "https://www.glassdoor.com/Award/Best-Places-to-Work-LST_KQ0,19.htm"
            async with session.get(url, timeout=20) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    for item in soup.find_all(['a', 'span'], class_=re.compile(r'employer|company', re.I)):
                        name = item.get_text(strip=True)
                        if self._is_new(name) and self.validator.validate(name):
                            seeds.append(SeedCompany(
                                name=name,
                                source='glassdoor',
                                tier=2,
                                confidence=0.85,
                            ))
        except Exception as e:
            logger.debug(f"Glassdoor error: {e}")
        
        logger.info(f"Glassdoor: {len(seeds)} seeds")
        return seeds
    
    async def _expand_producthunt(self, session: aiohttp.ClientSession) -> List[SeedCompany]:
        """Expand from Product Hunt"""
        seeds = []
        
        try:
            # Product Hunt uses GraphQL
            url = "https://www.producthunt.com/topics/developer-tools"
            async with session.get(url, timeout=20) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    for item in soup.find_all(['a', 'h2', 'h3']):
                        text = item.get_text(strip=True)
                        if self._is_new(text) and self.validator.validate(text) and len(text) < 40:
                            seeds.append(SeedCompany(
                                name=text,
                                source='producthunt',
                                tier=3,
                                confidence=0.7,
                            ))
        except Exception as e:
            logger.debug(f"Product Hunt error: {e}")
        
        logger.info(f"Product Hunt: {len(seeds)} seeds")
        return seeds
    
    async def _expand_wellfound(self, session: aiohttp.ClientSession) -> List[SeedCompany]:
        """Expand from Wellfound (formerly AngelList)"""
        seeds = []
        
        try:
            url = "https://wellfound.com/discover/startups"
            async with session.get(url, timeout=20) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    for link in soup.find_all('a', href=True):
                        href = link.get('href', '')
                        if '/company/' in href:
                            name = link.get_text(strip=True)
                            if self._is_new(name) and self.validator.validate(name):
                                seeds.append(SeedCompany(
                                    name=name,
                                    source='wellfound',
                                    tier=1,
                                    confidence=0.85,
                                ))
        except Exception as e:
            logger.debug(f"Wellfound error: {e}")
        
        logger.info(f"Wellfound: {len(seeds)} seeds")
        return seeds
    
    def _is_new(self, name: str) -> bool:
        """Check if company name hasn't been seen"""
        token = self.validator.generate_token(name)
        if token in self.seen_tokens:
            return False
        self.seen_tokens.add(token)
        return True
    
    def save_to_database(self, seeds: Dict[str, List[SeedCompany]]):
        """Save all seeds to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS seed_companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                source TEXT,
                tier INTEGER DEFAULT 3,
                confidence REAL DEFAULT 1.0,
                url TEXT,
                metadata TEXT DEFAULT '{}',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                tested_at TEXT,
                found BOOLEAN DEFAULT FALSE
            )
        ''')
        
        total = 0
        for source, seed_list in seeds.items():
            for seed in seed_list:
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO seed_companies (name, source, tier, confidence, url, metadata)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        self.validator.normalize(seed.name),
                        seed.source,
                        seed.tier,
                        seed.confidence,
                        seed.url,
                        json.dumps(seed.metadata),
                    ))
                    if cursor.rowcount > 0:
                        total += 1
                except sqlite3.IntegrityError:
                    pass
        
        conn.commit()
        conn.close()
        
        logger.info(f"Saved {total} new seeds to database")
        return total


# =============================================================================
# CLI
# =============================================================================

async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Mega Seed Expander')
    parser.add_argument('--tiers', nargs='+', type=int, default=[1, 2, 3],
                       help='Tiers to expand (1=premium, 2=good, 3=supplemental)')
    parser.add_argument('--db', default='job_intel.db', help='Database path')
    parser.add_argument('--dry-run', action='store_true', help='Do not save to database')
    
    args = parser.parse_args()
    
    expander = SeedExpander(db_path=args.db)
    
    logger.info(f"Starting mega expansion for tiers: {args.tiers}")
    results = await expander.expand_all(tiers=args.tiers)
    
    # Summary
    print("\n" + "="*60)
    print("MEGA SEED EXPANSION RESULTS")
    print("="*60)
    
    total = 0
    for source, seeds in sorted(results.items()):
        count = len(seeds)
        total += count
        print(f"  {source}: {count} seeds")
    
    print(f"\n  TOTAL: {total} unique seeds")
    
    if not args.dry_run:
        saved = expander.save_to_database(results)
        print(f"  SAVED: {saved} new seeds to database")
    else:
        print("  (Dry run - not saved)")


# =============================================================================
# HELPER FUNCTION FOR APP.PY INTEGRATION
# =============================================================================

async def run_expansion(db=None, tiers: List[int] = None) -> Dict:
    """
    Main entry point for app.py integration.
    
    Args:
        db: Database object with get_connection() method (optional, uses sqlite if not provided)
        tiers: List of tiers to expand [1, 2, 3]
        
    Returns:
        Stats dictionary with results
    """
    if tiers is None:
        tiers = [1, 2]
    
    logger.info(f"🌍 Starting mega seed expansion for tiers: {tiers}")
    
    # Use PostgreSQL if db provided, otherwise sqlite
    if db is not None:
        expander = SeedExpander(db_path=None)  # Won't use sqlite
        results = await expander.expand_all(tiers=tiers)
        
        # Save to PostgreSQL
        total_saved = 0
        for source, seeds in results.items():
            for seed in seeds:
                try:
                    with db.get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO seed_companies (name, source, tier)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (name) DO NOTHING
                            """, (seed.name, source, seed.tier))
                            if cur.rowcount > 0:
                                total_saved += 1
                        conn.commit()
                except Exception as e:
                    logger.debug(f"Error saving seed {seed.name}: {e}")
        
        total_found = sum(len(seeds) for seeds in results.values())
        
        stats = {
            'success': True,
            'tiers': tiers,
            'total_found': total_found,
            'total_saved': total_saved,
            'by_source': {source: len(seeds) for source, seeds in results.items()},
        }
    else:
        # Fallback to sqlite
        expander = SeedExpander(db_path='job_intel.db')
        results = await expander.expand_all(tiers=tiers)
        saved = expander.save_to_database(results)
        
        stats = {
            'success': True,
            'tiers': tiers,
            'total_found': sum(len(seeds) for seeds in results.values()),
            'total_saved': saved,
            'by_source': {source: len(seeds) for source, seeds in results.items()},
        }
    
    logger.info(f"✅ Mega expansion complete: {stats['total_found']} found, {stats['total_saved']} saved")
    return stats


if __name__ == '__main__':
    asyncio.run(main())
