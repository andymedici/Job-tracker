"""
Seed Token Expander v2.0
========================
Discovers new company names from multiple tiered sources to expand
the seed token database for job board discovery.

TIER 1 - High Hit Rate (Tech/Startups):
- Y Combinator Company Directory
- GitHub Organizations API
- ProductHunt
- GitHub Awesome Lists
- Crunchbase (free tier)

TIER 2 - Medium Hit Rate (Established Businesses):
- SEC EDGAR Public Companies
- USASpending.gov Federal Contractors
- SAM.gov Federal Vendors
- Inc 5000 / Fortune Lists
- Glassdoor Company Lists

Features:
- Priority-based seed processing
- Source hit rate tracking
- Automatic low-performer disabling
- Trickle processing to stay efficient
"""

import asyncio
import aiohttp
import json
import re
import logging
import os
from typing import List, Set, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from xml.etree import ElementTree as ET

from database import get_db, Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class SourceConfig:
    """Configuration for a company source."""
    name: str
    tier: int
    priority: int
    enabled: bool = True


# Source configurations
SOURCES = {
    # Tier 1 - High hit rate (tech/startups)
    'yc': SourceConfig('Y Combinator', tier=1, priority=90),
    'github_orgs': SourceConfig('GitHub Organizations', tier=1, priority=85),
    'producthunt': SourceConfig('ProductHunt', tier=1, priority=80),
    'github_awesome': SourceConfig('GitHub Awesome Lists', tier=1, priority=75),
    'crunchbase': SourceConfig('Crunchbase', tier=1, priority=70),
    
    # Tier 2 - Medium hit rate (established businesses)
    'sec_edgar': SourceConfig('SEC EDGAR', tier=2, priority=55),
    'usaspending': SourceConfig('USASpending.gov', tier=2, priority=50),
    'sam_gov': SourceConfig('SAM.gov', tier=2, priority=45),
    'inc5000': SourceConfig('Inc 5000', tier=2, priority=50),
    'fortune500': SourceConfig('Fortune 500', tier=2, priority=55),
    'glassdoor': SourceConfig('Glassdoor', tier=2, priority=45),
}


class SeedExpander:
    """Expands seed tokens from multiple tiered sources."""
    
    def __init__(self, db: Database = None):
        self.db = db or get_db()
        self.session: Optional[aiohttp.ClientSession] = None
        self.discovered_companies: Set[str] = set()
        self.results: Dict[str, List[str]] = {}
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=60)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    'User-Agent': 'Mozilla/5.0 (compatible; JobIntelBot/2.0; +https://github.com/job-intel)',
                    'Accept': 'application/json, text/html, */*'
                }
            )
        return self.session
    
    async def close(self):
        """Close the session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    # ========================================================================
    # TIER 1 SOURCES - High Hit Rate (Tech/Startups)
    # ========================================================================
    
    async def fetch_yc_companies(self) -> List[str]:
        """Fetch companies from Y Combinator's public Algolia API."""
        source = 'yc'
        logger.info(f"🚀 Fetching from Y Combinator...")
        companies = []
        
        try:
            session = await self.get_session()
            
            url = "https://45bwzj1sgc-dsn.algolia.net/1/indexes/*/queries"
            headers = {
                'x-algolia-api-key': 'NDYzYmNmMTRjYzU3YTY1MTNlMzgwMzY5NGIwNmNkMTNkNjE2NjE1NTQ5OGY4NjkwMmZhNzRkZjVjYTViZDY1N3Jlc3RyaWN0SW5kaWNlcz1ZQ0NvbXBhbnlfcHJvZHVjdGlvbiZ0YWdGaWx0ZXJzPSU1QiUyMnljZGNfcHVibGljJTIyJTVE',
                'x-algolia-application-id': '45BWZJ1SGC',
                'Content-Type': 'application/json'
            }
            
            for page in range(0, 50):  # Up to 5000 companies
                payload = {
                    "requests": [{
                        "indexName": "YCCompany_production",
                        "params": f"hitsPerPage=100&page={page}"
                    }]
                }
                
                async with session.post(url, json=payload, headers=headers) as resp:
                    if resp.status != 200:
                        break
                    
                    data = await resp.json()
                    results = data.get('results', [{}])[0]
                    hits = results.get('hits', [])
                    
                    if not hits:
                        break
                    
                    for hit in hits:
                        name = hit.get('name', '')
                        if name and self._is_valid_company_name(name):
                            companies.append(self._clean_company_name(name))
                
                await asyncio.sleep(0.1)
            
            logger.info(f"   Found {len(companies)} YC companies")
            
        except Exception as e:
            logger.error(f"   Error fetching YC: {e}")
        
        self.results[source] = companies
        return companies
    
    async def fetch_github_organizations(self) -> List[str]:
        """Fetch organization names from GitHub API."""
        source = 'github_orgs'
        logger.info(f"🐙 Fetching from GitHub Organizations...")
        companies = []
        
        try:
            session = await self.get_session()
            
            # Fetch multiple pages of organizations
            # GitHub's org API is paginated by since= (org id)
            since = 0
            
            for _ in range(20):  # ~2000 orgs
                url = f"https://api.github.com/organizations?per_page=100&since={since}"
                
                async with session.get(url) as resp:
                    if resp.status != 200:
                        break
                    
                    orgs = await resp.json()
                    if not orgs:
                        break
                    
                    for org in orgs:
                        name = org.get('login', '')
                        if name and len(name) >= 2:
                            # Convert org login to readable name
                            readable = name.replace('-', ' ').replace('_', ' ')
                            if self._is_valid_company_name(readable):
                                companies.append(self._clean_company_name(readable))
                        since = org.get('id', since)
                    
                await asyncio.sleep(0.5)  # Rate limit for GitHub
            
            logger.info(f"   Found {len(companies)} GitHub organizations")
            
        except Exception as e:
            logger.error(f"   Error fetching GitHub orgs: {e}")
        
        self.results[source] = companies
        return companies
    
    async def fetch_producthunt(self) -> List[str]:
        """Fetch company names from ProductHunt (via posts)."""
        source = 'producthunt'
        logger.info(f"🏹 Fetching from ProductHunt...")
        companies = []
        
        try:
            session = await self.get_session()
            
            # ProductHunt has a GraphQL API but also rendered pages we can parse
            # We'll fetch the featured/popular products pages
            urls = [
                "https://www.producthunt.com/topics/productivity",
                "https://www.producthunt.com/topics/developer-tools",
                "https://www.producthunt.com/topics/artificial-intelligence",
                "https://www.producthunt.com/topics/saas",
                "https://www.producthunt.com/topics/marketing",
                "https://www.producthunt.com/topics/fintech",
            ]
            
            for url in urls:
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            html = await resp.text()
                            # Extract product names from the page
                            # Look for data-test="post-name" or similar patterns
                            names = re.findall(r'data-test="post-name"[^>]*>([^<]+)<', html)
                            names += re.findall(r'"name":"([^"]{2,50})"', html)
                            
                            for name in names:
                                if self._is_valid_company_name(name):
                                    companies.append(self._clean_company_name(name))
                    
                    await asyncio.sleep(1)  # Be nice to PH
                except:
                    pass
            
            # Deduplicate
            companies = list(set(companies))
            logger.info(f"   Found {len(companies)} ProductHunt companies")
            
        except Exception as e:
            logger.error(f"   Error fetching ProductHunt: {e}")
        
        self.results[source] = companies
        return companies
    
    async def fetch_github_awesome_lists(self) -> List[str]:
        """Fetch company names from curated GitHub awesome lists."""
        source = 'github_awesome'
        logger.info(f"📚 Fetching from GitHub Awesome Lists...")
        companies = []
        
        # Curated list of awesome lists that contain company names
        awesome_repos = [
            ("sindresorhus/awesome", "readme.md"),
            ("kahun/awesome-sysadmin", "README.md"),
            ("avelino/awesome-go", "README.md"),
            ("vinta/awesome-python", "README.md"),
            ("awesome-selfhosted/awesome-selfhosted", "README.md"),
            ("akullpp/awesome-java", "README.md"),
            ("sorrycc/awesome-javascript", "README.md"),
        ]
        
        try:
            session = await self.get_session()
            
            for repo, file_path in awesome_repos:
                try:
                    url = f"https://raw.githubusercontent.com/{repo}/master/{file_path}"
                    
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            content = await resp.text()
                            
                            # Extract company/project names from markdown links
                            # Pattern: [Name](url) or **Name**
                            link_names = re.findall(r'\[([^\]]{2,40})\]\(https?://[^\)]+\)', content)
                            bold_names = re.findall(r'\*\*([^*]{2,40})\*\*', content)
                            
                            for name in link_names + bold_names:
                                # Filter out generic words
                                if self._is_valid_company_name(name) and not self._is_generic_word(name):
                                    companies.append(self._clean_company_name(name))
                    
                    await asyncio.sleep(0.3)
                except:
                    pass
            
            # Deduplicate
            companies = list(set(companies))
            logger.info(f"   Found {len(companies)} from Awesome Lists")
            
        except Exception as e:
            logger.error(f"   Error fetching Awesome Lists: {e}")
        
        self.results[source] = companies
        return companies
    
    async def fetch_crunchbase_free(self) -> List[str]:
        """Fetch company names from Crunchbase free/public sources."""
        source = 'crunchbase'
        logger.info(f"💰 Fetching from Crunchbase (public)...")
        companies = []
        
        # Crunchbase doesn't have a free API, but we can get company names
        # from their public pages and news
        try:
            session = await self.get_session()
            
            # Fetch from Crunchbase news/lists (public pages)
            urls = [
                "https://www.crunchbase.com/lists/most-recent-funding-rounds",
                "https://www.crunchbase.com/lists/recently-funded-startups",
            ]
            
            for url in urls:
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            html = await resp.text()
                            # Extract organization names
                            names = re.findall(r'/organization/([a-z0-9-]+)', html)
                            
                            for name in names:
                                readable = name.replace('-', ' ').title()
                                if self._is_valid_company_name(readable):
                                    companies.append(self._clean_company_name(readable))
                    
                    await asyncio.sleep(2)  # Be very nice to Crunchbase
                except:
                    pass
            
            companies = list(set(companies))
            logger.info(f"   Found {len(companies)} from Crunchbase")
            
        except Exception as e:
            logger.error(f"   Error fetching Crunchbase: {e}")
        
        self.results[source] = companies
        return companies
    
    # ========================================================================
    # TIER 2 SOURCES - Medium Hit Rate (Established Businesses)
    # ========================================================================
    
    async def fetch_sec_edgar(self) -> List[str]:
        """Fetch public company names from SEC EDGAR filings."""
        source = 'sec_edgar'
        logger.info(f"📈 Fetching from SEC EDGAR...")
        companies = []
        
        try:
            session = await self.get_session()
            
            # SEC EDGAR company search API
            # Fetch recent 10-K filers (annual reports = active companies)
            url = "https://www.sec.gov/cgi-bin/browse-edgar"
            params = {
                'action': 'getcurrent',
                'type': '10-K',
                'company': '',
                'dateb': '',
                'owner': 'include',
                'count': '100',
                'output': 'atom'
            }
            
            headers = {
                'User-Agent': 'JobIntelBot/2.0 (contact@example.com)',
                'Accept': 'application/atom+xml'
            }
            
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    content = await resp.text()
                    
                    # Parse XML/Atom feed
                    try:
                        root = ET.fromstring(content)
                        # Find company names in the feed
                        for entry in root.findall('.//{http://www.w3.org/2005/Atom}entry'):
                            title = entry.find('{http://www.w3.org/2005/Atom}title')
                            if title is not None and title.text:
                                # Title format is usually "CompanyName (10-K)"
                                name = re.sub(r'\s*\([^)]+\)\s*$', '', title.text).strip()
                                if self._is_valid_company_name(name):
                                    companies.append(self._clean_company_name(name))
                    except:
                        # Fallback: regex extraction
                        names = re.findall(r'<title>([^<]+)\s*\(10-K\)</title>', content)
                        for name in names:
                            if self._is_valid_company_name(name):
                                companies.append(self._clean_company_name(name))
            
            # Also fetch company tickers list
            ticker_url = "https://www.sec.gov/files/company_tickers.json"
            try:
                async with session.get(ticker_url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for key, company in data.items():
                            name = company.get('title', '')
                            if self._is_valid_company_name(name):
                                companies.append(self._clean_company_name(name))
            except:
                pass
            
            companies = list(set(companies))
            logger.info(f"   Found {len(companies)} from SEC EDGAR")
            
        except Exception as e:
            logger.error(f"   Error fetching SEC EDGAR: {e}")
        
        self.results[source] = companies
        return companies
    
    async def fetch_usaspending(self) -> List[str]:
        """Fetch federal contractor names from USASpending.gov API."""
        source = 'usaspending'
        logger.info(f"🇺🇸 Fetching from USASpending.gov...")
        companies = []
        
        try:
            session = await self.get_session()
            
            # USASpending API - get top recipients
            url = "https://api.usaspending.gov/api/v2/recipient/"
            
            # Get recipients by different categories
            for keyword in ['technology', 'software', 'consulting', 'engineering']:
                try:
                    search_url = "https://api.usaspending.gov/api/v2/autocomplete/recipient/"
                    payload = {
                        "search_text": keyword,
                        "limit": 100
                    }
                    
                    async with session.post(search_url, json=payload) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            recipients = data.get('results', [])
                            
                            for recipient in recipients:
                                name = recipient.get('recipient_name', '')
                                if self._is_valid_company_name(name):
                                    companies.append(self._clean_company_name(name))
                    
                    await asyncio.sleep(0.5)
                except:
                    pass
            
            # Also get top award recipients
            try:
                top_url = "https://api.usaspending.gov/api/v2/recipient/state/"
                async with session.get(top_url) as resp:
                    if resp.status == 200:
                        # This returns state data, not directly useful
                        pass
            except:
                pass
            
            companies = list(set(companies))
            logger.info(f"   Found {len(companies)} from USASpending")
            
        except Exception as e:
            logger.error(f"   Error fetching USASpending: {e}")
        
        self.results[source] = companies
        return companies
    
    async def fetch_sam_gov(self) -> List[str]:
        """Fetch federal vendors from SAM.gov (limited without API key)."""
        source = 'sam_gov'
        logger.info(f"🏛️ Fetching from SAM.gov...")
        companies = []
        
        # SAM.gov requires API key for full access
        # We can still get some data from public pages
        try:
            session = await self.get_session()
            
            # Public SAM.gov data opportunities page
            url = "https://sam.gov/api/prod/sgs/v1/search/"
            params = {
                'index': 'opp',
                'q': 'software',
                'page': '0',
                'size': '100',
                'mode': 'search',
                'sort': '-modifiedDate'
            }
            
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        opps = data.get('_embedded', {}).get('results', [])
                        
                        for opp in opps:
                            # Get awardee/vendor names if available
                            name = opp.get('organizationName', '') or opp.get('title', '')
                            if self._is_valid_company_name(name):
                                companies.append(self._clean_company_name(name))
            except:
                pass
            
            companies = list(set(companies))
            logger.info(f"   Found {len(companies)} from SAM.gov")
            
        except Exception as e:
            logger.error(f"   Error fetching SAM.gov: {e}")
        
        self.results[source] = companies
        return companies
    
    async def fetch_inc5000(self) -> List[str]:
        """Fetch Inc 5000 fastest-growing companies."""
        source = 'inc5000'
        logger.info(f"📊 Fetching from Inc 5000...")
        companies = []
        
        try:
            session = await self.get_session()
            
            # Inc 5000 has a JSON data endpoint for their list
            url = "https://www.inc.com/inc5000/2024/top-private-companies-2024-inc5000.html"
            
            async with session.get(url) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    
                    # Extract company names from the page
                    # Look for company name patterns
                    names = re.findall(r'"company"\s*:\s*"([^"]+)"', html)
                    names += re.findall(r'class="company[^"]*"[^>]*>([^<]+)<', html)
                    names += re.findall(r'data-company="([^"]+)"', html)
                    
                    for name in names:
                        if self._is_valid_company_name(name):
                            companies.append(self._clean_company_name(name))
            
            companies = list(set(companies))
            logger.info(f"   Found {len(companies)} from Inc 5000")
            
        except Exception as e:
            logger.error(f"   Error fetching Inc 5000: {e}")
        
        self.results[source] = companies
        return companies
    
    async def fetch_fortune500(self) -> List[str]:
        """Fetch Fortune 500 companies."""
        source = 'fortune500'
        logger.info(f"💼 Fetching from Fortune 500...")
        companies = []
        
        try:
            session = await self.get_session()
            
            # Fortune maintains a public API for their lists
            url = "https://fortune.com/ranking/fortune500/"
            
            async with session.get(url) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    
                    # Extract company names
                    names = re.findall(r'"companyName"\s*:\s*"([^"]+)"', html)
                    names += re.findall(r'class="[^"]*company-name[^"]*"[^>]*>([^<]+)<', html)
                    names += re.findall(r'/company/([^/"]+)', html)
                    
                    for name in names:
                        readable = name.replace('-', ' ').title()
                        if self._is_valid_company_name(readable):
                            companies.append(self._clean_company_name(readable))
            
            # Also add well-known Fortune 500 manually (they rarely change)
            fortune_500_static = [
                "Walmart", "Amazon", "Apple", "UnitedHealth", "Berkshire Hathaway",
                "CVS Health", "ExxonMobil", "Alphabet", "McKesson", "Cencora",
                "Costco", "Microsoft", "Cigna", "Cardinal Health", "Chevron",
                "Home Depot", "Walgreens", "Marathon Petroleum", "Elevance Health",
                "Kroger", "Ford", "Verizon", "JPMorgan Chase", "General Motors",
                "Centene", "Meta", "Comcast", "Phillips 66", "Valero Energy",
                "Dell Technologies", "Target", "Fannie Mae", "UPS", "Lowe's",
                "Bank of America", "Johnson & Johnson", "Archer Daniels Midland",
                "FedEx", "Humana", "Wells Fargo", "State Farm", "Pfizer",
                "Citigroup", "PepsiCo", "Intel", "Procter & Gamble", "General Electric",
                "IBM", "MetLife", "Prudential", "Albertsons", "Walt Disney",
                "Energy Transfer", "Lockheed Martin", "Goldman Sachs", "Freddie Mac",
                "Sysco", "HP", "Boeing", "StoneX", "Morgan Stanley",
                "Raytheon", "HCA Healthcare", "AbbVie", "Dow", "Tesla",
                "Allstate", "AIG", "Best Buy", "Charter Communications", "Merck",
                "New York Life", "Caterpillar", "Cisco", "TJX", "Publix",
                "ConocoPhillips", "Liberty Mutual", "Progressive", "Nationwide",
                "Tyson Foods", "Bristol-Myers Squibb", "Nike", "Deere", "American Express"
            ]
            
            for name in fortune_500_static:
                if self._is_valid_company_name(name):
                    companies.append(self._clean_company_name(name))
            
            companies = list(set(companies))
            logger.info(f"   Found {len(companies)} from Fortune 500")
            
        except Exception as e:
            logger.error(f"   Error fetching Fortune 500: {e}")
        
        self.results[source] = companies
        return companies
    
    async def fetch_glassdoor(self) -> List[str]:
        """Fetch company names from Glassdoor best places to work lists."""
        source = 'glassdoor'
        logger.info(f"🏢 Fetching from Glassdoor...")
        companies = []
        
        try:
            session = await self.get_session()
            
            # Glassdoor's best places to work
            url = "https://www.glassdoor.com/Award/Best-Places-to-Work-LST_KQ0,19.htm"
            
            async with session.get(url) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    
                    # Extract company names from the awards page
                    names = re.findall(r'"employerName"\s*:\s*"([^"]+)"', html)
                    names += re.findall(r'/Overview/Working-at-([^-]+)-', html)
                    names += re.findall(r'data-employer-name="([^"]+)"', html)
                    
                    for name in names:
                        readable = name.replace('-', ' ')
                        if self._is_valid_company_name(readable):
                            companies.append(self._clean_company_name(readable))
            
            companies = list(set(companies))
            logger.info(f"   Found {len(companies)} from Glassdoor")
            
        except Exception as e:
            logger.error(f"   Error fetching Glassdoor: {e}")
        
        self.results[source] = companies
        return companies
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def _clean_company_name(self, name: str) -> str:
        """Clean and normalize a company name."""
        if not name:
            return ""
        
        # Remove common suffixes
        suffixes = [
            ', Inc.', ', Inc', ' Inc.', ' Inc', ', LLC', ' LLC',
            ', Ltd.', ', Ltd', ' Ltd.', ' Ltd', ', Corp.', ', Corp',
            ' Corp.', ' Corp', ' Corporation', ', L.P.', ' L.P.',
            ' Company', ' Co.', ' Co', '®', '™', ' PLC', ' plc'
        ]
        
        cleaned = name.strip()
        for suffix in suffixes:
            if cleaned.endswith(suffix):
                cleaned = cleaned[:-len(suffix)].strip()
        
        # Remove special characters but keep spaces and basic punctuation
        cleaned = re.sub(r'[^\w\s\-\.\&]', '', cleaned)
        
        # Normalize whitespace
        cleaned = ' '.join(cleaned.split())
        
        return cleaned
    
    def _is_valid_company_name(self, name: str) -> bool:
        """Check if a string looks like a valid company name."""
        if not name or len(name) < 2 or len(name) > 100:
            return False
        
        # Must contain at least one letter
        if not any(c.isalpha() for c in name):
            return False
        
        # Skip common non-company strings
        skip_patterns = [
            r'^(the|a|an)\s*$',
            r'^https?://',
            r'^www\.',
            r'@',
            r'^#',
            r'^\d+$',
            r'^readme',
            r'^license',
            r'^contributing',
        ]
        
        name_lower = name.lower()
        for pattern in skip_patterns:
            if re.match(pattern, name_lower):
                return False
        
        return True
    
    def _is_generic_word(self, name: str) -> bool:
        """Check if name is a generic/common word."""
        generic_words = {
            'about', 'the', 'and', 'for', 'with', 'this', 'that', 'from',
            'have', 'will', 'can', 'are', 'was', 'were', 'been', 'being',
            'more', 'most', 'some', 'any', 'each', 'every', 'both', 'few',
            'other', 'such', 'only', 'same', 'than', 'too', 'very', 'just',
            'should', 'now', 'here', 'there', 'when', 'where', 'why', 'how',
            'all', 'if', 'or', 'because', 'as', 'until', 'while', 'of',
            'at', 'by', 'about', 'against', 'between', 'into', 'through',
            'during', 'before', 'after', 'above', 'below', 'to', 'in', 'on',
            'off', 'over', 'under', 'again', 'further', 'then', 'once',
            'awesome', 'list', 'tool', 'tools', 'app', 'apps', 'free',
            'open', 'source', 'code', 'data', 'file', 'files', 'web',
            'resources', 'resource', 'guide', 'tutorial', 'example'
        }
        
        return name.lower().strip() in generic_words
    
    # ========================================================================
    # EXPANSION ORCHESTRATION
    # ========================================================================
    
    async def expand_tier1(self) -> Dict[str, List[str]]:
        """Expand from all Tier 1 sources (high hit rate)."""
        logger.info("\n" + "="*60)
        logger.info("🎯 TIER 1 EXPANSION - High Hit Rate Sources")
        logger.info("="*60 + "\n")
        
        results = {}
        
        # Run all Tier 1 fetches
        results['yc'] = await self.fetch_yc_companies()
        results['github_orgs'] = await self.fetch_github_organizations()
        results['producthunt'] = await self.fetch_producthunt()
        results['github_awesome'] = await self.fetch_github_awesome_lists()
        results['crunchbase'] = await self.fetch_crunchbase_free()
        
        # Save to database with Tier 1 priority
        for source, companies in results.items():
            if companies:
                config = SOURCES.get(source, SourceConfig(source, tier=1, priority=80))
                self.db.save_seed_companies(companies, source, tier=1, priority=config.priority)
        
        total = sum(len(c) for c in results.values())
        logger.info(f"\n✅ Tier 1 Complete: {total} companies from {len(results)} sources")
        
        return results
    
    async def expand_tier2(self) -> Dict[str, List[str]]:
        """Expand from all Tier 2 sources (medium hit rate)."""
        logger.info("\n" + "="*60)
        logger.info("🎯 TIER 2 EXPANSION - Medium Hit Rate Sources")
        logger.info("="*60 + "\n")
        
        results = {}
        
        # Run all Tier 2 fetches
        results['sec_edgar'] = await self.fetch_sec_edgar()
        results['usaspending'] = await self.fetch_usaspending()
        results['sam_gov'] = await self.fetch_sam_gov()
        results['inc5000'] = await self.fetch_inc5000()
        results['fortune500'] = await self.fetch_fortune500()
        results['glassdoor'] = await self.fetch_glassdoor()
        
        # Save to database with Tier 2 priority
        for source, companies in results.items():
            if companies:
                config = SOURCES.get(source, SourceConfig(source, tier=2, priority=50))
                self.db.save_seed_companies(companies, source, tier=2, priority=config.priority)
        
        total = sum(len(c) for c in results.values())
        logger.info(f"\n✅ Tier 2 Complete: {total} companies from {len(results)} sources")
        
        return results
    
    async def expand_all(self) -> Dict[str, List[str]]:
        """Expand from all sources (Tier 1 and Tier 2)."""
        logger.info("\n" + "="*60)
        logger.info("🚀 FULL EXPANSION - All Tiers")
        logger.info("="*60 + "\n")
        
        all_results = {}
        
        # Tier 1 first (higher priority)
        tier1_results = await self.expand_tier1()
        all_results.update(tier1_results)
        
        # Then Tier 2
        tier2_results = await self.expand_tier2()
        all_results.update(tier2_results)
        
        # Calculate totals
        all_companies = set()
        for companies in all_results.values():
            for company in companies:
                all_companies.add(company.lower())
        
        all_results['total_unique'] = list(all_companies)
        
        logger.info("\n" + "="*60)
        logger.info("📊 EXPANSION SUMMARY")
        logger.info("="*60)
        logger.info(f"\nTier 1 Sources:")
        for source in ['yc', 'github_orgs', 'producthunt', 'github_awesome', 'crunchbase']:
            count = len(all_results.get(source, []))
            logger.info(f"  • {source}: {count} companies")
        
        logger.info(f"\nTier 2 Sources:")
        for source in ['sec_edgar', 'usaspending', 'sam_gov', 'inc5000', 'fortune500', 'glassdoor']:
            count = len(all_results.get(source, []))
            logger.info(f"  • {source}: {count} companies")
        
        logger.info(f"\n{'─'*40}")
        logger.info(f"Total Unique Companies: {len(all_companies)}")
        logger.info("="*60 + "\n")
        
        return all_results
    
    def get_source_stats(self) -> List[Dict]:
        """Get statistics for all sources."""
        return self.db.get_source_stats()
    
    def print_source_stats(self):
        """Print a formatted report of source statistics."""
        stats = self.get_source_stats()
        
        if not stats:
            logger.info("No source statistics available yet.")
            return
        
        logger.info("\n" + "="*70)
        logger.info("📊 SOURCE PERFORMANCE REPORT")
        logger.info("="*70)
        logger.info(f"{'Source':<20} {'Tier':<5} {'Tested':<8} {'Found':<7} {'Hit Rate':<10} {'Status'}")
        logger.info("─"*70)
        
        for s in stats:
            status = "✅" if s['enabled'] else "❌"
            hit_rate = f"{s['hit_rate']*100:.1f}%" if s['hit_rate'] else "N/A"
            logger.info(
                f"{s['source']:<20} {s['tier']:<5} {s['seeds_tested']:<8} "
                f"{s['seeds_found']:<7} {hit_rate:<10} {status}"
            )
        
        logger.info("="*70 + "\n")


# ========================================================================
# MAIN ENTRY POINTS
# ========================================================================

async def run_tier1_expansion() -> Dict[str, List[str]]:
    """Run Tier 1 expansion only."""
    expander = SeedExpander()
    try:
        results = await expander.expand_tier1()
        return results
    finally:
        await expander.close()


async def run_tier2_expansion() -> Dict[str, List[str]]:
    """Run Tier 2 expansion only."""
    expander = SeedExpander()
    try:
        results = await expander.expand_tier2()
        return results
    finally:
        await expander.close()


async def run_full_expansion() -> Dict[str, List[str]]:
    """Run full expansion (all tiers)."""
    expander = SeedExpander()
    try:
        results = await expander.expand_all()
        return results
    finally:
        await expander.close()


async def main():
    """Run the seed expander."""
    expander = SeedExpander()
    
    try:
        # Run full expansion
        results = await expander.expand_all()
        
        # Print source stats
        expander.print_source_stats()
        
        # Get total seeds available
        stats = expander.db.get_stats()
        print(f"\n✅ Total seeds in database: {stats.get('total_seeds', 0)}")
        print(f"   Seeds tested: {stats.get('seeds_tested', 0)}")
        
    finally:
        await expander.close()


if __name__ == "__main__":
    asyncio.run(main())
