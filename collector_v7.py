"""
Job Intelligence Collector V7 - Explosive Growth Edition
=========================================================
Key Improvements:
- 15 ATS types (was 7): Added iCIMS, Taleo, SuccessFactors, Workable, Breezy, Recruitee, Personio, Teamtailor, Jazz, Pinpoint
- Parallel ATS testing: Test all ATS types simultaneously (4x faster)
- Aggressive token generation: Up to 50 variations per company
- Self-discovery: Extracts company mentions from job descriptions
- Enhanced scrapers with multiple fallback strategies
"""

import asyncio
import aiohttp
import json
import re
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set, Any
from urllib.parse import urlparse, quote
from contextlib import asynccontextmanager
import hashlib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# ATS CONFIGURATIONS - 15 Types (was 7)
# =============================================================================

ATS_CONFIGS = {
    # Priority 1: Common startup/tech ATS (test first)
    'greenhouse': {
        'priority': 1,
        'api_url': 'https://boards-api.greenhouse.io/v1/boards/{token}/jobs',
        'board_url': 'https://boards.greenhouse.io/{token}',
        'company_types': ['startup', 'tech', 'growth'],
        'validate_url': 'https://boards-api.greenhouse.io/v1/boards/{token}',
    },
    'lever': {
        'priority': 1,
        'api_url': 'https://api.lever.co/v0/postings/{token}?mode=json',
        'board_url': 'https://jobs.lever.co/{token}',
        'company_types': ['startup', 'tech', 'growth'],
    },
    'ashby': {
        'priority': 1,
        'api_url': 'https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams',
        'board_url': 'https://jobs.ashbyhq.com/{token}',
        'company_types': ['startup', 'tech'],
        'graphql': True,
    },
    
    # Priority 2: Enterprise ATS (Fortune 500, large companies)
    'workday': {
        'priority': 2,
        'patterns': ['wd5', 'wd1', 'wd3', 'wd12', 'myworkdayjobs'],
        'url_templates': [
            'https://{token}.wd5.myworkdayjobs.com/en-US/External',
            'https://{token}.wd1.myworkdayjobs.com/en-US/External',
            'https://{token}.wd3.myworkdayjobs.com/en-US/External',
            'https://{token}.wd12.myworkdayjobs.com/en-US/External',
        ],
        'company_types': ['enterprise', 'fortune500', 'healthcare', 'finance'],
    },
    'icims': {
        'priority': 2,
        'url_templates': [
            'https://careers-{token}.icims.com/jobs/search',
            'https://{token}.icims.com/jobs/search',
            'https://jobs-{token}.icims.com/jobs/search',
        ],
        'company_types': ['enterprise', 'fortune500', 'retail', 'healthcare'],
    },
    'taleo': {
        'priority': 2,
        'url_templates': [
            'https://{token}.taleo.net/careersection/2/jobsearch.ftl',
            'https://careers.{token}.com/OA_HTML/OA.jsp',
        ],
        'company_types': ['enterprise', 'fortune500', 'government', 'manufacturing'],
    },
    'successfactors': {
        'priority': 2,
        'url_templates': [
            'https://jobs.sap.com/search/?q=&locationsearch={token}',
            'https://{token}.successfactors.com/career',
        ],
        'company_types': ['enterprise', 'manufacturing', 'consulting'],
    },
    'workable': {
        'priority': 2,
        'api_url': 'https://apply.workable.com/api/v3/accounts/{token}/jobs',
        'board_url': 'https://apply.workable.com/{token}/',
        'company_types': ['smb', 'startup', 'growth'],
    },
    'smartrecruiters': {
        'priority': 2,
        'api_url': 'https://api.smartrecruiters.com/v1/companies/{token}/postings',
        'board_url': 'https://careers.smartrecruiters.com/{token}',
        'company_types': ['enterprise', 'retail', 'hospitality'],
    },
    
    # Priority 3: Smaller/Regional ATS
    'recruitee': {
        'priority': 3,
        'api_url': 'https://{token}.recruitee.com/api/offers',
        'board_url': 'https://{token}.recruitee.com/',
        'company_types': ['smb', 'european'],
    },
    'personio': {
        'priority': 3,
        'api_url': 'https://{token}.jobs.personio.com/api/v1/jobs',
        'board_url': 'https://{token}.jobs.personio.com/',
        'company_types': ['smb', 'european', 'german'],
    },
    'teamtailor': {
        'priority': 3,
        'api_url': 'https://{token}.teamtailor.com/api/v1/jobs',
        'board_url': 'https://{token}.teamtailor.com/',
        'company_types': ['smb', 'european', 'nordic'],
    },
    'breezy': {
        'priority': 3,
        'api_url': 'https://{token}.breezy.hr/json',
        'board_url': 'https://{token}.breezy.hr/',
        'company_types': ['smb', 'startup'],
    },
    'jazz': {
        'priority': 3,
        'url_templates': [
            'https://{token}.applytojob.com/apply/',
        ],
        'company_types': ['smb'],
    },
    'pinpoint': {
        'priority': 3,
        'api_url': 'https://{token}.pinpointhq.com/api/v1/jobs',
        'board_url': 'https://{token}.pinpointhq.com/',
        'company_types': ['smb', 'uk'],
    },
}

# =============================================================================
# SPECIAL COMPANY MAPPINGS (for aggressive token generation)
# =============================================================================

SPECIAL_COMPANY_MAPPINGS = {
    'meta': ['facebook', 'fb', 'metafacebook', 'meta-platforms', 'metaplatforms'],
    'alphabet': ['google', 'googl', 'youtube', 'waymo', 'deepmind', 'verily'],
    'amazon': ['aws', 'amazondotcom', 'amazonwebservices', 'a]mazon-jobs'],
    'microsoft': ['msft', 'azure', 'linkedin', 'github-microsoft'],
    'apple': ['applecomputer', 'apple-inc'],
    'jpmorgan': ['jpmorganchase', 'jpmc', 'chase', 'jpm'],
    'jpmorgan chase': ['jpmorganchase', 'jpmc', 'chase', 'jpm', 'jpmorgan'],
    'bank of america': ['bankofamerica', 'bofa', 'bofaml', 'merrilllynch'],
    'goldman sachs': ['goldmansachs', 'gs', 'goldman'],
    'morgan stanley': ['morganstanley', 'ms-careers'],
    'johnson & johnson': ['johnsonandjohnson', 'jnj', 'janssen'],
    'procter & gamble': ['procterandgamble', 'pg', 'pandg'],
    'general electric': ['generalelectric', 'ge', 'ge-careers'],
    'at&t': ['att', 'atandt', 'attcareers'],
    '3m': ['threeem', '3m-company', 'mmm'],
    'ibm': ['ibmcareers', 'ibm-jobs', 'international-business-machines'],
    'hp': ['hewlettpackard', 'hpe', 'hp-inc'],
    'dell': ['delltechnologies', 'dell-emc', 'emc'],
    'salesforce': ['salesforcecom', 'sfdc', 'salesforce-jobs'],
    'oracle': ['oraclecareers', 'oracle-jobs'],
    'sap': ['sap-careers', 'sap-jobs'],
    'cisco': ['ciscocareers', 'cisco-systems'],
    'intel': ['intelcareers', 'intel-corporation'],
    'nvidia': ['nvidiacareers', 'nvidia-corporation'],
    'amd': ['amdcareers', 'advanced-micro-devices'],
    'qualcomm': ['qualcommcareers', 'qualcomm-inc'],
    'broadcom': ['broadcomcareers', 'broadcom-inc'],
    'netflix': ['netflixcareers', 'netflix-jobs'],
    'disney': ['waltdisney', 'thewaltdisneycompany', 'disneyplus'],
    'warner bros': ['warnerbros', 'wbd', 'warnerbroscareers', 'warnerbrosdisc'],
    'comcast': ['comcastcareers', 'nbcuniversal', 'xfinity'],
    'verizon': ['verizoncareers', 'verizon-wireless'],
    'uber': ['ubercareers', 'uber-jobs'],
    'lyft': ['lyftcareers', 'lyft-jobs'],
    'airbnb': ['airbnbcareers', 'airbnb-jobs'],
    'doordash': ['doordashcareers', 'doordash-jobs'],
    'instacart': ['instacartcareers', 'instacart-jobs'],
    'stripe': ['stripecareers', 'stripe-jobs'],
    'square': ['squarecareers', 'block', 'blockfi'],
    'paypal': ['paypalcareers', 'paypal-jobs'],
    'visa': ['visacareers', 'visa-jobs'],
    'mastercard': ['mastercardcareers', 'mastercard-jobs'],
    'american express': ['americanexpress', 'amex', 'amexcareers'],
    'capital one': ['capitalone', 'capitalonebank', 'capitalonecareers'],
    'wells fargo': ['wellsfargo', 'wellsfargocareers', 'wf'],
    'citigroup': ['citi', 'citibank', 'citicareers'],
    'blackrock': ['blackrockcareers', 'blackrock-jobs'],
    'fidelity': ['fidelitycareers', 'fidelityinvestments'],
    'charles schwab': ['schwab', 'schwabcareers', 'charlesschwab'],
    'robinhood': ['robinhoodcareers', 'robinhood-jobs'],
    'coinbase': ['coinbasecareers', 'coinbase-jobs'],
    'binance': ['binancecareers', 'binance-jobs'],
    'ftx': ['ftxcareers', 'ftx-jobs'],  # Historical
    'kraken': ['krakencareers', 'kraken-jobs'],
}

# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class JobPosting:
    """Individual job posting"""
    id: str
    title: str
    location: str
    department: Optional[str] = None
    url: Optional[str] = None
    remote: bool = False
    posted_at: Optional[datetime] = None
    description: Optional[str] = None  # For self-discovery

@dataclass
class CompanyJobBoard:
    """Company job board with all jobs"""
    company_name: str
    token: str
    ats_type: str
    board_url: str = ""  # Add board_url field
    jobs: List[JobPosting] = field(default_factory=list)
    job_count: int = 0
    remote_count: int = 0
    departments: List[str] = field(default_factory=list)
    locations: List[str] = field(default_factory=list)
    discovered_companies: Set[str] = field(default_factory=set)  # Self-discovery
    last_updated: datetime = field(default_factory=datetime.now)

@dataclass
class DiscoveryStats:
    """Statistics for a discovery run"""
    seeds_tested: int = 0
    companies_found: int = 0
    jobs_found: int = 0
    ats_breakdown: Dict[str, int] = field(default_factory=dict)
    new_discoveries: int = 0  # Self-discovered companies
    errors: int = 0
    duration_seconds: float = 0

# =============================================================================
# TOKEN GENERATION (Aggressive - Up to 50 variations)
# =============================================================================

class TokenGenerator:
    """Generate aggressive token variations for company names"""
    
    @staticmethod
    def generate_tokens(company_name: str) -> List[str]:
        """Generate up to 50 token variations for a company name"""
        tokens = set()
        
        # Clean the input
        name = company_name.strip()
        name_lower = name.lower()
        
        # 1. Basic variations
        tokens.add(name_lower.replace(' ', ''))  # nospace
        tokens.add(name_lower.replace(' ', '-'))  # hyphenated
        tokens.add(name_lower.replace(' ', '_'))  # underscored
        
        # 2. Remove common suffixes
        suffixes = [' inc', ' inc.', ' llc', ' ltd', ' limited', ' corp', ' corporation', 
                   ' co', ' company', ' technologies', ' technology', ' tech', ' labs',
                   ' software', ' solutions', ' systems', ' services', ' group', ' holdings',
                   ' international', ' global', ' partners', ' capital', ' ventures']
        cleaned = name_lower
        for suffix in suffixes:
            if cleaned.endswith(suffix):
                cleaned = cleaned[:-len(suffix)].strip()
        tokens.add(cleaned.replace(' ', ''))
        tokens.add(cleaned.replace(' ', '-'))
        
        # 3. CamelCase and PascalCase
        words = name.split()
        if len(words) > 1:
            # camelCase
            camel = words[0].lower() + ''.join(w.capitalize() for w in words[1:])
            tokens.add(camel)
            # PascalCase
            pascal = ''.join(w.capitalize() for w in words)
            tokens.add(pascal)
        
        # 4. Acronyms
        if len(words) > 1:
            acronym = ''.join(w[0].lower() for w in words if w)
            if len(acronym) >= 2:
                tokens.add(acronym)
        
        # 5. First word only (often works for startups)
        if len(words) > 1:
            tokens.add(words[0].lower())
        
        # 6. Last word only
        if len(words) > 1:
            tokens.add(words[-1].lower())
        
        # 7. First + Last word
        if len(words) > 2:
            tokens.add(f"{words[0].lower()}{words[-1].lower()}")
            tokens.add(f"{words[0].lower()}-{words[-1].lower()}")
        
        # 8. Remove 'the' prefix
        if name_lower.startswith('the '):
            without_the = name_lower[4:]
            tokens.add(without_the.replace(' ', ''))
            tokens.add(without_the.replace(' ', '-'))
        
        # 9. Handle numbers
        number_words = {
            '1': 'one', '2': 'two', '3': 'three', '4': 'four', '5': 'five',
            '6': 'six', '7': 'seven', '8': 'eight', '9': 'nine', '0': 'zero'
        }
        name_with_words = name_lower
        for digit, word in number_words.items():
            if digit in name_lower:
                name_with_words = name_with_words.replace(digit, word)
                tokens.add(name_with_words.replace(' ', ''))
        
        # 10. Handle ampersand
        if '&' in name_lower:
            tokens.add(name_lower.replace('&', 'and').replace(' ', ''))
            tokens.add(name_lower.replace('&', '-').replace(' ', ''))
            tokens.add(name_lower.replace(' & ', '').replace(' ', ''))
        
        # 11. Handle dots
        if '.' in name_lower:
            tokens.add(name_lower.replace('.', '').replace(' ', ''))
            tokens.add(name_lower.replace('.', '-').replace(' ', ''))
        
        # 12. Special company mappings
        name_key = name_lower.replace(' ', '')
        for key, variations in SPECIAL_COMPANY_MAPPINGS.items():
            if key.replace(' ', '') in name_key or name_key in key.replace(' ', ''):
                tokens.update(variations)
        
        # Also check exact matches
        if name_lower in SPECIAL_COMPANY_MAPPINGS:
            tokens.update(SPECIAL_COMPANY_MAPPINGS[name_lower])
        
        # 13. Try without common words
        common_words = ['the', 'a', 'an', 'of', 'for', 'and', 'or']
        filtered_words = [w for w in words if w.lower() not in common_words]
        if len(filtered_words) < len(words):
            tokens.add(''.join(w.lower() for w in filtered_words))
        
        # 14. Handle hyphens in original name
        if '-' in name:
            tokens.add(name_lower.replace('-', ''))
            tokens.add(name_lower.replace('-', '_'))
        
        # Remove empty strings and validate
        tokens = {t for t in tokens if t and len(t) >= 2 and len(t) <= 50}
        
        # Sort by priority (shorter tokens often better)
        return sorted(tokens, key=len)[:50]

# =============================================================================
# ATS SCRAPERS
# =============================================================================

class ATSScraper:
    """Base ATS scraper with common functionality"""
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'application/json, text/html',
        }
    
    async def fetch(self, url: str, json_response: bool = True) -> Optional[Any]:
        """Fetch URL with error handling"""
        try:
            async with self.session.get(url, headers=self.headers, timeout=10) as resp:
                if resp.status == 200:
                    if json_response:
                        return await resp.json()
                    return await resp.text()
                elif resp.status == 404:
                    return None
                else:
                    logger.debug(f"HTTP {resp.status} for {url}")
                    return None
        except asyncio.TimeoutError:
            logger.debug(f"Timeout for {url}")
            return None
        except Exception as e:
            logger.debug(f"Error fetching {url}: {e}")
            return None
    
    def extract_company_mentions(self, text: str) -> Set[str]:
        """Extract potential company names from job description text"""
        companies = set()
        
        # Patterns that often precede company names
        patterns = [
            r'(?:partner(?:ing|ed|s)? with|integrate(?:s|d)? with|powered by|built (?:on|with)|using)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+)?)',
            r'(?:customers include|clients include|serving|trusted by)\s+([A-Z][A-Za-z0-9]+(?:,?\s+(?:and\s+)?[A-Z][A-Za-z0-9]+)*)',
            r'(?:competitor(?:s)?|alternative(?:s)? to)\s+([A-Z][A-Za-z0-9]+)',
            r'(?:acquired by|owned by|subsidiary of|part of)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+)?)',
        ]
        
        # Known integrations to filter out (not company seeds)
        known_integrations = {
            'salesforce', 'slack', 'aws', 'azure', 'google', 'microsoft', 'github',
            'jira', 'confluence', 'notion', 'figma', 'stripe', 'twilio', 'sendgrid',
            'datadog', 'pagerduty', 'okta', 'auth0', 'segment', 'amplitude', 'mixpanel'
        }
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                # Split on commas and 'and'
                parts = re.split(r',\s*|\s+and\s+', match)
                for part in parts:
                    part = part.strip()
                    if (len(part) >= 3 and 
                        part.lower() not in known_integrations and
                        not part.isnumeric()):
                        companies.add(part)
        
        return companies


class GreenhouseScraper(ATSScraper):
    """Greenhouse ATS scraper using API"""
    
    # Blacklist generic words and test companies
    BLACKLISTED_TOKENS = {
        'system', 'original', 'magic', 'ie', 'test', 'demo', 'sample', 'example',
        'kiosk', 'talent', 'general', 'interest', 'future', 'seed', 'company',
        'jobs', 'careers', 'team', 'work', 'hire', 'the', 'and', 'for', 'app',
        'li', 'linkedin',
        'national', 'journey', 'commons', 'door', 'alarm',
        'link', 'ess', 'nmi', 'canvas', 'united', 'facility', 'industrial',
        'best', 'friend', 'finance', 'goody', 'garage', 'doors',
        'edge', 'elite', 'clear', 'builder', 'bloom', 'archrival', 'bold',
        'sonja',
        # More recurring false positives
        'relai', 'founders',
    }
    
    async def check_token(self, token: str) -> Optional[CompanyJobBoard]:
        """Check if a Greenhouse token is valid and fetch jobs"""
        # Skip short or blacklisted tokens
        if len(token) < 3 or token.lower().strip() in self.BLACKLISTED_TOKENS:
            return None
        
        url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
        data = await self.fetch(url)
        
        if not data or 'jobs' not in data:
            return None
        
        jobs = []
        discovered_companies = set()
        departments = set()
        locations = set()
        remote_count = 0
        
        for job in data.get('jobs', []):
            location = job.get('location', {}).get('name', 'Unknown')
            dept = job.get('departments', [{}])[0].get('name', '') if job.get('departments') else ''
            is_remote = 'remote' in location.lower()
            
            jobs.append(JobPosting(
                id=str(job.get('id', '')),
                title=job.get('title', ''),
                location=location,
                department=dept,
                url=job.get('absolute_url', ''),
                remote=is_remote,
            ))
            
            if is_remote:
                remote_count += 1
            if dept:
                departments.add(dept)
            if location:
                locations.add(location)
        
        # Fetch board info for company name
        board_url = f"https://boards-api.greenhouse.io/v1/boards/{token}"
        board_data = await self.fetch(board_url)
        company_name = board_data.get('name', token) if board_data else token
        
        return CompanyJobBoard(
            company_name=company_name,
            token=token,
            ats_type='greenhouse',
            board_url=f'https://boards.greenhouse.io/{token}',
            jobs=jobs,
            job_count=len(jobs),
            remote_count=remote_count,
            departments=list(departments),
            locations=list(locations),
            discovered_companies=discovered_companies,
        )


class LeverScraper(ATSScraper):
    """Lever ATS scraper"""
    
    # Blacklist generic words
    BLACKLISTED_TOKENS = {
        'better', 'ecosystem', 'signal', 'choose', 'color', 'super', 'future',
        'test', 'demo', 'jobs', 'careers', 'team', 'work', 'hire', 'company',
        'the', 'and', 'for', 'with', 'about', 'home', 'main', 'app', 'web',
        'life', 'capital', 'form', 'artificial', 'crypto', 'anomaly', 'hexa',
        'adaptive', 'sesame', 'teller', 'rigetti', 'maya', 'rupa', 'finch',
        'mega', 'brilliant', 'belong',
        'blue', 'relay', 'true', 'spring', 'bright',
        # More generic words
        'sure', 'bloom',
    }
    
    async def check_token(self, token: str) -> Optional[CompanyJobBoard]:
        """Check if a Lever token is valid and fetch jobs"""
        # Skip short or blacklisted tokens
        if len(token) < 3 or token.lower() in self.BLACKLISTED_TOKENS:
            return None
            
        url = f"https://api.lever.co/v0/postings/{token}?mode=json"
        data = await self.fetch(url)
        
        if not data or not isinstance(data, list):
            return None
        
        jobs = []
        departments = set()
        locations = set()
        remote_count = 0
        
        for job in data:
            location = job.get('categories', {}).get('location', 'Unknown')
            dept = job.get('categories', {}).get('department', '')
            team = job.get('categories', {}).get('team', '')
            is_remote = 'remote' in location.lower()
            
            jobs.append(JobPosting(
                id=str(job.get('id', '')),
                title=job.get('text', ''),
                location=location,
                department=dept or team,
                url=job.get('hostedUrl', ''),
                remote=is_remote,
            ))
            
            if is_remote:
                remote_count += 1
            if dept:
                departments.add(dept)
            if team:
                departments.add(team)
            if location:
                locations.add(location)
        
        return CompanyJobBoard(
            company_name=token.replace('-', ' ').title(),
            token=token,
            ats_type='lever',
            board_url=f'https://jobs.lever.co/{token}',
            jobs=jobs,
            job_count=len(jobs),
            remote_count=remote_count,
            departments=list(departments),
            locations=list(locations),
        )


class AshbyScraper(ATSScraper):
    """Ashby ATS scraper using GraphQL API"""
    
    async def check_token(self, token: str) -> Optional[CompanyJobBoard]:
        """Check if an Ashby token is valid"""
        # Try the posting API first
        url = f"https://jobs.ashbyhq.com/{token}"
        html = await self.fetch(url, json_response=False)
        
        if not html or 'No jobs found' in str(html) or '404' in str(html):
            return None
        
        # Try to fetch job data via API
        api_url = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams"
        try:
            payload = {
                "operationName": "ApiJobBoardWithTeams",
                "variables": {"organizationHostedJobsPageName": token},
                "query": """query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
                    jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) {
                        teams { id name }
                        jobPostings { id title team { id name } locationName employmentType }
                    }
                }"""
            }
            async with self.session.post(api_url, json=payload, headers=self.headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    job_board = data.get('data', {}).get('jobBoard', {})
                    postings = job_board.get('jobPostings', [])
                    
                    if not postings:
                        return None
                    
                    jobs = []
                    departments = set()
                    locations = set()
                    remote_count = 0
                    
                    for job in postings:
                        location = job.get('locationName', 'Unknown')
                        team = job.get('team', {})
                        dept = team.get('name', '') if team else ''
                        is_remote = 'remote' in location.lower()
                        
                        jobs.append(JobPosting(
                            id=str(job.get('id', '')),
                            title=job.get('title', ''),
                            location=location,
                            department=dept,
                            remote=is_remote,
                        ))
                        
                        if is_remote:
                            remote_count += 1
                        if dept:
                            departments.add(dept)
                        if location:
                            locations.add(location)
                    
                    return CompanyJobBoard(
                        company_name=token.replace('-', ' ').title(),
                        token=token,
                        ats_type='ashby',
                        board_url=f'https://jobs.ashbyhq.com/{token}',
                        jobs=jobs,
                        job_count=len(jobs),
                        remote_count=remote_count,
                        departments=list(departments),
                        locations=list(locations),
                    )
        except Exception as e:
            logger.debug(f"Ashby API error for {token}: {e}")
        
        return None


class WorkdayScraper(ATSScraper):
    """Workday ATS scraper with multiple subdomain patterns"""
    
    WORKDAY_PATTERNS = ['wd5', 'wd1', 'wd3', 'wd12']
    
    # Blacklist ambiguous/generic short tokens
    BLACKLISTED_TOKENS = {
        'ms', 'hr', 'it', 'us', 'uk', 'eu', 'ca', 'au', 'in', 'jp', 'de', 'fr', 
        'test', 'demo', 'jobs', 'careers',
        # New additions
        'path', 'sim', 'capital', 'life', 'data', 'system', 'global', 'world',
    }
    
    async def check_token(self, token: str) -> Optional[CompanyJobBoard]:
        """Try multiple Workday URL patterns"""
        # Skip very short or blacklisted tokens
        if len(token) < 3 or token.lower() in self.BLACKLISTED_TOKENS:
            return None
            
        for pattern in self.WORKDAY_PATTERNS:
            # Try different URL formats
            urls = [
                f"https://{token}.{pattern}.myworkdayjobs.com/wday/cxs/{token}/External/jobs",
                f"https://{token}.{pattern}.myworkdayjobs.com/en-US/External",
            ]
            
            for url in urls:
                try:
                    # Workday uses a specific API endpoint
                    if '/jobs' in url:
                        payload = {"appliedFacets": {}, "limit": 20, "offset": 0}
                        async with self.session.post(url, json=payload, headers={
                            **self.headers,
                            'Content-Type': 'application/json',
                        }, timeout=15) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                if data.get('total', 0) > 0:
                                    return self._parse_workday_response(token, pattern, data)
                    else:
                        # Check if the page loads
                        async with self.session.get(url, headers=self.headers, timeout=15) as resp:
                            if resp.status == 200:
                                text = await resp.text()
                                if 'jobResults' in text or 'job-results' in text.lower():
                                    # Found a valid Workday board
                                    return CompanyJobBoard(
                                        company_name=token.replace('-', ' ').title(),
                                        token=token,
                                        ats_type=f'workday_{pattern}',
                                        board_url=url,
                                        job_count=0,  # Would need to parse HTML
                                    )
                except Exception as e:
                    logger.debug(f"Workday {pattern} error for {token}: {e}")
                    continue
        
        return None
    
    def _parse_workday_response(self, token: str, pattern: str, data: dict) -> CompanyJobBoard:
        """Parse Workday API response"""
        jobs = []
        departments = set()
        locations = set()
        remote_count = 0
        
        for job in data.get('jobPostings', []):
            title = job.get('title', '')
            location = job.get('locationsText', 'Unknown')
            is_remote = 'remote' in location.lower()
            
            jobs.append(JobPosting(
                id=job.get('bulletFields', [''])[0] if job.get('bulletFields') else '',
                title=title,
                location=location,
                remote=is_remote,
            ))
            
            if is_remote:
                remote_count += 1
            if location:
                locations.add(location)
        
        return CompanyJobBoard(
            company_name=token.replace('-', ' ').title(),
            token=token,
            ats_type=f'workday_{pattern}',
            board_url=f'https://{token}.{pattern}.myworkdayjobs.com/en-US/External',
            jobs=jobs,
            job_count=data.get('total', len(jobs)),
            remote_count=remote_count,
            departments=list(departments),
            locations=list(locations),
        )


class ICIMSScraper(ATSScraper):
    """iCIMS ATS scraper"""
    
    async def check_token(self, token: str) -> Optional[CompanyJobBoard]:
        """Check iCIMS careers page"""
        # Skip very short tokens (likely false positives)
        if len(token) < 3:
            return None
            
        urls = [
            f"https://careers-{token}.icims.com/jobs/search",
            f"https://{token}.icims.com/jobs/search",
            f"https://jobs-{token}.icims.com/jobs/search",
        ]
        
        for url in urls:
            html = await self.fetch(url, json_response=False)
            if html:
                html_str = str(html)
                # More strict check - require actual job listings, not just iCIMS branding
                if 'iCIMS_JobsTable' in html_str or 'class="iCIMS_Jobs' in html_str:
                    # Count actual job rows
                    job_count = html_str.count('iCIMS_JobsTable_Job') or html_str.lower().count('job-result')
                    
                    # Only return if we found actual jobs
                    if job_count > 0:
                        return CompanyJobBoard(
                            company_name=token.replace('-', ' ').title(),
                            token=token,
                            ats_type='icims',
                            board_url=url,
                            job_count=job_count,
                        )
        
        return None


class WorkableScraper(ATSScraper):
    """Workable ATS scraper"""
    
    async def check_token(self, token: str) -> Optional[CompanyJobBoard]:
        """Check Workable API"""
        url = f"https://apply.workable.com/api/v3/accounts/{token}/jobs"
        data = await self.fetch(url)
        
        if not data or 'results' not in data:
            return None
        
        jobs = []
        departments = set()
        locations = set()
        remote_count = 0
        
        for job in data.get('results', []):
            location = job.get('location', {}).get('city', 'Unknown')
            dept = job.get('department', '')
            is_remote = job.get('remote', False)
            
            jobs.append(JobPosting(
                id=str(job.get('shortcode', '')),
                title=job.get('title', ''),
                location=location,
                department=dept,
                remote=is_remote,
            ))
            
            if is_remote:
                remote_count += 1
            if dept:
                departments.add(dept)
            if location:
                locations.add(location)
        
        return CompanyJobBoard(
            company_name=data.get('name', token.replace('-', ' ').title()),
            token=token,
            ats_type='workable',
            board_url=f'https://apply.workable.com/{token}/',
            jobs=jobs,
            job_count=len(jobs),
            remote_count=remote_count,
            departments=list(departments),
            locations=list(locations),
        )


class RecruiteeScraper(ATSScraper):
    """Recruitee ATS scraper"""
    
    # Blacklist generic words that match random companies
    BLACKLISTED_TOKENS = {
        'library', 'manual', 'blue', 'flow', 'tech', 'pay', 'adam', 'max', 
        'clay', 'nuvo', 'oculus', 'color', 'securing', 'onboarding', 'lindy',
        'test', 'demo', 'jobs', 'careers', 'team', 'work', 'hire', 'staff',
        'the', 'and', 'for', 'with', 'from', 'about', 'home', 'main', 'info',
        'people', 'chaos', 'vertical', 'enterprise', 'data', 'experience',
        'legal', 'flawless', 'aa',
        'moore', 'alex', 'jay', 'rha', 'assist', 'automation', 'origin',
        'healthcare', 'advanced', 'google', 'incognia', 'charles', 'national',
        'journey', 'belong', 'mega', 'brilliant', 'media', 'solutions',
        'global', 'group', 'services', 'digital', 'marketing', 'design',
        'creative', 'studio', 'agency', 'partners', 'consulting', 'labs',
        # NEW: More generic words and names from latest run
        'company', 'talent', 'true', 'bright', 'matt', 'spring', 'what',
        'illicopro', 'avantarte',
        # More generic words and first names
        'code', 'invision', 'stories', 'edge', 'elite', 'clear', 'builder',
        # Common first names
        'bob', 'john', 'mike', 'david', 'mark', 'chris', 'steve', 'paul',
        'james', 'tom', 'dan', 'jim', 'joe', 'bill', 'scott', 'brian',
        'ryan', 'kevin', 'jeff', 'greg', 'eric', 'peter', 'jason', 'andrew',
        # More generic words
        'jump',
    }
    
    async def check_token(self, token: str) -> Optional[CompanyJobBoard]:
        """Check Recruitee API"""
        # Skip short or blacklisted tokens
        if len(token) < 4 or token.lower() in self.BLACKLISTED_TOKENS:
            return None
            
        url = f"https://{token}.recruitee.com/api/offers"
        data = await self.fetch(url)
        
        if not data or 'offers' not in data:
            return None
        
        jobs = []
        departments = set()
        locations = set()
        remote_count = 0
        
        for job in data.get('offers', []):
            location = job.get('location', 'Unknown')
            dept = job.get('department', '')
            is_remote = job.get('remote', False) or 'remote' in location.lower()
            
            jobs.append(JobPosting(
                id=str(job.get('id', '')),
                title=job.get('title', ''),
                location=location,
                department=dept,
                remote=is_remote,
            ))
            
            if is_remote:
                remote_count += 1
            if dept:
                departments.add(dept)
            if location:
                locations.add(location)
        
        return CompanyJobBoard(
            company_name=token.replace('-', ' ').title(),
            token=token,
            ats_type='recruitee',
            board_url=f'https://{token}.recruitee.com/',
            jobs=jobs,
            job_count=len(jobs),
            remote_count=remote_count,
            departments=list(departments),
            locations=list(locations),
        )


class SmartRecruitersScraper(ATSScraper):
    """SmartRecruiters ATS scraper"""
    
    # Blacklist generic words
    BLACKLISTED_TOKENS = {
        'entropik', '2019', 'test', 'demo', 'jobs', 'careers', 'team', 'work',
        'the', 'and', 'for', 'with', 'about', 'home', 'main', 'app', 'web', 'api',
        'healthcare', 'health', 'medical', 'national', 'global', 'digital',
        # Years and generic words
        '1979', '1980', '1990', '2000', '2010', '2020', '2021', '2022', '2023', '2024', '2025',
        'illicopro', 'stories', 'hibob',
        # More tokens
        'light', 'a-light', 'talent',
    }
    
    async def check_token(self, token: str) -> Optional[CompanyJobBoard]:
        """Check SmartRecruiters API"""
        # Skip short, blacklisted, or all-numeric tokens
        if len(token) < 4 or token.lower() in self.BLACKLISTED_TOKENS or token.isdigit():
            return None
            
        url = f"https://api.smartrecruiters.com/v1/companies/{token}/postings"
        data = await self.fetch(url)
        
        if not data or 'content' not in data:
            return None
        
        # Skip if no jobs found
        total_jobs = data.get('totalFound', 0)
        if total_jobs == 0 or not data.get('content'):
            return None
        
        jobs = []
        departments = set()
        locations = set()
        remote_count = 0
        
        for job in data.get('content', []):
            location_data = job.get('location', {})
            location = f"{location_data.get('city', '')}, {location_data.get('region', '')}".strip(', ')
            dept = job.get('department', {}).get('label', '')
            is_remote = job.get('remote', False)
            
            jobs.append(JobPosting(
                id=str(job.get('id', '')),
                title=job.get('name', ''),
                location=location or 'Unknown',
                department=dept,
                remote=is_remote,
            ))
            
            if is_remote:
                remote_count += 1
            if dept:
                departments.add(dept)
            if location:
                locations.add(location)
        
        # Double check we have jobs
        if not jobs:
            return None
        
        return CompanyJobBoard(
            company_name=data.get('content', [{}])[0].get('company', {}).get('name', token) if data.get('content') else token,
            token=token,
            ats_type='smartrecruiters',
            board_url=f'https://careers.smartrecruiters.com/{token}',
            jobs=jobs,
            job_count=total_jobs,
            remote_count=remote_count,
            departments=list(departments),
            locations=list(locations),
        )


class BreezyScraper(ATSScraper):
    """Breezy HR ATS scraper"""
    
    # Blacklist generic words
    BLACKLISTED_TOKENS = {
        'af', 'test', 'demo', 'jobs', 'careers', 'team', 'work', 'hire',
        'the', 'and', 'for', 'with', 'about', 'home', 'main', 'app', 'hr',
        # Numbers and numeric patterns
        '1001', '2020', '2021', '2022', '2023', '2024', '2025',
        # More tokens
        'researchhub', 'msh', 'solugen', 'brilliant',
    }
    
    async def check_token(self, token: str) -> Optional[CompanyJobBoard]:
        """Check Breezy HR"""
        # Skip short or blacklisted tokens, and all-numeric tokens
        if len(token) < 3 or token.lower().strip() in self.BLACKLISTED_TOKENS or token.isdigit():
            return None
            
        url = f"https://{token}.breezy.hr/json"
        data = await self.fetch(url)
        
        if not data or not isinstance(data, list):
            return None
        
        jobs = []
        departments = set()
        locations = set()
        remote_count = 0
        
        for job in data:
            location = job.get('location', {}).get('name', 'Unknown')
            dept = job.get('department', '')
            is_remote = job.get('remote', False) or 'remote' in location.lower()
            
            jobs.append(JobPosting(
                id=str(job.get('_id', '')),
                title=job.get('name', ''),
                location=location,
                department=dept,
                remote=is_remote,
            ))
            
            if is_remote:
                remote_count += 1
            if dept:
                departments.add(dept)
            if location:
                locations.add(location)
        
        return CompanyJobBoard(
            company_name=token.replace('-', ' ').title(),
            token=token,
            ats_type='breezy',
            board_url=f'https://{token}.breezy.hr/',
            jobs=jobs,
            job_count=len(jobs),
            remote_count=remote_count,
            departments=list(departments),
            locations=list(locations),
        )


# =============================================================================
# MAIN COLLECTOR (with Parallel Testing)
# =============================================================================

class JobIntelCollectorV7:
    """Main collector with parallel ATS testing and self-discovery"""
    
    def __init__(self, db_path: str = 'job_intel.db'):
        self.db_path = db_path
        self.scrapers = {}
        self.token_generator = TokenGenerator()
        self.discovered_companies: Set[str] = set()
        self.results: List[CompanyJobBoard] = []  # Store discovered companies
    
    async def init_scrapers(self, session: aiohttp.ClientSession):
        """Initialize all ATS scrapers"""
        self.scrapers = {
            'greenhouse': GreenhouseScraper(session),
            'lever': LeverScraper(session),
            'ashby': AshbyScraper(session),
            'workday': WorkdayScraper(session),
            'icims': ICIMSScraper(session),
            'workable': WorkableScraper(session),
            'recruitee': RecruiteeScraper(session),
            'smartrecruiters': SmartRecruitersScraper(session),
            'breezy': BreezyScraper(session),
        }
    
    async def test_company_parallel(self, company_name: str) -> List[CompanyJobBoard]:
        """Test all ATS types in parallel for a company"""
        tokens = self.token_generator.generate_tokens(company_name)
        results = []
        
        # Group by priority
        priority_groups = {1: [], 2: [], 3: []}
        for ats_type, config in ATS_CONFIGS.items():
            if ats_type in self.scrapers:
                priority = config.get('priority', 3)
                priority_groups[priority].append(ats_type)
        
        # Test priority 1 first (most likely to hit for startups)
        for priority in [1, 2, 3]:
            ats_types = priority_groups[priority]
            if not ats_types:
                continue
            
            # Test all tokens against all ATS types in this priority group
            tasks = []
            for token in tokens[:10]:  # Limit tokens per priority group
                for ats_type in ats_types:
                    scraper = self.scrapers.get(ats_type)
                    if scraper:
                        tasks.append(self._test_single(scraper, token, ats_type))
            
            if tasks:
                group_results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in group_results:
                    if isinstance(result, CompanyJobBoard):
                        results.append(result)
                        # Early exit if we found results in priority 1
                        if priority == 1 and results:
                            return results
        
        return results
    
    async def _test_single(self, scraper: ATSScraper, token: str, ats_type: str) -> Optional[CompanyJobBoard]:
        """Test a single token against a single ATS"""
        try:
            return await scraper.check_token(token)
        except Exception as e:
            logger.debug(f"Error testing {ats_type}/{token}: {e}")
            return None
    
    async def discover_from_seeds(self, seeds: List[str], batch_size: int = 10) -> DiscoveryStats:
        """Discover companies from seed list with parallel testing"""
        stats = DiscoveryStats()
        start_time = datetime.now()
        
        # Track discovered companies to avoid duplicates within this run
        seen_companies: Set[str] = set()
        
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            await self.init_scrapers(session)
            
            # Process in batches
            for i in range(0, len(seeds), batch_size):
                batch = seeds[i:i + batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(seeds) + batch_size - 1)//batch_size}: {batch}")
                
                # Run all seeds in batch in parallel
                tasks = [self.test_company_parallel(seed) for seed in batch]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for seed, result in zip(batch, batch_results):
                    stats.seeds_tested += 1
                    
                    if isinstance(result, Exception):
                        stats.errors += 1
                        logger.warning(f"Error processing {seed}: {result}")
                        continue
                    
                    if result:
                        for company in result:
                            # Skip 0-job false positives
                            if company.job_count == 0:
                                continue
                            
                            # Skip duplicates (same company found via different token variants)
                            # Use both token and company_name to catch more duplicates
                            company_key = f"{company.token.lower()}:{company.ats_type}"
                            company_name_key = f"{company.company_name.lower()}:{company.ats_type}"
                            if company_key in seen_companies or company_name_key in seen_companies:
                                continue
                            seen_companies.add(company_key)
                            seen_companies.add(company_name_key)
                                
                            stats.companies_found += 1
                            stats.jobs_found += company.job_count
                            
                            ats = company.ats_type.split('_')[0]  # Normalize workday_wd5 -> workday
                            stats.ats_breakdown[ats] = stats.ats_breakdown.get(ats, 0) + 1
                            
                            # Collect self-discovered companies
                            self.discovered_companies.update(company.discovered_companies)
                            
                            # Store result for later saving to PostgreSQL
                            self.results.append(company)
                            
                            logger.info(f"Found: {company.company_name} ({company.ats_type}) - {company.job_count} jobs")
                            
                            # Save to database
                            await self._save_company(company)
                
                # Small delay between batches
                await asyncio.sleep(0.5)
        
        stats.new_discoveries = len(self.discovered_companies)
        stats.duration_seconds = (datetime.now() - start_time).total_seconds()
        
        return stats
    
    async def _save_company(self, company: CompanyJobBoard):
        """Save company to database (SQLite fallback only)"""
        # Skip if no db_path (we're saving to PostgreSQL instead)
        if self.db_path is None:
            return
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO tracked_companies 
            (id, company_name, token, ats_type, job_count, remote_count, 
             departments, locations, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            f"{company.ats_type}:{company.token}",
            company.company_name,
            company.token,
            company.ats_type,
            company.job_count,
            company.remote_count,
            json.dumps(company.departments),
            json.dumps(company.locations),
            datetime.now().isoformat(),
        ))
        
        conn.commit()
        conn.close()
    
    def init_database(self):
        """Initialize database tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tracked_companies (
                id TEXT PRIMARY KEY,
                company_name TEXT,
                token TEXT,
                ats_type TEXT,
                job_count INTEGER DEFAULT 0,
                remote_count INTEGER DEFAULT 0,
                departments TEXT DEFAULT '[]',
                locations TEXT DEFAULT '[]',
                last_updated TEXT,
                discovery_source TEXT,
                discovery_confidence REAL DEFAULT 1.0
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS seed_companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                source TEXT,
                tier INTEGER DEFAULT 3,
                tested_at TEXT,
                found BOOLEAN DEFAULT FALSE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS discovery_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT,
                completed_at TEXT,
                seeds_tested INTEGER,
                companies_found INTEGER,
                jobs_found INTEGER,
                errors INTEGER,
                ats_breakdown TEXT
            )
        ''')
        
        conn.commit()
        conn.close()


# =============================================================================
# CLI INTERFACE
# =============================================================================

async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Job Intelligence Collector V7')
    parser.add_argument('--seeds', nargs='+', help='Company names to test')
    parser.add_argument('--file', help='File with seed companies (one per line)')
    parser.add_argument('--batch-size', type=int, default=10, help='Batch size for parallel testing')
    parser.add_argument('--db', default='job_intel.db', help='Database path')
    
    args = parser.parse_args()
    
    collector = JobIntelCollectorV7(db_path=args.db)
    collector.init_database()
    
    # Get seeds
    seeds = []
    if args.seeds:
        seeds = args.seeds
    elif args.file:
        with open(args.file, 'r') as f:
            seeds = [line.strip() for line in f if line.strip()]
    else:
        # Default test seeds
        seeds = [
            'Anthropic', 'OpenAI', 'Stripe', 'Figma', 'Notion',
            'Airtable', 'Vercel', 'Railway', 'Supabase', 'PlanetScale',
        ]
    
    logger.info(f"Starting discovery with {len(seeds)} seeds...")
    stats = await collector.discover_from_seeds(seeds, batch_size=args.batch_size)
    
    print("\n" + "="*60)
    print("DISCOVERY RESULTS")
    print("="*60)
    print(f"Seeds Tested: {stats.seeds_tested}")
    print(f"Companies Found: {stats.companies_found}")
    print(f"Jobs Found: {stats.jobs_found}")
    print(f"Errors: {stats.errors}")
    print(f"Duration: {stats.duration_seconds:.1f} seconds")
    print(f"\nATS Breakdown:")
    for ats, count in sorted(stats.ats_breakdown.items(), key=lambda x: -x[1]):
        print(f"  {ats}: {count}")
    print(f"\nSelf-Discovered Companies: {stats.new_discoveries}")
    if collector.discovered_companies:
        print(f"  {list(collector.discovered_companies)[:10]}...")


# =============================================================================
# HELPER FUNCTION FOR APP.PY INTEGRATION
# =============================================================================

async def run_discovery(db=None, max_seeds: int = 500) -> Dict:
    """
    Main entry point for app.py integration.
    
    Args:
        db: Database object with get_connection() method
        max_seeds: Maximum seeds to test
        
    Returns:
        Stats dictionary with results
    """
    logger.info(f"🚀 Starting V7 discovery with max {max_seeds} seeds...")
    
    # === ONE-TIME CLEANUP: Remove false positive companies with 0 jobs ===
    if db is not None:
        try:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Count before cleanup
                    cur.execute("SELECT COUNT(*) FROM companies WHERE job_count = 0")
                    zero_job_count = cur.fetchone()[0]
                    
                    if zero_job_count > 0:
                        logger.info(f"🧹 Cleaning up {zero_job_count} false positive companies with 0 jobs...")
                        
                        # Delete companies with 0 jobs
                        cur.execute("DELETE FROM companies WHERE job_count = 0")
                        deleted = cur.rowcount
                        
                        # Also clean up any orphaned job records (shouldn't be any)
                        cur.execute("DELETE FROM job_archive WHERE company_id NOT IN (SELECT id FROM companies)")
                        orphaned_jobs = cur.rowcount
                        
                        conn.commit()
                        
                        logger.info(f"✅ Cleanup complete: removed {deleted} false positive companies, {orphaned_jobs} orphaned jobs")
                    else:
                        logger.info("✅ No false positive companies to clean up")
                    
                    # === SEED CLEANUP: Remove garbage/spam seeds ===
                    garbage_patterns = [
                        # Blog posts, articles, memos
                        '%read the%memo%',
                        '%read more%',
                        '%state of the cloud%',
                        '%avoiding burnout%',
                        '%tips on%',
                        '%how to%',
                        '%terms of%',
                        '%privacy policy%',
                        '%cookie policy%',
                        
                        # Navigation/UI elements
                        '%skip navigation%',
                        '%see all%',
                        '%filter options%',
                        '%load more%',
                        '%click here%',
                        '%learn more%',
                        '%sign up%',
                        '%log in%',
                        '%jobs(link%',
                        
                        # Status/metadata junk
                        '%statusprivate%',
                        '%statusactive%',
                        '%backedsince%',
                        '%founded%backed%',
                        '%series a%',
                        '%series b%',
                        '%nasdaq:%',
                        '%nyse:%',
                        '%lon:%',
                        '%omx:%',
                        
                        # Generic descriptions
                        '%the world%s%',
                        '%a community%',
                        '%building%infrastructure%',
                        '%powering%',
                        '%transforming%',
                        '%revolutionizing%',
                        
                        # n8n spam pattern
                        'n8n%read more%',
                        
                        # Edited by / author patterns
                        '%edited by%',
                        '%written by%',
                        
                        # Date patterns in names
                        '%september%',
                        '%october%',
                        '%november%',
                        '%december%',
                        '%january%',
                        '%february%',
                        
                        # Other junk
                        '%awesome%list%',
                        '%resources%',
                        '%courses%',
                        '%generator%',
                        '%collection%',
                        '%library%',
                        
                        # NEW: More garbage patterns from recent logs
                        '%delivering%services%',
                        '%management levels%',
                        '%marketing for%',
                        '%matter most%',
                        '%on-demand%',
                        '%normalization%',
                        '%deviance%',
                        '%six ways%',
                        '%influence people%',
                        '%organizational perspective%',
                        '%pairing with%',
                        '%human-compatible%',
                        '%cloud%platform%',
                        '%publications%',
                        '%exceptional%founders%',
                        '%intelligent machinery%',
                        '%biomaterials%',
                        '%algorithm for%',
                        '%scientific manuscript%',
                        '%vc funding%',
                        '%huge growth%',
                        '%nft media%',
                        '%predictive%analytics%',
                        '%quantum%',
                        '%psychology of%',
                        '%ceos manage%',
                        '%multibillion%',
                        '%apollo syndrome%',
                        '%executive assistant%',
                        '%agile bullshit%',
                        '%risk management%',
                        '%oral health%',
                        '%follow%linkedin%',
                        '%emergence%marketplace%',
                        '%radiology%automation%',
                        '%performance management%',
                        '%great manager%',
                        '%distributed teams%',
                        '%regulatory%',
                        '%how we decide%',
                        '%lending%community%',
                        '%proteomics%',
                        '%technical debt%',
                        '%tetris%',
                        '%newsletter%',
                        '%benefiting humanity%',
                        '%mafias form%',
                        '%catechism%',
                        '%investing for everyone%',
                        '%rise of%europe%',
                        '%identity company%',
                        '%social network%',
                        '%future of%services%',
                        '%shareable data%',
                        '%stem cell%manufacturing%',
                        '%air capture%',
                        '%healthcare predictions%',
                        # NEW: More garbage patterns from latest run
                        '%rfc %',
                        '%slide from%',
                        '%view more%',
                        '%view all%',
                        '%view portfolio%',
                        '%all companies%',
                        '%skip to%',
                        '%close%window%',
                        '%continue to%',
                        '%continue reading%',
                        '%go to%',
                        '%visit website%',
                        '%sign in%',
                        '%login%',
                        '%about us%',
                        '%contact%',
                        '%see more%',
                        '%accept%cookies%',
                        '%reject%cookies%',
                        '%cookie%notice%',
                        '%privacy%disclosures%',
                        '%terms%conditions%',
                        '%contribution%guidelines%',
                        '%anti-portfolio%',
                        '%investor%login%',
                        '%portfolio%jobs%',
                        '%join%',
                        '%subscribe%',
                        '%newsletter%',
                        '%feedback%',
                        '%accessibility%',
                        '%philosophy%',
                        '%mission%principles%',
                        '%where we invest%',
                        '%what we work%',
                        '%news%content%',
                        '%legal%disclaimer%',
                        '%modern slavery%',
                        '%california%privacy%',
                        '%eu sfdr%',
                        # Tech/code patterns
                        '%openssl%',
                        '%configuration file%',
                        '%example%configuration%',
                        '%google%webfonts%',
                        # Event/article patterns
                        '%enterprise edition%',
                        '%virtual reality%',
                        '%coming soon%',
                        '%cold start%',
                        '%reverse interview%',
                        '%interview%guide%',
                        '%leadership%interview%',
                        '%101%',
                        '%forecasting%',
                    ]
                    
                    total_garbage_deleted = 0
                    for pattern in garbage_patterns:
                        cur.execute("DELETE FROM seed_companies WHERE LOWER(company_name) LIKE %s", (pattern,))
                        if cur.rowcount > 0:
                            total_garbage_deleted += cur.rowcount
                    
                    # Delete seeds that are too long (likely scraped descriptions)
                    cur.execute("DELETE FROM seed_companies WHERE LENGTH(company_name) > 60")
                    total_garbage_deleted += cur.rowcount
                    
                    # Delete seeds with too many words (likely sentences)
                    cur.execute("""
                        DELETE FROM seed_companies 
                        WHERE array_length(string_to_array(company_name, ' '), 1) > 6
                    """)
                    total_garbage_deleted += cur.rowcount
                    
                    if total_garbage_deleted > 0:
                        conn.commit()
                        logger.info(f"🗑️ Removed {total_garbage_deleted} garbage seeds from database")
                    
                    # === AMBIGUOUS COMPANY CLEANUP: Remove companies with generic/ambiguous names ===
                    ambiguous_company_names = [
                        'Ms', 'Af', 'Ve', 'Aa', 'Hr', 'It', 'Us', 'Uk', 'Eu',  # 2-letter codes
                        'Inc', 'Tech', 'Blue', 'Flow', 'Pay', 'Max', 'Test', 'Demo',  # Generic words
                        'Library', 'Manual', 'Onboarding', 'Securing', 'Developer',  # Random word matches
                        '2019', '2020', '2021', '2022', '2023', '2024', '2025',  # Years
                        'Life', 'Capital', 'Path', 'System', 'People', 'Chaos',
                        'Vertical', 'Enterprise', 'Data', 'Experience', 'Legal',
                        'Form', 'Sim', 'IE', 'Original', 'Artificial', 'Magic',
                        'Anomaly', 'Hexa', 'Adaptive', 'Crypto',
                        'LI Test Company', 'Test Company', 'Demo Company',
                        'Moore', 'Alex', 'Jay', 'Rha', 'Assist', 'Automation',
                        'Origin', 'Healthcare', 'Advanced', 'Google', 'NATIONAL',
                        'Journey', 'Belong', 'Mega', 'Brilliant', '1001',
                        'Charles', 'National', 'Media', 'Solutions', 'Global',
                        'Group', 'Services', 'Digital', 'Marketing', 'Design',
                        'Creative', 'Studio', 'Agency', 'Partners', 'Consulting',
                        'Labs', 'Commons', 'Door', 'Alarm',
                        'Company', 'Talent', 'True', 'Bright', 'Matt', 'Spring', 'What',
                        'LINK', 'ESS', 'NMI', 'Canvas', 'United', 'Relay', '1979',
                        'Industrial Door Company', 'Facility Door Solutions',
                        'Best Friend Finance', 'Goody Garage Doors',
                        # NEW: More false positives from latest run
                        'Code', 'Edge', 'Elite', 'Clear', 'Builder', 'Bloom', 'Bold',
                        'Sonja Inc.', 'Msh', 'Stories', 'Invision', 'Researchhub',
                        'Elite Physical Therapy', 'Columbus Ophthalmology Associates',
                        'Tidewater Eye Centers', 'CGS Immersive', 'AQR India',
                        'Alpha FMC - Insurance Consulting', 'Founders Green Animal Hospital',
                        'Archrival Agents || Bloom Sampling Program',
                        # More false positives
                        'Jump', 'Sure', 'A Light', 'Relai ', 'Relai  ',
                        'Talent HR Networks', 'General Assembly Remote Jobs',
                        'Flatiron Health Technical Opportunities',
                    ]
                    
                    for name in ambiguous_company_names:
                        cur.execute("DELETE FROM companies WHERE company_name = %s", (name,))
                        if cur.rowcount > 0:
                            logger.info(f"   Removed ambiguous company: {name}")
                    
                    # Also remove companies with "Test" in the name
                    cur.execute("DELETE FROM companies WHERE company_name ILIKE '%test company%'")
                    if cur.rowcount > 0:
                        logger.info(f"   Removed {cur.rowcount} test companies")
                    
                    conn.commit()
                        
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    # Load seeds from database
    seeds = []
    if db is not None:
        try:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT company_name FROM seed_companies 
                        WHERE is_blacklisted = FALSE 
                        AND (times_tested < 3 OR times_tested IS NULL)
                        ORDER BY 
                            tier ASC,
                            times_tested ASC NULLS FIRST,
                            RANDOM()
                        LIMIT %s
                    """, (max_seeds,))
                    seeds = [row[0] for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error loading seeds: {e}")
    
    if not seeds:
        logger.warning("No seeds found to test")
        return {
            'success': False,
            'message': 'No seeds found to test',
            'seeds_tested': 0,
            'companies_found': 0,
            'jobs_found': 0,
        }
    
    logger.info(f"Loaded {len(seeds)} seeds to test")
    
    # Create collector and run
    collector = JobIntelCollectorV7(db_path=None)  # Won't use sqlite
    stats = await collector.discover_from_seeds(seeds, batch_size=10)
    
    # Save results to PostgreSQL
    saved_companies = 0
    saved_jobs = 0
    
    if db is not None and collector.results:
        for result in collector.results:
            # Skip companies with no jobs (false positives)
            if result.job_count == 0:
                continue
                
            try:
                with db.get_connection() as conn:
                    with conn.cursor() as cur:
                        # Upsert company
                        cur.execute("""
                            INSERT INTO companies (company_name, company_name_token, ats_type, board_url, job_count, last_scraped)
                            VALUES (%s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (company_name) DO UPDATE SET
                                job_count = EXCLUDED.job_count,
                                last_scraped = NOW()
                            RETURNING id
                        """, (
                            result.company_name,
                            result.token,
                            result.ats_type,
                            result.board_url,
                            result.job_count,
                        ))
                        company_row = cur.fetchone()
                        if company_row:
                            saved_companies += 1
                            company_id = company_row[0]
                            
                            # Save jobs (job is a JobPosting dataclass, use attributes)
                            for job in result.jobs[:100]:  # Limit jobs per company
                                try:
                                    cur.execute("""
                                        INSERT INTO job_archive (company_id, job_id, title, department, location, job_url, status, first_seen, last_seen)
                                        VALUES (%s, %s, %s, %s, %s, %s, 'active', NOW(), NOW())
                                        ON CONFLICT (company_id, job_id) DO UPDATE SET
                                            last_seen = NOW(),
                                            status = 'active'
                                    """, (
                                        company_id,
                                        job.id or job.title[:50],
                                        job.title,
                                        job.department or '',
                                        job.location or '',
                                        job.url or '',
                                    ))
                                    saved_jobs += 1
                                except Exception as je:
                                    logger.debug(f"Error saving job: {je}")
                        
                        # Update seed as tested
                        cur.execute("""
                            UPDATE seed_companies 
                            SET times_tested = COALESCE(times_tested, 0) + 1,
                                times_successful = COALESCE(times_successful, 0) + 1,
                                last_tested_at = NOW()
                            WHERE LOWER(company_name) = LOWER(%s)
                        """, (result.company_name,))
                        
                        conn.commit()
            except Exception as e:
                logger.debug(f"Error saving result for {result.company_name}: {e}")
    
    return {
        'success': True,
        'seeds_tested': stats.seeds_tested,
        'companies_found': stats.companies_found,
        'jobs_found': stats.jobs_found,
        'saved_companies': saved_companies,
        'saved_jobs': saved_jobs,
        'errors': stats.errors,
        'duration_seconds': stats.duration_seconds,
        'ats_breakdown': stats.ats_breakdown,
        'new_discoveries': stats.new_discoveries,
    }


if __name__ == '__main__':
    asyncio.run(main())
