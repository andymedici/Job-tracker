"""
Configuration Management
Centralized application configuration
"""
import os
from dataclasses import dataclass, field
from typing import Optional, Dict, List


@dataclass
class Config:
    """Application configuration"""
    
    # =========================================================================
    # EXISTING SETTINGS (unchanged)
    # =========================================================================
    
    # Database
    DATABASE_URL: str = os.getenv('DATABASE_URL', '')
    DB_POOL_SIZE: int = int(os.getenv('DB_POOL_SIZE', 15))
    DB_MAX_OVERFLOW: int = int(os.getenv('DB_MAX_OVERFLOW', 25))
    
    # Security
    API_KEY: str = os.getenv('API_KEY', '')
    ADMIN_API_KEY: str = os.getenv('ADMIN_API_KEY', '')
    
    # Redis
    REDIS_URL: Optional[str] = os.getenv('REDIS_URL')
    
    # Features
    ENABLE_WEB_SCRAPING: bool = os.getenv('ENABLE_WEB_SCRAPING', 'true').lower() == 'true'
    ENABLE_EMAIL_REPORTS: bool = os.getenv('ENABLE_EMAIL_REPORTS', 'false').lower() == 'true'
    ALLOW_QUERY_API_KEY: bool = os.getenv('ALLOW_QUERY_API_KEY', 'false').lower() == 'true'
    
    # Flask
    PORT: int = int(os.getenv('PORT', 8080))
    DEBUG: bool = os.getenv('DEBUG', 'false').lower() == 'true'
    
    # Scheduler
    SCHEDULER_TIMEZONE: str = os.getenv('SCHEDULER_TIMEZONE', 'UTC')
    REFRESH_INTERVAL_HOURS: int = int(os.getenv('REFRESH_INTERVAL_HOURS', 6))
    
    # Monitoring
    SENTRY_DSN: Optional[str] = os.getenv('SENTRY_DSN')
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
    # =========================================================================
    # UPGRADE MODULE SETTINGS (new)
    # =========================================================================
    
    # Collector V7 Settings
    COLLECTOR_BATCH_SIZE: int = int(os.getenv('COLLECTOR_BATCH_SIZE', 10))
    COLLECTOR_TIMEOUT: int = int(os.getenv('COLLECTOR_TIMEOUT', 30))
    COLLECTOR_MAX_RETRIES: int = int(os.getenv('COLLECTOR_MAX_RETRIES', 3))
    COLLECTOR_PARALLEL_WORKERS: int = int(os.getenv('COLLECTOR_PARALLEL_WORKERS', 5))
    
    # Rate Limiting (requests per second)
    RATE_LIMIT_GREENHOUSE: float = float(os.getenv('RATE_LIMIT_GREENHOUSE', 2.0))
    RATE_LIMIT_LEVER: float = float(os.getenv('RATE_LIMIT_LEVER', 2.0))
    RATE_LIMIT_WORKDAY: float = float(os.getenv('RATE_LIMIT_WORKDAY', 1.0))
    RATE_LIMIT_DEFAULT: float = float(os.getenv('RATE_LIMIT_DEFAULT', 1.5))
    
    # Seed Expander Settings
    SEED_EXPANDER_TIERS: str = os.getenv('SEED_EXPANDER_TIERS', '1,2')  # Comma-separated
    SEED_MIN_LENGTH: int = int(os.getenv('SEED_MIN_LENGTH', 2))
    SEED_MAX_LENGTH: int = int(os.getenv('SEED_MAX_LENGTH', 100))
    SEED_MAX_WORDS: int = int(os.getenv('SEED_MAX_WORDS', 8))
    
    # Self-Growth Settings
    SELF_GROWTH_ENABLED: bool = os.getenv('SELF_GROWTH_ENABLED', 'true').lower() == 'true'
    SELF_GROWTH_MIN_CONFIDENCE: float = float(os.getenv('SELF_GROWTH_MIN_CONFIDENCE', 0.7))
    SELF_GROWTH_AUTO_PROMOTE: bool = os.getenv('SELF_GROWTH_AUTO_PROMOTE', 'true').lower() == 'true'
    SELF_GROWTH_DAILY_LIMIT: int = int(os.getenv('SELF_GROWTH_DAILY_LIMIT', 500))
    
    # Proxy Settings (optional)
    PROXY_ENABLED: bool = os.getenv('PROXY_ENABLED', 'false').lower() == 'true'
    PROXY_URL: Optional[str] = os.getenv('PROXY_URL')
    PROXY_ROTATION: bool = os.getenv('PROXY_ROTATION', 'false').lower() == 'true'
    
    # Cache Settings
    CACHE_ATS_RESULTS: bool = os.getenv('CACHE_ATS_RESULTS', 'true').lower() == 'true'
    CACHE_TTL_SECONDS: int = int(os.getenv('CACHE_TTL_SECONDS', 3600))
    
    # Scheduled Tasks
    SELF_GROWTH_SCHEDULE_HOUR: int = int(os.getenv('SELF_GROWTH_SCHEDULE_HOUR', 4))  # 4 AM UTC
    MEGA_EXPAND_SCHEDULE_DAY: str = os.getenv('MEGA_EXPAND_SCHEDULE_DAY', 'sun')  # Sunday
    MEGA_EXPAND_SCHEDULE_HOUR: int = int(os.getenv('MEGA_EXPAND_SCHEDULE_HOUR', 5))  # 5 AM UTC


# =========================================================================
# ATS CONFIGURATIONS (used by collector_v7.py)
# =========================================================================

ATS_CONFIGS: Dict[str, dict] = {
    'greenhouse': {
        'priority': 1,
        'api_url': 'https://boards-api.greenhouse.io/v1/boards/{token}/jobs',
        'board_url': 'https://boards.greenhouse.io/{token}',
        'rate_limit': 2.0,
    },
    'lever': {
        'priority': 1,
        'api_url': 'https://api.lever.co/v0/postings/{token}?mode=json',
        'board_url': 'https://jobs.lever.co/{token}',
        'rate_limit': 2.0,
    },
    'ashby': {
        'priority': 1,
        'api_url': 'https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams',
        'board_url': 'https://jobs.ashbyhq.com/{token}',
        'rate_limit': 2.0,
    },
    'workday': {
        'priority': 2,
        'url_patterns': [
            'https://{token}.wd5.myworkdayjobs.com/wday/cxs/{token}/External/jobs',
            'https://{token}.wd1.myworkdayjobs.com/wday/cxs/{token}/External/jobs',
            'https://{token}.wd3.myworkdayjobs.com/wday/cxs/{token}/External/jobs',
            'https://{token}.wd12.myworkdayjobs.com/wday/cxs/{token}/External/jobs',
        ],
        'rate_limit': 1.0,
    },
    'icims': {
        'priority': 2,
        'url_patterns': [
            'https://careers-{token}.icims.com/jobs/search',
            'https://{token}.icims.com/jobs/search',
        ],
        'rate_limit': 1.0,
    },
    'taleo': {
        'priority': 2,
        'url_pattern': 'https://{token}.taleo.net/careersection/jobsearch.ftl',
        'rate_limit': 1.0,
    },
    'successfactors': {
        'priority': 2,
        'url_pattern': 'https://{token}.successfactors.com/career',
        'rate_limit': 1.0,
    },
    'workable': {
        'priority': 1,
        'api_url': 'https://apply.workable.com/api/v1/widget/accounts/{token}',
        'board_url': 'https://apply.workable.com/{token}',
        'rate_limit': 2.0,
    },
    'smartrecruiters': {
        'priority': 2,
        'api_url': 'https://api.smartrecruiters.com/v1/companies/{token}/postings',
        'board_url': 'https://careers.smartrecruiters.com/{token}',
        'rate_limit': 1.5,
    },
    'recruitee': {
        'priority': 2,
        'api_url': 'https://{token}.recruitee.com/api/offers',
        'board_url': 'https://{token}.recruitee.com',
        'rate_limit': 2.0,
    },
    'personio': {
        'priority': 3,
        'api_url': 'https://{token}.jobs.personio.com/api/v1/jobs',
        'board_url': 'https://{token}.jobs.personio.com',
        'rate_limit': 1.5,
    },
    'teamtailor': {
        'priority': 2,
        'api_url': 'https://api.teamtailor.com/v1/jobs',
        'board_url': 'https://career.{token}.com',
        'rate_limit': 1.5,
    },
    'breezy': {
        'priority': 3,
        'api_url': 'https://{token}.breezy.hr/json',
        'board_url': 'https://{token}.breezy.hr',
        'rate_limit': 2.0,
    },
    'jazz': {
        'priority': 3,
        'api_url': 'https://{token}.applytojob.com/apply/jobs',
        'board_url': 'https://{token}.applytojob.com/apply',
        'rate_limit': 1.5,
    },
    'pinpoint': {
        'priority': 3,
        'api_url': 'https://{token}.pinpointhq.com/api/v1/jobs',
        'board_url': 'https://{token}.pinpointhq.com',
        'rate_limit': 1.5,
    },
}


# =========================================================================
# SPECIAL COMPANY MAPPINGS (Fortune 500, big tech, etc.)
# =========================================================================

COMPANY_TOKEN_MAPPINGS: Dict[str, List[str]] = {
    'meta': ['meta', 'facebook', 'fb', 'metacareers'],
    'alphabet': ['alphabet', 'google', 'googl'],
    'amazon': ['amazon', 'amzn', 'aws'],
    'apple': ['apple', 'applecare'],
    'microsoft': ['microsoft', 'msft', 'ms'],
    'jpmorgan chase': ['jpmorgan', 'jpmorganchase', 'jpmc', 'chase', 'jpm'],
    'bank of america': ['bankofamerica', 'bofa', 'boa', 'bac'],
    'wells fargo': ['wellsfargo', 'wf', 'wfc'],
    'citigroup': ['citigroup', 'citi', 'citibank'],
    'goldman sachs': ['goldmansachs', 'gs', 'goldman'],
    'morgan stanley': ['morganstanley', 'ms', 'morgan-stanley'],
    'johnson & johnson': ['johnsonandjohnson', 'jnj', 'jandj'],
    'procter & gamble': ['procterandgamble', 'pg', 'proctergamble'],
    'coca-cola': ['cocacola', 'coke', 'ko'],
    'pepsico': ['pepsico', 'pepsi', 'pep'],
    'walmart': ['walmart', 'wmt', 'wal-mart'],
    'home depot': ['homedepot', 'hd', 'thd'],
    'att': ['att', 'at-t', 'atandt'],
    'verizon': ['verizon', 'vz', 'vzw'],
    'comcast': ['comcast', 'cmcsa', 'xfinity'],
    'disney': ['disney', 'dis', 'waltdisney'],
    'netflix': ['netflix', 'nflx'],
    'nvidia': ['nvidia', 'nvda'],
    'salesforce': ['salesforce', 'sfdc', 'crm'],
    'adobe': ['adobe', 'adbe'],
    'oracle': ['oracle', 'orcl'],
    'ibm': ['ibm', 'international-business-machines'],
    'intel': ['intel', 'intc'],
    'cisco': ['cisco', 'csco'],
    'uber': ['uber'],
    'lyft': ['lyft'],
    'airbnb': ['airbnb', 'abnb'],
    'doordash': ['doordash', 'dash'],
    'instacart': ['instacart'],
    'stripe': ['stripe'],
    'plaid': ['plaid'],
    'square': ['square', 'squareup', 'block', 'sq'],
    'paypal': ['paypal', 'pypl'],
    'robinhood': ['robinhood', 'hood'],
    'coinbase': ['coinbase', 'coin'],
    'openai': ['openai', 'open-ai'],
    'anthropic': ['anthropic'],
    'databricks': ['databricks'],
    'snowflake': ['snowflake', 'snow'],
    'datadog': ['datadog', 'ddog'],
    'splunk': ['splunk', 'splk'],
    'mongodb': ['mongodb', 'mdb'],
    'elastic': ['elastic', 'estc', 'elasticsearch'],
    'twilio': ['twilio', 'twlo'],
    'okta': ['okta'],
    'crowdstrike': ['crowdstrike', 'crwd'],
    'palo alto networks': ['paloaltonetworks', 'panw', 'paloalto'],
    'zscaler': ['zscaler', 'zs'],
    'fortinet': ['fortinet', 'ftnt'],
}


# =========================================================================
# SEED SOURCE URLS (used by mega_seed_expander.py)
# =========================================================================

SEED_SOURCES: Dict[str, dict] = {
    'yc_api': {
        'tier': 1,
        'url': 'https://45bwzj1sgc-dsn.algolia.net/1/indexes/*/queries',
        'type': 'api',
        'expected_count': 4000,
    },
    'inc_5000': {
        'tier': 1,
        'url': 'https://www.inc.com/inc5000',
        'type': 'scrape',
        'expected_count': 5000,
    },
    'forbes_cloud100': {
        'tier': 1,
        'url': 'https://www.forbes.com/lists/cloud100',
        'type': 'scrape',
        'expected_count': 100,
    },
    'sec_tickers': {
        'tier': 2,
        'url': 'https://www.sec.gov/files/company_tickers.json',
        'type': 'api',
        'expected_count': 8000,
    },
    'wikipedia_tech': {
        'tier': 2,
        'url': 'https://en.wikipedia.org/wiki/List_of_largest_technology_companies_by_revenue',
        'type': 'scrape',
        'expected_count': 500,
    },
    'builtin_sf': {
        'tier': 2,
        'url': 'https://builtin.com/companies/san-francisco',
        'type': 'scrape',
        'expected_count': 500,
    },
    'builtin_nyc': {
        'tier': 2,
        'url': 'https://builtin.com/companies/new-york',
        'type': 'scrape',
        'expected_count': 500,
    },
    'product_hunt': {
        'tier': 3,
        'url': 'https://www.producthunt.com/topics/developer-tools',
        'type': 'scrape',
        'expected_count': 2000,
    },
    'wellfound': {
        'tier': 3,
        'url': 'https://wellfound.com/companies',
        'type': 'scrape',
        'expected_count': 5000,
    },
}


# =========================================================================
# VC PORTFOLIOS (used by mega_seed_expander.py)
# =========================================================================

VC_PORTFOLIOS: List[Dict[str, str]] = [
    {'name': 'a16z', 'url': 'https://a16z.com/portfolio/'},
    {'name': 'sequoia', 'url': 'https://www.sequoiacap.com/our-companies/'},
    {'name': 'accel', 'url': 'https://www.accel.com/portfolio'},
    {'name': 'benchmark', 'url': 'https://www.benchmark.com/portfolio'},
    {'name': 'greylock', 'url': 'https://greylock.com/portfolio/'},
    {'name': 'index', 'url': 'https://www.indexventures.com/companies/'},
    {'name': 'lightspeed', 'url': 'https://lsvp.com/portfolio/'},
    {'name': 'general_catalyst', 'url': 'https://www.generalcatalyst.com/portfolio'},
    {'name': 'bessemer', 'url': 'https://www.bvp.com/portfolio'},
    {'name': 'insight', 'url': 'https://www.insightpartners.com/portfolio/'},
    {'name': 'tiger_global', 'url': 'https://www.tigerglobal.com/portfolio'},
    {'name': 'founders_fund', 'url': 'https://foundersfund.com/portfolio/'},
    {'name': 'nea', 'url': 'https://www.nea.com/portfolio'},
    {'name': 'kleiner_perkins', 'url': 'https://www.kleinerperkins.com/portfolios/'},
    {'name': 'redpoint', 'url': 'https://www.redpoint.com/companies/'},
    {'name': 'spark', 'url': 'https://www.sparkcapital.com/portfolio'},
    {'name': 'usv', 'url': 'https://www.usv.com/portfolio'},
    {'name': 'first_round', 'url': 'https://firstround.com/companies/'},
    {'name': 'thrive', 'url': 'https://thrivecap.com/companies/'},
    {'name': 'khosla', 'url': 'https://www.khoslaventures.com/portfolio'},
    {'name': 'craft', 'url': 'https://www.craftventures.com/portfolio'},
    {'name': 'felicis', 'url': 'https://www.felicis.com/portfolio'},
    {'name': 'battery', 'url': 'https://www.battery.com/our-companies/'},
    {'name': 'ivp', 'url': 'https://www.ivp.com/portfolio/'},
    {'name': 'emergence', 'url': 'https://www.emcap.com/companies'},
]


# =========================================================================
# VALIDATION BLACKLISTS (used by mega_seed_expander.py)
# =========================================================================

BLACKLIST_UI_TERMS: set = {
    'login', 'logout', 'sign in', 'sign up', 'register', 'forgot password',
    'menu', 'navigation', 'header', 'footer', 'sidebar', 'search', 'filter',
    'sort', 'pagination', 'next', 'previous', 'back', 'home', 'about',
    'contact', 'privacy', 'terms', 'cookies', 'settings', 'profile',
}

BLACKLIST_GEOGRAPHIC: set = {
    'new york', 'san francisco', 'los angeles', 'chicago', 'boston',
    'seattle', 'austin', 'denver', 'miami', 'atlanta', 'dallas',
    'california', 'texas', 'florida', 'washington', 'massachusetts',
    'usa', 'united states', 'canada', 'uk', 'germany', 'france',
    'remote', 'hybrid', 'onsite', 'worldwide', 'global',
}

BLACKLIST_JUNK: set = {
    'test', 'demo', 'example', 'sample', 'placeholder', 'lorem', 'ipsum',
    'null', 'undefined', 'n/a', 'na', 'none', 'unknown', 'other',
    'company', 'corporation', 'inc', 'llc', 'ltd', 'corp',
    'job', 'jobs', 'career', 'careers', 'hiring', 'openings',
    'position', 'positions', 'role', 'roles', 'opportunity',
}


# =========================================================================
# SELF-GROWTH PATTERNS (used by self_growth_intelligence.py)
# =========================================================================

DISCOVERY_PATTERNS: Dict[str, Dict] = {
    'partner': {
        'patterns': ['partner with', 'partnered with', 'partnership with', 'in partnership'],
        'confidence': 0.7,
    },
    'customer': {
        'patterns': ['customers include', 'trusted by', 'used by', 'chosen by'],
        'confidence': 0.6,
    },
    'competitor': {
        'patterns': ['competitor to', 'alternative to', 'competes with', 'vs'],
        'confidence': 0.75,
    },
    'integration': {
        'patterns': ['integrates with', 'integrate with', 'integration with', 'works with'],
        'confidence': 0.7,
    },
    'acquired': {
        'patterns': ['acquired by', 'acquisition of', 'parent company'],
        'confidence': 0.85,
    },
    'funding': {
        'patterns': ['raises $', 'raised $', 'closes $', 'secures $', 'funding round'],
        'confidence': 0.9,
    },
}

# Known integrations to filter out (not new companies)
KNOWN_INTEGRATIONS: set = {
    'salesforce', 'hubspot', 'slack', 'github', 'gitlab', 'jira', 'confluence',
    'google', 'microsoft', 'aws', 'azure', 'gcp', 'stripe', 'twilio',
    'zapier', 'segment', 'amplitude', 'mixpanel', 'datadog', 'splunk',
    'snowflake', 'databricks', 'fivetran', 'airbyte', 'dbt',
}


# Singleton instance
config = Config()
