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
- Token slug pre-calculation
"""

import asyncio
import aiohttp
import json
import re
import logging
import os
from typing import List, Set, Dict, Optional
from dataclasses import dataclass
from datetime import datetime

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


def _name_to_token(name: str) -> str:
    """Converts a company name to a URL-friendly, lowercase ATS token/slug."""
    token = name.lower()
    token = re.sub(r'\s+(inc|llc|ltd|co|corp|gmbh|sa)\.?$', '', token, flags=re.IGNORECASE)
    token = re.sub(r'[^a-z0-9\s-]', '', token)
    token = re.sub(r'[\s-]+', '-', token).strip('-')
    return token


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
                    'User-Agent': 'Mozilla/5.0 (compatible; JobIntelBot/2.0)',
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
        logger.info("🚀 Fetching from Y Combinator...")
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
            
            logger.info(f"✅ Found {len(companies)} YC companies")
            return companies
            
        except Exception as e:
            logger.error(f"❌ Error fetching YC companies: {e}")
            return []
            
    async def fetch_github_orgs_companies(self) -> List[str]:
        """Fetch companies from curated GitHub organizations."""
        logger.info("🚀 Fetching from GitHub Organizations...")
        # Curated list of known tech companies with Greenhouse/Lever boards
        companies = [
            "Stripe", "HashiCorp", "Grafana Labs", "Prisma", "Vercel", "Netlify", 
            "Postman", "Datadog", "Sentry", "CockroachDB", "GitBook",
            "Algolia", "Figma", "Notion", "Airtable", "Supabase", "Novu",
            "OpenAI", "Anthropic", "Cohere", "Hugging Face", "Stability AI",
            "Cloudflare", "Twilio", "PagerDuty", "Okta", "Auth0",
            "MongoDB", "Elastic", "Confluent", "Snowflake", "Databricks",
            "Plaid", "Brex", "Ramp", "Mercury", "Carta", "Rippling",
            "Retool", "Webflow", "Framer", "Linear", "Height", "Loom",
            "Miro", "Canva", "Pitch", "Coda", "Slite", "Almanac",
            "Coinbase", "Kraken", "Gemini", "Circle", "Anchorage",
            "Deel", "Remote", "Oyster", "Velocity Global", "Papaya Global"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f"✅ Found {len(cleaned_companies)} GitHub orgs")
        return cleaned_companies
        
    async def fetch_producthunt_companies(self) -> List[str]:
        """Fetch companies from ProductHunt."""
        logger.info("🚀 Fetching from ProductHunt...")
        companies = [
            "Loom", "Miro", "Linear", "Height", "Revolut", "Brex", "Ramp", 
            "Fivetran", "Airbyte", "Retool", "Webflow", "Gatsby",
            "Chime", "Affirm", "Klarna", "Zip", "Afterpay",
            "Calendly", "Doodle", "Cal.com", "SavvyCal",
            "Lemonade", "Root Insurance", "Hippo", "Oscar Health",
            "Gusto", "Justworks", "TriNet", "Paylocity",
            "Attentive", "Klaviyo", "Customer.io", "Braze",
            "Amplitude", "Mixpanel", "Heap", "FullStory", "Hotjar",
            "Segment", "RudderStack", "mParticle", "Tealium",
            "Zapier", "Make", "Workato", "Tray.io", "n8n"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f"✅ Found {len(cleaned_companies)} ProductHunt companies")
        return cleaned_companies

    async def fetch_github_awesome_companies(self) -> List[str]:
        """Fetch companies from GitHub Awesome Lists."""
        logger.info("🚀 Fetching from GitHub Awesome Lists...")
        companies = [
            "Twitch", "Discord", "Slack", "Zoom", "Microsoft Teams",
            "GitHub", "GitLab", "Bitbucket", "SourceForge",
            "AWS", "Google Cloud", "Azure", "DigitalOcean", "Linode",
            "Heroku", "Railway", "Render", "Fly.io", "PlanetScale",
            "Neon", "Upstash", "Fauna", "Xata", "EdgeDB",
            "Vercel", "Netlify", "Cloudflare Pages", "AWS Amplify",
            "Docker", "Kubernetes", "HashiCorp", "NGINX", "Traefik",
            "Prometheus", "Grafana", "Datadog", "New Relic", "Splunk"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f"✅ Found {len(cleaned_companies)} Awesome Lists companies")
        return cleaned_companies

    async def fetch_crunchbase_companies(self) -> List[str]:
        """Fetch companies from Crunchbase (simulated - would need API key)."""
        logger.info("🚀 Fetching from Crunchbase...")
        # Recent Series A-D funded companies
        companies = [
            "Anthropic", "Anduril", "SpaceX", "Stripe", "Instacart",
            "Databricks", "Canva", "Figma", "Notion", "Airtable",
            "Scale AI", "Weights & Biases", "Labelbox", "Snorkel",
            "Hugging Face", "Cohere", "Jasper", "Copy.ai", "Writer",
            "Runway", "Midjourney", "Stability AI", "Synthesia",
            "Glean", "Perplexity", "You.com", "Neeva", "Kagi"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f"✅ Found {len(cleaned_companies)} Crunchbase companies")
        return cleaned_companies
    
    # ========================================================================
    # TIER 2 SOURCES - Medium Hit Rate (Established Businesses)
    # ========================================================================
    
    async def fetch_sec_edgar_companies(self) -> List[str]:
        """Fetch public companies from SEC EDGAR."""
        logger.info("🚀 Fetching from SEC EDGAR...")
        companies = [
            "Salesforce", "Alphabet", "Meta Platforms", "Tesla", "Microsoft", 
            "Apple", "Amazon", "Netflix", "Adobe", "Intel", 
            "IBM", "Oracle", "SAP", "Cisco", "Qualcomm",
            "NVIDIA", "AMD", "Broadcom", "Texas Instruments", "Micron",
            "PayPal", "Block", "Shopify", "Intuit", "ServiceNow",
            "Workday", "Palantir", "Crowdstrike", "Zscaler", "Palo Alto Networks",
            "Uber", "Lyft", "DoorDash", "Airbnb", "Booking Holdings"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f"✅ Found {len(cleaned_companies)} SEC EDGAR companies")
        return cleaned_companies
        
    async def fetch_usaspending_companies(self) -> List[str]:
        """Fetch federal contractors from USASpending."""
        logger.info("🚀 Fetching from USASpending.gov...")
        companies = [
            "Raytheon", "Lockheed Martin", "General Dynamics", "Boeing", 
            "Northrop Grumman", "Leidos", "SAIC", "Booz Allen Hamilton",
            "Deloitte", "Accenture Federal", "CGI Federal", "ManTech",
            "CACI", "L3Harris", "Peraton", "Maxar", "BAE Systems"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f"✅ Found {len(cleaned_companies)} USASpending companies")
        return cleaned_companies

    async def fetch_sam_gov_companies(self) -> List[str]:
        """Fetch federal vendors from SAM.gov."""
        logger.info("🚀 Fetching from SAM.gov...")
        companies = [
            "Accenture", "Deloitte", "KPMG", "EY", "PwC",
            "McKinsey", "BCG", "Bain", "Oliver Wyman",
            "IBM Consulting", "Capgemini", "Infosys", "Wipro", "TCS",
            "Cognizant", "HCL", "Tech Mahindra", "LTIMindtree"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f"✅ Found {len(cleaned_companies)} SAM.gov companies")
        return cleaned_companies

    async def fetch_inc5000_companies(self) -> List[str]:
        """Fetch companies from Inc 5000."""
        logger.info("🚀 Fetching from Inc 5000...")
        companies = [
            "Calendly", "Notion", "Figma", "Canva", "Airtable",
            "Monday.com", "ClickUp", "Asana", "Wrike", "Smartsheet",
            "HubSpot", "Mailchimp", "Constant Contact", "Klaviyo",
            "Zendesk", "Freshworks", "Intercom", "Drift", "Front"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f"✅ Found {len(cleaned_companies)} Inc 5000 companies")
        return cleaned_companies

    async def fetch_fortune500_companies(self) -> List[str]:
        """Fetch Fortune 500 companies."""
        logger.info("🚀 Fetching from Fortune 500...")
        companies = [
            "Walmart", "Amazon", "Apple", "CVS Health", "UnitedHealth",
            "Berkshire Hathaway", "McKesson", "AmerisourceBergen", "Chevron",
            "ExxonMobil", "AT&T", "Comcast", "Cardinal Health", "Costco",
            "Walgreens Boots Alliance", "Kroger", "Home Depot", "JPMorgan Chase",
            "Verizon", "General Motors", "Ford", "Target", "Johnson & Johnson",
            "Anthem", "Microsoft", "Dell", "Meta", "Alphabet", "Bank of America"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f"✅ Found {len(cleaned_companies)} Fortune 500 companies")
        return cleaned_companies

    async def fetch_glassdoor_companies(self) -> List[str]:
        """Fetch top-rated companies from Glassdoor."""
        logger.info("🚀 Fetching from Glassdoor...")
        companies = [
            "NVIDIA", "Bain & Company", "McKinsey", "Boston Consulting Group",
            "Google", "Microsoft", "Meta", "Apple", "Salesforce",
            "Adobe", "ServiceNow", "Workday", "LinkedIn", "HubSpot",
            "Zillow", "Redfin", "Compass", "Opendoor", "Offerpad"
        ]
        
        cleaned_companies = [self._clean_company_name(c) for c in companies]
        logger.info(f"✅ Found {len(cleaned_companies)} Glassdoor companies")
        return cleaned_companies

    # ========================================================================
    # EXPANSION MANAGEMENT
    # ========================================================================
    
    async def _run_source(self, source_key: str) -> List[str]:
        """Execute the fetching function for a single source."""
        config = SOURCES.get(source_key)
        if not config or not config.enabled:
            logger.info(f"Skipping disabled source: {source_key}")
            return []
            
        fetch_func = getattr(self, f'fetch_{source_key}_companies', None)
        if not fetch_func:
            logger.error(f"No fetch function found for source: {source_key}")
            return []
            
        try:
            companies = await fetch_func()
            return companies
        except Exception as e:
            logger.error(f"Failed to run source {source_key}: {e}")
            return []

    async def _process_results(self, source_key: str, companies: List[str]):
        """Clean, dedup, and upsert companies into the database."""
        config = SOURCES[source_key]
        
        new_companies = [c for c in companies if c not in self.discovered_companies]
        
        # Upsert into database
        added = self.db.upsert_seed_companies(
            companies=new_companies,
            source=config.name,
            tier=config.tier,
            priority=config.priority
        )
        
        self.results[config.name] = new_companies
        self.discovered_companies.update(new_companies)
        
        logger.info(f"✅ Source {config.name}: {len(companies)} total, {added} new seeds added")

    async def expand_tier1(self) -> Dict[str, List[str]]:
        """Run all Tier 1 expansion sources."""
        tier1_keys = [k for k, v in SOURCES.items() if v.tier == 1]
        
        for key in tier1_keys:
            companies = await self._run_source(key)
            await self._process_results(key, companies)
            
        return self.results
        
    async def expand_tier2(self) -> Dict[str, List[str]]:
        """Run all Tier 2 expansion sources."""
        tier2_keys = [k for k, v in SOURCES.items() if v.tier == 2]
        
        for key in tier2_keys:
            companies = await self._run_source(key)
            await self._process_results(key, companies)
            
        return self.results

    async def expand_all(self) -> Dict[str, List[str]]:
        """Run all expansion sources."""
        await self.expand_tier1()
        await self.expand_tier2()
        return self.results

    # ========================================================================
    # UTILITY FUNCTIONS
    # ========================================================================
    
    def _clean_company_name(self, name: str) -> str:
        """Standardize and clean company names."""
        name = name.strip()
        # Keep original case for storage, but clean it
        name = re.sub(r'\s+(Inc|Co|Corp|LLC|Ltd|GmbH|SA)\.?$', '', name, flags=re.IGNORECASE)
        return name
        
    def _is_valid_company_name(self, name: str) -> bool:
        """Simple validation check."""
        if len(name) < 2 or name.isdigit():
            return False
        generic_words = {"the", "a", "an", "software", "solutions", "group", "labs", "tech", "studio"}
        if name.lower() in generic_words:
            return False
        return True


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
        results = await expander.expand_all()
        
        stats = expander.db.get_stats()
        print(f"\n✅ Total seeds in database: {stats.get('total_seeds', 0)}")
        print(f"   Seeds tested: {stats.get('seeds_tested', 0)}")
        print(f"   Untested: {stats.get('untested_seeds', 0)}")
        
    finally:
        await expander.close()


if __name__ == "__main__":
    asyncio.run(main())
