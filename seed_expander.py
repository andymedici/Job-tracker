"""
Seed Token Expander
====================
Discovers new company names from multiple reliable sources to expand
the seed token database for job board discovery.

Sources:
1. Y Combinator Company Directory (public API)
2. GitHub awesome lists (static markdown)
3. Public company databases
4. Fortune/Inc lists (where available)
5. VC portfolio pages
6. Industry-specific lists

Uses PostgreSQL for Railway deployment.
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
class CompanySource:
    """Represents a source for company discovery."""
    name: str
    url: str
    source_type: str  # 'api', 'github_raw', 'html'
    parser: str  # Parser function name


class SeedExpander:
    """Expands seed tokens from multiple sources."""
    
    def __init__(self, db: Database = None):
        self.db = db or get_db()
        self.session: Optional[aiohttp.ClientSession] = None
        self.discovered_companies: Set[str] = set()
    
    async def get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={'User-Agent': 'SeedExpander/1.0'}
            )
        return self.session
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    # ==================== Y COMBINATOR ====================
    
    async def fetch_yc_companies(self) -> List[str]:
        """Fetch companies from Y Combinator's public API."""
        logger.info("Fetching Y Combinator companies...")
        companies = []
        
        try:
            session = await self.get_session()
            
            # YC has a public algolia-powered API
            url = "https://45bwzj1sgc-dsn.algolia.net/1/indexes/*/queries"
            
            headers = {
                'x-algolia-api-key': 'NDYzYmNmMTRjYzU3YTY1MTNlMzgwMzY5NGIwNmNkMTNkNjE2NjE1NTQ5OGY4NjkwMmZhNzRkZjVjYTViZDY1N3Jlc3RyaWN0SW5kaWNlcz1ZQ0NvbXBhbnlfcHJvZHVjdGlvbiZ0YWdGaWx0ZXJzPSU1QiUyMnljZGNfcHVibGljJTIyJTVE',
                'x-algolia-application-id': '45BWZJ1SGC',
                'Content-Type': 'application/json'
            }
            
            # Fetch multiple batches
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
                        if name and len(name) >= 2:
                            companies.append(name)
                            self.discovered_companies.add(name.lower())
                
                await asyncio.sleep(0.1)  # Rate limit
            
            logger.info(f"Found {len(companies)} YC companies")
            
        except Exception as e:
            logger.error(f"Error fetching YC companies: {e}")
        
        return companies
    
    # ==================== GITHUB AWESOME LISTS ====================
    
    async def fetch_github_list(self, repo: str, file_path: str) -> List[str]:
        """Fetch company names from a GitHub markdown file."""
        companies = []
        
        try:
            session = await self.get_session()
            url = f"https://raw.githubusercontent.com/{repo}/master/{file_path}"
            
            async with session.get(url) as resp:
                if resp.status != 200:
                    # Try main branch
                    url = f"https://raw.githubusercontent.com/{repo}/main/{file_path}"
                    async with session.get(url) as resp2:
                        if resp2.status != 200:
                            return []
                        text = await resp2.text()
                else:
                    text = await resp.text()
            
            # Parse markdown for company names
            # Look for links and list items
            patterns = [
                r'\[([^\]]+)\]\(https?://[^\)]+\)',  # [Company](url)
                r'^\s*[-*]\s+\*?\*?([A-Z][A-Za-z0-9\s&\.]+)',  # - Company or * Company
                r'\|\s*\[?([A-Z][A-Za-z0-9\s&\.]+)\]?\s*\|',  # | Company |
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, text, re.MULTILINE)
                for match in matches:
                    name = match.strip()
                    if self._is_valid_company_name(name):
                        companies.append(name)
                        self.discovered_companies.add(name.lower())
            
        except Exception as e:
            logger.error(f"Error fetching GitHub list {repo}: {e}")
        
        return companies
    
    async def fetch_all_github_lists(self) -> List[str]:
        """Fetch from multiple GitHub awesome lists."""
        logger.info("Fetching GitHub awesome lists...")
        
        sources = [
            ("poteto/hiring-without-whiteboards", "README.md"),
            ("remoteintech/remote-jobs", "README.md"),
            ("lukasz-madon/awesome-remote-job", "README.md"),
            ("tramcar/awesome-job-boards", "README.md"),
            ("j-delaney/easy-application", "README.md"),
            ("Kapeli/Dash-User-Contributions", "README.md"),  # Tech companies
            ("sdmg15/Best-websites-a-programmer-should-visit", "README.md"),
        ]
        
        all_companies = []
        
        for repo, file_path in sources:
            companies = await self.fetch_github_list(repo, file_path)
            all_companies.extend(companies)
            logger.info(f"  {repo}: {len(companies)} companies")
            await asyncio.sleep(0.5)
        
        return list(set(all_companies))
    
    # ==================== TECH COMPANY LISTS ====================
    
    def get_fortune_500_tech(self) -> List[str]:
        """Get Fortune 500 tech companies (curated list)."""
        return [
            # Top 100 Tech from Fortune 500
            "Apple", "Amazon", "Alphabet", "Microsoft", "Meta", "Tesla",
            "Nvidia", "Broadcom", "Oracle", "Salesforce", "Adobe", "Netflix",
            "Intel", "Cisco", "IBM", "Qualcomm", "AMD", "Texas Instruments",
            "Applied Materials", "Lam Research", "Micron", "Western Digital",
            "Seagate", "Dell Technologies", "HP Inc", "HPE", "Lenovo",
            "Accenture", "Cognizant", "Infosys", "Wipro", "TCS",
            "ServiceNow", "Workday", "Splunk", "VMware", "Citrix",
            "Palo Alto Networks", "Fortinet", "CrowdStrike", "Zscaler",
            "Cloudflare", "Fastly", "Akamai", "F5 Networks",
            "Autodesk", "Ansys", "Synopsys", "Cadence", "PTC",
            "Intuit", "PayPal", "Block", "Fiserv", "Fidelity National",
            "Global Payments", "Visa", "Mastercard", "American Express",
            "Uber", "Lyft", "DoorDash", "Instacart", "Grubhub",
            "Airbnb", "Booking Holdings", "Expedia", "TripAdvisor",
            "eBay", "Etsy", "Wayfair", "Chewy", "Carvana",
            "Zoom", "DocuSign", "Dropbox", "Box", "Slack",
            "Atlassian", "MongoDB", "Elastic", "Confluent", "Snowflake",
            "Databricks", "Palantir", "UiPath", "C3.ai",
            "Twilio", "Okta", "Auth0", "Ping Identity",
            "CrowdStrike", "SentinelOne", "Tanium", "Carbon Black",
        ]
    
    def get_unicorns(self) -> List[str]:
        """Get known unicorn companies."""
        return [
            # AI/ML Unicorns
            "OpenAI", "Anthropic", "Cohere", "Inflection AI", "Adept",
            "Stability AI", "Midjourney", "Jasper", "Writer", "Copy.ai",
            "Hugging Face", "Scale AI", "Labelbox", "Snorkel AI",
            "Weights & Biases", "Determined AI", "Anyscale", "Modal",
            "Runway", "Descript", "Synthesia", "HeyGen",
            
            # Fintech Unicorns
            "Stripe", "Plaid", "Brex", "Ramp", "Mercury", "Jeeves",
            "Chime", "Current", "Dave", "MoneyLion", "Varo",
            "Robinhood", "Webull", "Public", "Alpaca",
            "Coinbase", "Kraken", "Gemini", "Circle", "Paxos",
            "Affirm", "Klarna", "Afterpay", "Sezzle", "Zip",
            "Marqeta", "Lithic", "Unit", "Treasury Prime",
            "Plaid", "Finicity", "Yodlee", "MX",
            
            # Enterprise Unicorns
            "Notion", "Coda", "Airtable", "Monday.com", "ClickUp",
            "Figma", "Canva", "Miro", "Loom", "Pitch",
            "Linear", "Height", "Shortcut", "Productboard",
            "Amplitude", "Mixpanel", "Heap", "PostHog", "June",
            "Segment", "mParticle", "RudderStack", "Hightouch",
            "dbt Labs", "Fivetran", "Airbyte", "Stitch",
            "Snowflake", "Databricks", "Clickhouse", "StarRocks",
            "Retool", "Internal", "Airplane", "Superblocks",
            "Vercel", "Netlify", "Render", "Railway", "Fly.io",
            
            # Security Unicorns
            "Snyk", "Lacework", "Orca Security", "Wiz", "Aqua Security",
            "Cybereason", "Vectra AI", "Exabeam", "Sumo Logic",
            "1Password", "Bitwarden", "Dashlane", "LastPass",
            "Vanta", "Drata", "Secureframe", "Laika",
            
            # HR/Recruiting Unicorns
            "Rippling", "Deel", "Remote", "Oyster", "Papaya Global",
            "Lattice", "Culture Amp", "15Five", "Leapsome",
            "Greenhouse", "Lever", "Ashby", "Gem",
            "Gusto", "Justworks", "TriNet", "Namely",
            
            # Dev Tools Unicorns
            "GitHub", "GitLab", "Bitbucket", "Sourcegraph",
            "CircleCI", "Buildkite", "Harness", "Codefresh",
            "LaunchDarkly", "Split", "Statsig", "Eppo",
            "Sentry", "Datadog", "New Relic", "Dynatrace",
            "PagerDuty", "OpsGenie", "Incident.io", "FireHydrant",
            "Postman", "Insomnia", "Hoppscotch",
            "Supabase", "PlanetScale", "Neon", "CockroachDB",
            
            # Healthcare Unicorns  
            "Oscar Health", "Clover Health", "Devoted Health",
            "Ro", "Hims & Hers", "Nurx", "SimpleHealth",
            "Cerebral", "Lyra Health", "Spring Health", "Headspace",
            "Calm", "Talkspace", "BetterHelp",
            "Tempus", "Flatiron Health", "Veracyte",
            "Carbon Health", "One Medical", "Forward", "Parsley Health",
            
            # E-commerce/Retail Unicorns
            "Shopify", "BigCommerce", "Webflow", "Squarespace",
            "Faire", "Bulletin", "Abound",
            "Bolt", "Fast", "Catch",
            "Attentive", "Klaviyo", "Postscript", "Sendlane",
            "Gorgias", "Gladly", "Kustomer",
            "Yotpo", "Stamped", "Okendo", "Junip",
            
            # Logistics Unicorns
            "Flexport", "Shippo", "EasyPost", "ShipBob",
            "Convoy", "Uber Freight", "Loadsmart",
            "project44", "FourKites", "Samsara",
            
            # Real Estate Unicorns
            "Opendoor", "Offerpad", "Knock", "Orchard",
            "Divvy", "Landis", "Ribbon", "Accept.inc",
            "Loft", "QuintoAndar", "Compass",
            
            # EdTech Unicorns
            "Coursera", "Udemy", "Skillshare", "MasterClass",
            "Duolingo", "Babbel", "Busuu",
            "Chegg", "Course Hero", "Quizlet", "Brainly",
            "Lambda School", "Springboard", "Thinkful",
            "Guild Education", "InStride", "Degreed",
            
            # Gaming Unicorns
            "Unity", "Epic Games", "Roblox", "Niantic",
            "Discord", "Guilded",
            "FaZe Clan", "100 Thieves", "TSM",
            
            # Climate/Energy Unicorns
            "Stripe Climate", "Watershed", "Persefoni",
            "Arcadia", "OhmConnect", "Sense",
            "ChargePoint", "EVgo", "Electrify America",
            "Redwood Materials", "Li-Cycle", "Battery Resources",
        ]
    
    def get_yc_top_companies(self) -> List[str]:
        """Get YC's most successful companies."""
        return [
            # YC Top 100
            "Airbnb", "Stripe", "Instacart", "DoorDash", "Coinbase",
            "Dropbox", "Reddit", "Twitch", "Cruise", "GitLab",
            "Zapier", "Gusto", "Faire", "Brex", "Flexport",
            "Checkr", "Segment", "Amplitude", "Webflow", "Figma",
            "Notion", "Linear", "Retool", "Vercel", "Railway",
            "PostHog", "Supabase", "Airplane", "Render",
            "Cal.com", "Clerk", "Resend", "Trigger.dev",
            "Liveblocks", "Tinybird", "Lago", "Polar",
            "Langchain", "LlamaIndex", "Weights & Biases",
            "Scale AI", "Labelbox", "Snorkel",
            "OpenSea", "Alchemy", "QuickNode", "Tenderly",
            "Algolia", "Meilisearch", "Typesense",
            "Sentry", "LogRocket", "FullStory",
            "Heap", "June", "Koala",
            "Mixpanel", "Amplitude", "PostHog",
            "Customer.io", "Vero", "Courier",
            "Nylas", "Merge", "Finch",
            "Plaid", "Unit", "Treasury Prime",
            "Ramp", "Brex", "Mercury",
            "Deel", "Remote", "Oyster",
            "Ashby", "Gem", "Dover",
            "Lattice", "Leapsome", "Culture Amp",
            "Vanta", "Drata", "Secureframe",
            "Snyk", "Semgrep", "Socket",
            "1Password", "Tailscale", "Teleport",
            "Doppler", "Infisical", "HashiCorp",
            "Terraform", "Vault", "Consul",
            "Docker", "Podman", "containerd",
        ]
    
    def get_series_a_b_companies(self) -> List[str]:
        """Get recent Series A/B funded companies."""
        return [
            # Recent Series A/B (2023-2024)
            "Anthropic", "Mistral AI", "Cohere", "Adept", "Inflection",
            "Perplexity", "You.com", "Neeva", "Kagi",
            "Character.AI", "Replika", "Inworld AI",
            "Runway ML", "Pika Labs", "Genmo",
            "ElevenLabs", "Resemble AI", "WellSaid",
            "Glean", "Hebbia", "Vectara",
            "Pinecone", "Weaviate", "Qdrant", "Chroma",
            "Modal", "Baseten", "Banana", "Replicate",
            "Humanloop", "PromptLayer", "Helicone",
            "Orb", "Metronome", "Lago", "Stigg",
            "WorkOS", "Stytch", "Descope",
            "Propel", "Method", "Argyle",
            "Sardine", "Unit21", "Alloy",
            "Persona", "Socure", "Jumio",
            "Coder", "Gitpod", "Codespaces",
            "Warp", "Fig", "Charm",
            "Raycast", "Linear", "Height",
            "Campsite", "Loom", "Grain",
            "Fireflies", "Otter", "Fathom",
            "Krisp", "Airgram", "tl;dv",
            "Sprig", "Maze", "UserTesting",
            "Dovetail", "Notably", "Marvin",
            "Census", "Hightouch", "Polytomic",
            "Monte Carlo", "Bigeye", "Soda",
            "Atlan", "Alation", "Collibra",
            "dbt Labs", "Transform", "MetricFlow",
            "Preset", "Lightdash", "Evidence",
            "Metabase", "Redash", "Apache Superset",
            "Equals", "Causal", "Omni",
            "Eppo", "Statsig", "LaunchDarkly",
            "Growthbook", "DevCycle", "Unleash",
            "incident.io", "Rootly", "FireHydrant",
            "Cortex", "OpsLevel", "Rely",
            "Backstage", "Port", "Configure8",
        ]
    
    # ==================== CRUNCHBASE DATA ====================
    
    async def fetch_crunchbase_trending(self) -> List[str]:
        """Attempt to get trending companies from Crunchbase (limited without API)."""
        # Note: Full Crunchbase API requires paid access
        # This uses their public trending data where available
        
        # Fallback to curated list of recently funded companies
        return self.get_series_a_b_companies()
    
    # ==================== MAIN EXPANSION LOGIC ====================
    
    def _is_valid_company_name(self, name: str) -> bool:
        """Check if string is likely a valid company name."""
        if not name or len(name) < 2 or len(name) > 100:
            return False
        
        # Skip common false positives
        skip_patterns = [
            r'^(the|a|an|and|or|of|in|to|for|on|with|by|at|from)$',
            r'^(remote|hybrid|onsite|full-time|part-time|contract)$',
            r'^(engineering|design|product|sales|marketing|hr)$',
            r'^(senior|junior|lead|staff|principal|manager|director)$',
            r'^\d+$',
            r'^(http|www|mailto|github|linkedin|twitter)',
            r'^\W+$',
        ]
        
        name_lower = name.lower().strip()
        for pattern in skip_patterns:
            if re.match(pattern, name_lower):
                return False
        
        # Must start with letter or number
        if not re.match(r'^[A-Za-z0-9]', name):
            return False
        
        return True
    
    def _clean_company_name(self, name: str) -> str:
        """Clean and normalize company name."""
        # Remove common suffixes
        suffixes = [
            ' Inc.', ' Inc', ' LLC', ' Corp.', ' Corp', ' Ltd.', ' Ltd',
            ' Co.', ' Co', ' Technologies', ' Technology', ' Tech',
            ' Software', ' Systems', ' Solutions', ' Services',
            ' Group', ' Holdings', ' Ventures', ' Partners',
            ', Inc.', ', LLC', ', Corp.'
        ]
        
        cleaned = name.strip()
        for suffix in suffixes:
            if cleaned.endswith(suffix):
                cleaned = cleaned[:-len(suffix)].strip()
        
        return cleaned
    
    async def expand_all(self) -> Dict[str, List[str]]:
        """Run all expansion sources and return discovered companies."""
        results = {}
        
        # 1. Y Combinator API
        yc_companies = await self.fetch_yc_companies()
        results['yc'] = yc_companies
        
        # 2. GitHub lists
        github_companies = await self.fetch_all_github_lists()
        results['github'] = github_companies
        
        # 3. Fortune 500 Tech
        fortune_companies = self.get_fortune_500_tech()
        results['fortune'] = fortune_companies
        
        # 4. Unicorns
        unicorn_companies = self.get_unicorns()
        results['unicorns'] = unicorn_companies
        
        # 5. YC Top Companies
        yc_top = self.get_yc_top_companies()
        results['yc_top'] = yc_top
        
        # 6. Series A/B Companies
        series_ab = self.get_series_a_b_companies()
        results['series_ab'] = series_ab
        
        # Deduplicate and clean
        all_companies = set()
        for source, companies in results.items():
            for company in companies:
                cleaned = self._clean_company_name(company)
                if self._is_valid_company_name(cleaned):
                    all_companies.add(cleaned)
        
        results['total_unique'] = list(all_companies)
        
        logger.info(f"\n📊 Expansion Results:")
        logger.info(f"  YC API: {len(results['yc'])} companies")
        logger.info(f"  GitHub Lists: {len(results['github'])} companies")
        logger.info(f"  Fortune 500: {len(results['fortune'])} companies")
        logger.info(f"  Unicorns: {len(results['unicorns'])} companies")
        logger.info(f"  YC Top: {len(results['yc_top'])} companies")
        logger.info(f"  Series A/B: {len(results['series_ab'])} companies")
        logger.info(f"  ───────────────────────")
        logger.info(f"  Total Unique: {len(all_companies)} companies")
        
        return results
    
    def save_to_db(self, companies: List[str], source: str):
        """Save discovered companies to database."""
        added = self.db.save_seed_companies(companies, source)
        logger.info(f"Saved {added} new companies from {source}")
        return added
    
    def get_all_seeds(self) -> List[str]:
        """Get all seed companies from database."""
        return self.db.get_seed_companies(limit=5000)
    
    def export_seeds(self, output_file: str = "seeds.txt"):
        """Export all seeds to a text file."""
        companies = self.get_all_seeds()
        
        with open(output_file, 'w') as f:
            for company in sorted(companies):
                f.write(f"{company}\n")
        
        logger.info(f"Exported {len(companies)} companies to {output_file}")


async def main():
    """Run the seed expander."""
    expander = SeedExpander()
    
    try:
        # Expand from all sources
        results = await expander.expand_all()
        
        # Save to database
        for source, companies in results.items():
            if source != 'total_unique' and companies:
                expander.save_to_db(companies, source)
        
        # Export to file
        expander.export_seeds('expanded_seeds.txt')
        
        # Print summary
        all_seeds = expander.get_all_seeds()
        print(f"\n✅ Total seeds available: {len(all_seeds)}")
        
    finally:
        await expander.close()


if __name__ == "__main__":
    asyncio.run(main())
