"""
Self-Growth Intelligence System
================================
Automatically discovers new companies from:
1. Job description text mining (partners, integrations, competitors)
2. Company website crawling (customers, partners, testimonials)
3. News/funding monitoring
4. Industry/geographic clustering
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
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class DiscoveredCompany:
    """A company discovered through self-growth"""
    name: str
    source_company: str  # Which tracked company led to discovery
    discovery_type: str  # 'partner', 'customer', 'competitor', 'integration', 'news', 'similar'
    confidence: float  # 0.0 to 1.0
    context: str  # Why we think this is a company
    discovered_at: datetime = field(default_factory=datetime.now)
    url: Optional[str] = None
    metadata: Dict = field(default_factory=dict)

@dataclass
class GrowthStats:
    """Statistics for a growth analysis run"""
    companies_analyzed: int = 0
    discoveries_from_jobs: int = 0
    discoveries_from_websites: int = 0
    discoveries_from_news: int = 0
    total_discoveries: int = 0
    high_confidence: int = 0  # >= 0.7
    promoted_to_seeds: int = 0
    duration_seconds: float = 0


# =============================================================================
# KNOWN INTEGRATIONS (Filter out - not company seeds)
# =============================================================================

KNOWN_INTEGRATIONS = {
    # Cloud Providers
    'aws', 'amazon web services', 'azure', 'google cloud', 'gcp', 'digitalocean',
    'heroku', 'vercel', 'netlify', 'cloudflare', 'fastly',
    
    # Dev Tools (too common)
    'github', 'gitlab', 'bitbucket', 'jira', 'confluence', 'notion', 'slack',
    'discord', 'teams', 'zoom', 'figma', 'miro', 'linear', 'asana',
    
    # Databases
    'postgresql', 'postgres', 'mysql', 'mongodb', 'redis', 'elasticsearch',
    'dynamodb', 'firebase', 'supabase', 'planetscale',
    
    # Auth/Identity
    'okta', 'auth0', 'onelogin', 'google sso', 'saml', 'oauth',
    
    # Analytics
    'google analytics', 'mixpanel', 'amplitude', 'segment', 'heap',
    'datadog', 'new relic', 'sentry', 'pagerduty',
    
    # Payment
    'stripe', 'paypal', 'braintree', 'square', 'plaid',
    
    # CRM/Marketing
    'salesforce', 'hubspot', 'marketo', 'mailchimp', 'sendgrid', 'twilio',
    
    # Generic terms
    'api', 'sdk', 'rest', 'graphql', 'webhook', 'integration', 'plugin',
}


# =============================================================================
# JOB DESCRIPTION MINING
# =============================================================================

class JobDescriptionMiner:
    """Extract company mentions from job descriptions"""
    
    # Patterns that often precede company names
    EXTRACTION_PATTERNS = [
        # Partners
        (r'(?:partner(?:ing|ed|s)?\s+with|strategic\s+partner(?:s)?)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3})', 'partner', 0.7),
        
        # Integrations
        (r'(?:integrate(?:s|d)?\s+with|built\s+on|powered\s+by)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,2})', 'integration', 0.6),
        
        # Customers
        (r'(?:customers?\s+include|clients?\s+include|serving|trusted\s+by|used\s+by)\s+([A-Z][A-Za-z0-9]+(?:,?\s+(?:and\s+)?[A-Z][A-Za-z0-9]+)*)', 'customer', 0.6),
        
        # Competitors
        (r'(?:competitor(?:s)?|alternative(?:s)?\s+to|compared\s+to|better\s+than)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,2})', 'competitor', 0.75),
        
        # Acquired/Owned
        (r'(?:acquired\s+by|owned\s+by|subsidiary\s+of|part\s+of|division\s+of)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,2})', 'parent', 0.85),
        
        # Experience requirements mentioning companies
        (r'(?:experience\s+(?:at|with)|worked\s+at|background\s+(?:at|in))\s+([A-Z][A-Za-z0-9]+(?:,?\s+(?:or\s+)?[A-Z][A-Za-z0-9]+)*)', 'industry_peer', 0.5),
    ]
    
    @classmethod
    def extract_companies(cls, text: str, source_company: str) -> List[DiscoveredCompany]:
        """Extract potential company names from job description text"""
        discoveries = []
        
        for pattern, discovery_type, base_confidence in cls.EXTRACTION_PATTERNS:
            matches = re.findall(pattern, text, re.IGNORECASE)
            
            for match in matches:
                # Split on commas and 'and'
                parts = re.split(r',\s*|\s+and\s+|\s+or\s+', match)
                
                for part in parts:
                    name = part.strip()
                    
                    # Validate
                    if not cls._is_valid_company(name):
                        continue
                    
                    # Check if it's a known integration
                    if name.lower() in KNOWN_INTEGRATIONS:
                        continue
                    
                    discoveries.append(DiscoveredCompany(
                        name=name,
                        source_company=source_company,
                        discovery_type=discovery_type,
                        confidence=base_confidence,
                        context=f"Found in job description pattern: {discovery_type}",
                    ))
        
        return discoveries
    
    @staticmethod
    def _is_valid_company(name: str) -> bool:
        """Validate potential company name"""
        if not name or len(name) < 2 or len(name) > 50:
            return False
        
        # Must start with capital letter
        if not name[0].isupper():
            return False
        
        # Must have at least 2 letters
        if sum(1 for c in name if c.isalpha()) < 2:
            return False
        
        # Reject common words
        common = {'The', 'A', 'An', 'This', 'That', 'Our', 'Your', 'We', 'They',
                 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday',
                 'January', 'February', 'March', 'April', 'May', 'June',
                 'July', 'August', 'September', 'October', 'November', 'December'}
        if name in common:
            return False
        
        return True


# =============================================================================
# WEBSITE CRAWLER
# =============================================================================

class WebsiteCrawler:
    """Crawl company websites for partners/customers"""
    
    PAGES_TO_CHECK = [
        '/',
        '/customers',
        '/case-studies',
        '/partners',
        '/integrations',
        '/about',
        '/about-us',
        '/company',
    ]
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        }
    
    async def crawl_company(self, company_name: str, website_url: str) -> List[DiscoveredCompany]:
        """Crawl a company's website for partner/customer mentions"""
        discoveries = []
        
        base_url = website_url.rstrip('/')
        
        for page in self.PAGES_TO_CHECK:
            url = f"{base_url}{page}"
            
            try:
                async with self.session.get(url, headers=self.headers, timeout=10) as resp:
                    if resp.status != 200:
                        continue
                    
                    html = await resp.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Extract from different sources
                    discoveries.extend(self._extract_from_logos(soup, company_name, page))
                    discoveries.extend(self._extract_from_links(soup, company_name, page))
                    discoveries.extend(self._extract_from_testimonials(soup, company_name, page))
                    
            except Exception as e:
                logger.debug(f"Error crawling {url}: {e}")
                continue
            
            # Rate limit
            await asyncio.sleep(0.5)
        
        return discoveries
    
    def _extract_from_logos(self, soup: BeautifulSoup, source: str, page: str) -> List[DiscoveredCompany]:
        """Extract company names from logo sections"""
        discoveries = []
        
        # Look for logo sections
        logo_patterns = [
            r'logo', r'customer', r'client', r'partner', r'trusted',
            r'used-by', r'featured', r'as-seen'
        ]
        
        for pattern in logo_patterns:
            sections = soup.find_all(['div', 'section', 'ul'], class_=re.compile(pattern, re.I))
            
            for section in sections:
                # Look for alt text on images
                for img in section.find_all('img', alt=True):
                    alt = img.get('alt', '').strip()
                    if alt and 3 <= len(alt) <= 40:
                        # Clean up common suffixes
                        name = re.sub(r'\s*(logo|icon|image).*$', '', alt, flags=re.I).strip()
                        if name and len(name) >= 3:
                            discoveries.append(DiscoveredCompany(
                                name=name,
                                source_company=source,
                                discovery_type='customer',
                                confidence=0.8,
                                context=f"Logo on {page} page",
                            ))
                
                # Look for title attributes
                for elem in section.find_all(['img', 'a', 'div'], title=True):
                    title = elem.get('title', '').strip()
                    if title and 3 <= len(title) <= 40:
                        discoveries.append(DiscoveredCompany(
                            name=title,
                            source_company=source,
                            discovery_type='customer',
                            confidence=0.75,
                            context=f"Title attribute on {page} page",
                        ))
        
        return discoveries
    
    def _extract_from_links(self, soup: BeautifulSoup, source: str, page: str) -> List[DiscoveredCompany]:
        """Extract partner companies from links"""
        discoveries = []
        
        # Look for partner/integration links
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Skip navigation links
            if len(text) < 3 or len(text) > 40:
                continue
            
            # Check if it's an external company link
            if href.startswith('http') and source.lower() not in href.lower():
                # Check parent containers for context
                parent = link.find_parent(['div', 'section', 'li'])
                if parent:
                    parent_class = ' '.join(parent.get('class', []))
                    if re.search(r'partner|integration|customer', parent_class, re.I):
                        discoveries.append(DiscoveredCompany(
                            name=text,
                            source_company=source,
                            discovery_type='partner',
                            confidence=0.75,
                            context=f"External link on {page} page",
                            url=href,
                        ))
        
        return discoveries
    
    def _extract_from_testimonials(self, soup: BeautifulSoup, source: str, page: str) -> List[DiscoveredCompany]:
        """Extract companies from testimonial attributions"""
        discoveries = []
        
        # Look for testimonial sections
        testimonial_patterns = [r'testimonial', r'quote', r'review', r'case-study']
        
        for pattern in testimonial_patterns:
            sections = soup.find_all(['div', 'section', 'blockquote'], class_=re.compile(pattern, re.I))
            
            for section in sections:
                # Look for company attribution
                # Often in format "Name, Title at Company" or "Company | Role"
                attribution = section.find(['cite', 'span', 'p'], class_=re.compile(r'author|attribution|source|company', re.I))
                
                if attribution:
                    text = attribution.get_text(strip=True)
                    # Try to extract company name
                    patterns = [
                        r'(?:at|@)\s+([A-Z][A-Za-z0-9\s&]+)',
                        r'\|\s*([A-Z][A-Za-z0-9\s&]+)',
                        r',\s*([A-Z][A-Za-z0-9\s&]+)$',
                    ]
                    
                    for pat in patterns:
                        match = re.search(pat, text)
                        if match:
                            name = match.group(1).strip()
                            if 3 <= len(name) <= 40:
                                discoveries.append(DiscoveredCompany(
                                    name=name,
                                    source_company=source,
                                    discovery_type='customer',
                                    confidence=0.85,
                                    context=f"Testimonial attribution on {page} page",
                                ))
                            break
        
        return discoveries


# =============================================================================
# NEWS/FUNDING MONITOR
# =============================================================================

class NewsMonitor:
    """Monitor news for company discoveries"""
    
    NEWS_SOURCES = [
        'https://techcrunch.com/tag/funding/',
        'https://news.crunchbase.com/venture/',
    ]
    
    FUNDING_PATTERNS = [
        r'([A-Z][A-Za-z0-9\s]+)\s+(?:raises?|closes?|secures?|announces?)\s+\$[\d,]+\s*(?:M|B|million|billion)',
        r'\$[\d,]+\s*(?:M|B|million|billion)\s+(?:for|into)\s+([A-Z][A-Za-z0-9\s]+)',
        r'([A-Z][A-Za-z0-9\s]+)\s+(?:Series\s+[A-Z]|seed|funding)',
    ]
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        }
    
    async def check_news(self) -> List[DiscoveredCompany]:
        """Check news sources for new company funding"""
        discoveries = []
        
        for source_url in self.NEWS_SOURCES:
            try:
                async with self.session.get(source_url, headers=self.headers, timeout=15) as resp:
                    if resp.status != 200:
                        continue
                    
                    html = await resp.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Find article headlines
                    for headline in soup.find_all(['h2', 'h3', 'a'], class_=re.compile(r'title|headline|post', re.I)):
                        text = headline.get_text(strip=True)
                        
                        for pattern in self.FUNDING_PATTERNS:
                            match = re.search(pattern, text, re.I)
                            if match:
                                name = match.group(1).strip()
                                # Clean up
                                name = re.sub(r'\s+', ' ', name)
                                if 3 <= len(name) <= 40 and name[0].isupper():
                                    discoveries.append(DiscoveredCompany(
                                        name=name,
                                        source_company='news',
                                        discovery_type='funding',
                                        confidence=0.9,
                                        context=f"Funding news: {text[:100]}",
                                        url=source_url,
                                    ))
                                break
                    
            except Exception as e:
                logger.debug(f"Error checking news {source_url}: {e}")
        
        return discoveries


# =============================================================================
# INDUSTRY CLUSTERING
# =============================================================================

class IndustryClusterer:
    """Cluster companies by industry for targeted expansion"""
    
    INDUSTRY_KEYWORDS = {
        'fintech': ['payment', 'banking', 'lending', 'insurance', 'trading', 'crypto', 'blockchain'],
        'healthtech': ['health', 'medical', 'clinical', 'patient', 'healthcare', 'biotech', 'pharma'],
        'edtech': ['education', 'learning', 'training', 'course', 'student', 'teacher', 'school'],
        'proptech': ['real estate', 'property', 'housing', 'mortgage', 'rental', 'home'],
        'legaltech': ['legal', 'law', 'attorney', 'contract', 'compliance', 'regulatory'],
        'hrtech': ['recruiting', 'hiring', 'hr', 'human resources', 'talent', 'payroll', 'benefits'],
        'martech': ['marketing', 'advertising', 'analytics', 'seo', 'email', 'social media'],
        'devtools': ['developer', 'api', 'infrastructure', 'cloud', 'devops', 'monitoring'],
        'cybersecurity': ['security', 'cyber', 'threat', 'vulnerability', 'encryption', 'identity'],
        'ai_ml': ['artificial intelligence', 'machine learning', 'ai', 'ml', 'deep learning', 'nlp'],
    }
    
    def cluster_companies(self, companies: List[Dict]) -> Dict[str, List[str]]:
        """Group companies by industry based on job titles and descriptions"""
        clusters = defaultdict(list)
        
        for company in companies:
            name = company.get('company_name', '')
            jobs = company.get('jobs', [])
            
            # Aggregate text from jobs
            text = ' '.join([
                job.get('title', '') + ' ' + job.get('description', '')
                for job in jobs
            ]).lower()
            
            # Score each industry
            industry_scores = {}
            for industry, keywords in self.INDUSTRY_KEYWORDS.items():
                score = sum(1 for kw in keywords if kw in text)
                if score > 0:
                    industry_scores[industry] = score
            
            # Assign to top industry
            if industry_scores:
                top_industry = max(industry_scores, key=industry_scores.get)
                clusters[top_industry].append(name)
        
        return dict(clusters)


# =============================================================================
# GEOGRAPHIC CLUSTERING
# =============================================================================

class GeographicClusterer:
    """Identify hot hiring locations"""
    
    def identify_hot_locations(self, companies: List[Dict], min_companies: int = 3) -> Dict[str, List[str]]:
        """Find locations with multiple tracked companies"""
        location_companies = defaultdict(list)
        
        for company in companies:
            name = company.get('company_name', '')
            locations = company.get('locations', [])
            
            for loc in locations:
                # Normalize location
                loc = loc.strip()
                if loc and len(loc) > 2:
                    location_companies[loc].append(name)
        
        # Filter to hot locations
        hot_locations = {
            loc: companies for loc, companies in location_companies.items()
            if len(companies) >= min_companies
        }
        
        return hot_locations


# =============================================================================
# MAIN SELF-GROWTH ENGINE
# =============================================================================

class SelfGrowthEngine:
    """Main engine coordinating all self-growth mechanisms"""
    
    def __init__(self, db_path: str = 'job_intel.db'):
        self.db_path = db_path
        self.discoveries: List[DiscoveredCompany] = []
        self.seen_names: Set[str] = set()
    
    async def run_analysis(self) -> GrowthStats:
        """Run full self-growth analysis"""
        stats = GrowthStats()
        start_time = datetime.now()
        
        # Load tracked companies
        companies = self._load_tracked_companies()
        stats.companies_analyzed = len(companies)
        
        logger.info(f"Analyzing {len(companies)} tracked companies...")
        
        async with aiohttp.ClientSession() as session:
            # 1. Mine job descriptions
            logger.info("Mining job descriptions...")
            for company in companies:
                job_discoveries = self._mine_job_descriptions(company)
                stats.discoveries_from_jobs += len(job_discoveries)
                self._add_discoveries(job_discoveries)
            
            # 2. Crawl websites (sample - too slow for all)
            logger.info("Crawling company websites (sample)...")
            crawler = WebsiteCrawler(session)
            sample_companies = companies[:50]  # Limit to 50
            
            for company in sample_companies:
                # Try to construct website URL
                token = company.get('token', '')
                possible_urls = [
                    f"https://{token}.com",
                    f"https://www.{token}.com",
                ]
                
                for url in possible_urls:
                    try:
                        web_discoveries = await crawler.crawl_company(
                            company.get('company_name', ''),
                            url
                        )
                        stats.discoveries_from_websites += len(web_discoveries)
                        self._add_discoveries(web_discoveries)
                        break
                    except:
                        continue
            
            # 3. Check news
            logger.info("Checking funding news...")
            news_monitor = NewsMonitor(session)
            news_discoveries = await news_monitor.check_news()
            stats.discoveries_from_news += len(news_discoveries)
            self._add_discoveries(news_discoveries)
        
        # 4. Cluster for insights
        logger.info("Clustering companies...")
        industry_clusterer = IndustryClusterer()
        geo_clusterer = GeographicClusterer()
        
        industry_clusters = industry_clusterer.cluster_companies(companies)
        geo_clusters = geo_clusterer.identify_hot_locations(companies)
        
        logger.info(f"Found {len(industry_clusters)} industry clusters")
        logger.info(f"Found {len(geo_clusters)} hot locations")
        
        # Calculate stats
        stats.total_discoveries = len(self.discoveries)
        stats.high_confidence = sum(1 for d in self.discoveries if d.confidence >= 0.7)
        stats.duration_seconds = (datetime.now() - start_time).total_seconds()
        
        # Save discoveries
        stats.promoted_to_seeds = self._save_discoveries()
        
        return stats
    
    def _load_tracked_companies(self) -> List[Dict]:
        """Load tracked companies from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT company_name, token, ats_type, departments, locations
            FROM tracked_companies
            WHERE job_count > 0
        ''')
        
        companies = []
        for row in cursor.fetchall():
            companies.append({
                'company_name': row[0],
                'token': row[1],
                'ats_type': row[2],
                'departments': json.loads(row[3]) if row[3] else [],
                'locations': json.loads(row[4]) if row[4] else [],
            })
        
        # Also load existing seed names to avoid duplicates
        cursor.execute('SELECT name FROM seed_companies')
        for row in cursor.fetchall():
            self.seen_names.add(row[0].lower())
        
        conn.close()
        return companies
    
    def _mine_job_descriptions(self, company: Dict) -> List[DiscoveredCompany]:
        """Mine job descriptions for a company"""
        # In a full implementation, we'd have stored job descriptions
        # For now, use department names as proxy
        discoveries = []
        
        company_name = company.get('company_name', '')
        departments = company.get('departments', [])
        
        # Check for industry-specific patterns in department names
        dept_text = ' '.join(departments)
        discoveries.extend(JobDescriptionMiner.extract_companies(dept_text, company_name))
        
        return discoveries
    
    def _add_discoveries(self, new_discoveries: List[DiscoveredCompany]):
        """Add new discoveries, deduplicating"""
        for d in new_discoveries:
            name_lower = d.name.lower()
            if name_lower not in self.seen_names:
                self.seen_names.add(name_lower)
                self.discoveries.append(d)
    
    def _save_discoveries(self) -> int:
        """Save discoveries to database, promoting high-confidence ones to seeds"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create discoveries table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS self_growth_discoveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                source_company TEXT,
                discovery_type TEXT,
                confidence REAL,
                context TEXT,
                url TEXT,
                promoted_to_seed BOOLEAN DEFAULT FALSE,
                discovered_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        promoted = 0
        
        for discovery in self.discoveries:
            # Save discovery
            cursor.execute('''
                INSERT INTO self_growth_discoveries 
                (name, source_company, discovery_type, confidence, context, url, discovered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                discovery.name,
                discovery.source_company,
                discovery.discovery_type,
                discovery.confidence,
                discovery.context,
                discovery.url,
                discovery.discovered_at.isoformat(),
            ))
            
            discovery_id = cursor.lastrowid
            
            # Auto-promote high-confidence discoveries
            if discovery.confidence >= 0.7:
                try:
                    tier = 1 if discovery.confidence >= 0.85 else 2
                    cursor.execute('''
                        INSERT OR IGNORE INTO seed_companies 
                        (name, source, tier, confidence)
                        VALUES (?, ?, ?, ?)
                    ''', (
                        discovery.name,
                        f'self_growth_{discovery.discovery_type}',
                        tier,
                        discovery.confidence,
                    ))
                    
                    if cursor.rowcount > 0:
                        promoted += 1
                        cursor.execute('''
                            UPDATE self_growth_discoveries 
                            SET promoted_to_seed = TRUE 
                            WHERE id = ?
                        ''', (discovery_id,))
                        
                except sqlite3.IntegrityError:
                    pass
        
        conn.commit()
        conn.close()
        
        logger.info(f"Saved {len(self.discoveries)} discoveries, promoted {promoted} to seeds")
        return promoted
    
    def get_discoveries(self, min_confidence: float = 0.0) -> List[DiscoveredCompany]:
        """Get discoveries above minimum confidence"""
        return [d for d in self.discoveries if d.confidence >= min_confidence]
    
    def get_discovery_summary(self) -> Dict:
        """Get summary of discoveries by type"""
        summary = defaultdict(int)
        for d in self.discoveries:
            summary[d.discovery_type] += 1
        return dict(summary)


# =============================================================================
# CLI
# =============================================================================

async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Self-Growth Intelligence System')
    parser.add_argument('--db', default='job_intel.db', help='Database path')
    parser.add_argument('--min-confidence', type=float, default=0.5,
                       help='Minimum confidence for reporting')
    
    args = parser.parse_args()
    
    engine = SelfGrowthEngine(db_path=args.db)
    
    logger.info("Starting self-growth analysis...")
    stats = await engine.run_analysis()
    
    print("\n" + "="*60)
    print("SELF-GROWTH ANALYSIS RESULTS")
    print("="*60)
    print(f"Companies Analyzed: {stats.companies_analyzed}")
    print(f"\nDiscoveries:")
    print(f"  From Job Descriptions: {stats.discoveries_from_jobs}")
    print(f"  From Websites: {stats.discoveries_from_websites}")
    print(f"  From News: {stats.discoveries_from_news}")
    print(f"  TOTAL: {stats.total_discoveries}")
    print(f"\nQuality:")
    print(f"  High Confidence (>=0.7): {stats.high_confidence}")
    print(f"  Promoted to Seeds: {stats.promoted_to_seeds}")
    print(f"\nDuration: {stats.duration_seconds:.1f} seconds")
    
    # Show summary by type
    summary = engine.get_discovery_summary()
    print(f"\nDiscoveries by Type:")
    for dtype, count in sorted(summary.items(), key=lambda x: -x[1]):
        print(f"  {dtype}: {count}")
    
    # Show top discoveries
    top_discoveries = sorted(
        engine.get_discoveries(min_confidence=args.min_confidence),
        key=lambda x: -x.confidence
    )[:20]
    
    if top_discoveries:
        print(f"\nTop {len(top_discoveries)} Discoveries:")
        for d in top_discoveries:
            print(f"  {d.name} ({d.discovery_type}) - {d.confidence:.2f}")
            print(f"    Source: {d.source_company}")
            print(f"    Context: {d.context[:60]}...")


if __name__ == '__main__':
    asyncio.run(main())
