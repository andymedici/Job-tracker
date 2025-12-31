"""
Self-Growth Intelligence System (Simplified)
=============================================
Automatically discovers new companies from:
1. Job description text mining (partners, integrations, competitors)
2. Company website crawling (customers, partners, testimonials)
3. News/funding monitoring

Works with existing database tables - no migrations needed.
Discoveries are added directly to seed_companies table.
"""

import asyncio
import aiohttp
import json
import re
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Set, Optional
from collections import defaultdict

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    logging.warning("BeautifulSoup not available - website crawling disabled")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class DiscoveredCompany:
    """A company discovered through self-growth"""
    name: str
    source_company: str
    discovery_type: str  # 'partner', 'customer', 'competitor', 'integration', 'news'
    confidence: float  # 0.0 to 1.0
    context: str
    discovered_at: datetime = field(default_factory=datetime.now)
    url: Optional[str] = None


# =============================================================================
# KNOWN INTEGRATIONS (Filter out - too common to be useful seeds)
# =============================================================================

KNOWN_INTEGRATIONS = {
    # Cloud Providers
    'aws', 'amazon web services', 'azure', 'google cloud', 'gcp', 'digitalocean',
    'heroku', 'vercel', 'netlify', 'cloudflare', 'fastly',
    
    # Dev Tools
    'github', 'gitlab', 'bitbucket', 'jira', 'confluence', 'notion', 'slack',
    'discord', 'teams', 'zoom', 'figma', 'miro', 'linear', 'asana',
    
    # Databases
    'postgresql', 'postgres', 'mysql', 'mongodb', 'redis', 'elasticsearch',
    'dynamodb', 'firebase', 'supabase', 'planetscale',
    
    # Auth/Identity
    'okta', 'auth0', 'onelogin', 'google', 'microsoft',
    
    # Analytics
    'google analytics', 'mixpanel', 'amplitude', 'segment', 'heap',
    'datadog', 'new relic', 'sentry', 'pagerduty',
    
    # Payment
    'stripe', 'paypal', 'braintree', 'square', 'plaid',
    
    # CRM/Marketing
    'salesforce', 'hubspot', 'marketo', 'mailchimp', 'sendgrid', 'twilio',
    
    # Generic terms
    'api', 'sdk', 'rest', 'graphql', 'webhook', 'integration', 'plugin',
    'saas', 'cloud', 'platform', 'software', 'service', 'solution',
}


# =============================================================================
# JOB DESCRIPTION MINING
# =============================================================================

class JobDescriptionMiner:
    """Extract company mentions from job descriptions"""
    
    EXTRACTION_PATTERNS = [
        # Partners
        (r'(?:partner(?:ing|ed|s)?\s+with|strategic\s+partner(?:s)?)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3})', 'partner', 0.7),
        (r'(?:in\s+partnership\s+with)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3})', 'partner', 0.7),
        
        # Customers
        (r'(?:customers?\s+(?:include|like|such\s+as))\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3})', 'customer', 0.6),
        (r'(?:trusted\s+by|used\s+by|chosen\s+by)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3})', 'customer', 0.6),
        
        # Competitors
        (r'(?:competitor(?:s)?\s+(?:to|like|include))\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3})', 'competitor', 0.75),
        (r'(?:alternative\s+to)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3})', 'competitor', 0.75),
        
        # Integrations
        (r'(?:integrat(?:e|es|ion)\s+with)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3})', 'integration', 0.7),
        (r'(?:works?\s+with|connects?\s+to)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3})', 'integration', 0.65),
        
        # Acquired
        (r'(?:acquired\s+by|acquisition\s+of|parent\s+company)\s+([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3})', 'acquired', 0.85),
    ]
    
    @classmethod
    def extract_companies(cls, text: str, source_company: str) -> List[DiscoveredCompany]:
        """Extract company names from text"""
        discoveries = []
        seen = set()
        
        for pattern, discovery_type, confidence in cls.EXTRACTION_PATTERNS:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                name = match.group(1).strip()
                name_lower = name.lower()
                
                # Filter out known integrations and junk
                if name_lower in KNOWN_INTEGRATIONS:
                    continue
                if name_lower in seen:
                    continue
                if len(name) < 2 or len(name) > 50:
                    continue
                if not re.match(r'^[A-Za-z]', name):
                    continue
                
                seen.add(name_lower)
                discoveries.append(DiscoveredCompany(
                    name=name,
                    source_company=source_company,
                    discovery_type=discovery_type,
                    confidence=confidence,
                    context=match.group(0)[:100],
                ))
        
        return discoveries


# =============================================================================
# WEBSITE CRAWLER
# =============================================================================

class WebsiteCrawler:
    """Crawl company websites for partner/customer mentions"""
    
    PAGES_TO_CHECK = ['/', '/customers', '/case-studies', '/partners', '/about']
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
    
    async def crawl_company(self, company_name: str, base_url: str) -> List[DiscoveredCompany]:
        """Crawl a company's website for mentions"""
        if not BS4_AVAILABLE:
            return []
        
        discoveries = []
        
        for page in self.PAGES_TO_CHECK:
            url = base_url.rstrip('/') + page
            try:
                async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status != 200:
                        continue
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Look for company logos/names in typical sections
                    discoveries.extend(self._extract_from_logos(soup, company_name, url))
                    discoveries.extend(self._extract_from_testimonials(soup, company_name, url))
                    
            except Exception:
                continue
        
        return discoveries
    
    def _extract_from_logos(self, soup, source_company: str, url: str) -> List[DiscoveredCompany]:
        """Extract company names from logo sections"""
        discoveries = []
        
        # Look for common logo container classes
        logo_containers = soup.find_all(class_=re.compile(r'logo|customer|partner|client', re.I))
        
        for container in logo_containers:
            # Check img alt texts
            for img in container.find_all('img'):
                alt = img.get('alt', '')
                if alt and len(alt) > 2 and alt[0].isupper():
                    if alt.lower() not in KNOWN_INTEGRATIONS:
                        discoveries.append(DiscoveredCompany(
                            name=alt,
                            source_company=source_company,
                            discovery_type='customer',
                            confidence=0.6,
                            context=f"Logo on {url}",
                            url=url,
                        ))
        
        return discoveries[:10]  # Limit per page
    
    def _extract_from_testimonials(self, soup, source_company: str, url: str) -> List[DiscoveredCompany]:
        """Extract company names from testimonials"""
        discoveries = []
        
        testimonials = soup.find_all(class_=re.compile(r'testimonial|quote|review', re.I))
        
        for testimonial in testimonials:
            # Look for company attribution
            cite = testimonial.find(['cite', 'figcaption']) or testimonial.find(class_=re.compile(r'author|attribution', re.I))
            if cite:
                text = cite.get_text()
                # Extract company name (usually after "@" or "at" or ",")
                match = re.search(r'(?:@|at|,)\s*([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,2})', text)
                if match:
                    name = match.group(1).strip()
                    if name.lower() not in KNOWN_INTEGRATIONS:
                        discoveries.append(DiscoveredCompany(
                            name=name,
                            source_company=source_company,
                            discovery_type='customer',
                            confidence=0.7,
                            context=f"Testimonial: {text[:80]}",
                            url=url,
                        ))
        
        return discoveries[:5]


# =============================================================================
# NEWS MONITOR
# =============================================================================

class NewsMonitor:
    """Monitor funding news for new companies"""
    
    FUNDING_PATTERNS = [
        r'([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,2})\s+raises?\s+\$[\d.]+[MB]',
        r'([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,2})\s+closes?\s+\$[\d.]+[MB]',
        r'([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,2})\s+secures?\s+\$[\d.]+[MB]',
        r'([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,2})\s+announces?\s+Series\s+[A-Z]',
    ]
    
    NEWS_SOURCES = [
        'https://techcrunch.com/tag/funding/',
        'https://news.crunchbase.com/venture/',
    ]
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
    
    async def check_news(self) -> List[DiscoveredCompany]:
        """Check news sources for funding announcements"""
        if not BS4_AVAILABLE:
            return []
        
        discoveries = []
        
        for source_url in self.NEWS_SOURCES:
            try:
                async with self.session.get(source_url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status != 200:
                        continue
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Get article headlines
                    headlines = soup.find_all(['h1', 'h2', 'h3', 'a'], class_=re.compile(r'title|headline', re.I))
                    
                    for headline in headlines[:20]:
                        text = headline.get_text()
                        for pattern in self.FUNDING_PATTERNS:
                            match = re.search(pattern, text)
                            if match:
                                name = match.group(1).strip()
                                if name.lower() not in KNOWN_INTEGRATIONS:
                                    discoveries.append(DiscoveredCompany(
                                        name=name,
                                        source_company='news',
                                        discovery_type='funding',
                                        confidence=0.85,
                                        context=text[:100],
                                        url=source_url,
                                    ))
                    
            except Exception:
                continue
        
        return discoveries


# =============================================================================
# SELF-GROWTH ENGINE
# =============================================================================

class SelfGrowthEngine:
    """Main engine for self-growth company discovery"""
    
    def __init__(self, db):
        """
        Initialize with database connection.
        
        Args:
            db: Database object with get_connection() method (from database.py)
        """
        self.db = db
        self.discoveries: List[DiscoveredCompany] = []
        self.seen_names: Set[str] = set()
        self.stats = {
            'companies_analyzed': 0,
            'discoveries_from_jobs': 0,
            'discoveries_from_websites': 0,
            'discoveries_from_news': 0,
            'total_discoveries': 0,
            'promoted_to_seeds': 0,
        }
    
    async def run_analysis(self, limit: int = 200) -> Dict:
        """
        Run full self-growth analysis.
        
        Args:
            limit: Max number of companies to analyze
            
        Returns:
            Stats dictionary
        """
        start_time = datetime.now()
        
        # Load tracked companies and existing seeds
        companies = self._load_tracked_companies(limit)
        self._load_existing_seeds()
        
        self.stats['companies_analyzed'] = len(companies)
        logger.info(f"🧠 Analyzing {len(companies)} tracked companies for growth opportunities...")
        
        async with aiohttp.ClientSession(
            headers={'User-Agent': 'Mozilla/5.0 (compatible; JobIntel/1.0)'}
        ) as session:
            
            # 1. Mine job descriptions
            logger.info("📝 Mining job descriptions...")
            for company in companies:
                job_discoveries = self._mine_job_descriptions(company)
                self.stats['discoveries_from_jobs'] += len(job_discoveries)
                self._add_discoveries(job_discoveries)
            
            # 2. Crawl websites (sample)
            if BS4_AVAILABLE:
                logger.info("🌐 Crawling company websites...")
                crawler = WebsiteCrawler(session)
                sample = companies[:30]  # Limit for speed
                
                for company in sample:
                    board_url = company.get('board_url', '')
                    if board_url:
                        # Try to derive company website from board URL
                        try:
                            from urllib.parse import urlparse
                            parsed = urlparse(board_url)
                            # Try common patterns
                            token = company.get('company_name_token', '')
                            if token:
                                web_urls = [
                                    f"https://{token}.com",
                                    f"https://www.{token}.com",
                                ]
                                for url in web_urls:
                                    try:
                                        web_discoveries = await crawler.crawl_company(
                                            company.get('company_name', ''),
                                            url
                                        )
                                        self.stats['discoveries_from_websites'] += len(web_discoveries)
                                        self._add_discoveries(web_discoveries)
                                        break
                                    except:
                                        continue
                        except:
                            pass
            
            # 3. Check news
            logger.info("📰 Checking funding news...")
            news_monitor = NewsMonitor(session)
            try:
                news_discoveries = await news_monitor.check_news()
                self.stats['discoveries_from_news'] += len(news_discoveries)
                self._add_discoveries(news_discoveries)
            except Exception as e:
                logger.warning(f"News check failed: {e}")
        
        # Calculate totals
        self.stats['total_discoveries'] = len(self.discoveries)
        high_confidence = [d for d in self.discoveries if d.confidence >= 0.7]
        
        # Promote to seeds
        promoted = self._promote_to_seeds(high_confidence)
        self.stats['promoted_to_seeds'] = promoted
        
        duration = (datetime.now() - start_time).total_seconds()
        self.stats['duration_seconds'] = duration
        
        # Log summary
        logger.info("=" * 60)
        logger.info("🧠 SELF-GROWTH ANALYSIS COMPLETE")
        logger.info("=" * 60)
        logger.info(f"   Companies Analyzed: {self.stats['companies_analyzed']}")
        logger.info(f"   Discoveries from Jobs: {self.stats['discoveries_from_jobs']}")
        logger.info(f"   Discoveries from Websites: {self.stats['discoveries_from_websites']}")
        logger.info(f"   Discoveries from News: {self.stats['discoveries_from_news']}")
        logger.info(f"   Total Discoveries: {self.stats['total_discoveries']}")
        logger.info(f"   High Confidence (>=0.7): {len(high_confidence)}")
        logger.info(f"   Promoted to Seeds: {promoted}")
        logger.info(f"   Duration: {duration:.1f}s")
        logger.info("=" * 60)
        
        # Log top discoveries
        if self.discoveries:
            top = sorted(self.discoveries, key=lambda x: -x.confidence)[:10]
            logger.info("Top discoveries:")
            for d in top:
                logger.info(f"   {d.name} ({d.discovery_type}) - confidence: {d.confidence:.2f}")
        
        return self.stats
    
    def _load_tracked_companies(self, limit: int) -> List[Dict]:
        """Load tracked companies from database"""
        companies = []
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            c.company_name,
                            c.company_name_token,
                            c.ats_type,
                            c.board_url,
                            c.job_count
                        FROM companies c
                        WHERE c.job_count > 0
                        ORDER BY c.job_count DESC
                        LIMIT %s
                    """, (limit,))
                    
                    for row in cur.fetchall():
                        companies.append({
                            'company_name': row[0],
                            'company_name_token': row[1],
                            'ats_type': row[2],
                            'board_url': row[3],
                            'job_count': row[4],
                        })
        except Exception as e:
            logger.error(f"Error loading companies: {e}")
        
        return companies
    
    def _load_existing_seeds(self):
        """Load existing seed names to avoid duplicates"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Load from seed_companies
                    cur.execute("SELECT name FROM seed_companies")
                    for row in cur.fetchall():
                        self.seen_names.add(row[0].lower())
                    
                    # Also load existing tracked companies
                    cur.execute("SELECT company_name FROM companies")
                    for row in cur.fetchall():
                        self.seen_names.add(row[0].lower())
                        
        except Exception as e:
            logger.warning(f"Error loading existing seeds: {e}")
    
    def _mine_job_descriptions(self, company: Dict) -> List[DiscoveredCompany]:
        """Mine job data for company mentions"""
        discoveries = []
        company_name = company.get('company_name', '')
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # Get job titles and departments for this company
                    cur.execute("""
                        SELECT title, department, location
                        FROM job_archive
                        WHERE company_id = (
                            SELECT id FROM companies WHERE company_name = %s LIMIT 1
                        )
                        AND status = 'active'
                        LIMIT 100
                    """, (company_name,))
                    
                    # Combine text for mining
                    text_parts = []
                    for row in cur.fetchall():
                        if row[0]: text_parts.append(row[0])
                        if row[1]: text_parts.append(row[1])
                    
                    combined_text = ' '.join(text_parts)
                    discoveries = JobDescriptionMiner.extract_companies(combined_text, company_name)
                    
        except Exception as e:
            logger.debug(f"Error mining jobs for {company_name}: {e}")
        
        return discoveries
    
    def _add_discoveries(self, new_discoveries: List[DiscoveredCompany]):
        """Add new discoveries, deduplicating"""
        for d in new_discoveries:
            name_lower = d.name.lower()
            if name_lower not in self.seen_names:
                self.seen_names.add(name_lower)
                self.discoveries.append(d)
    
    def _promote_to_seeds(self, discoveries: List[DiscoveredCompany]) -> int:
        """Add high-confidence discoveries to seed_companies table"""
        promoted = 0
        
        for discovery in discoveries:
            try:
                with self.db.get_connection() as conn:
                    with conn.cursor() as cur:
                        # Determine tier based on confidence
                        tier = 1 if discovery.confidence >= 0.85 else 2
                        
                        # Insert into seed_companies
                        cur.execute("""
                            INSERT INTO seed_companies (name, source, tier)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (name) DO NOTHING
                        """, (
                            discovery.name,
                            f'self_growth_{discovery.discovery_type}',
                            tier,
                        ))
                        
                        if cur.rowcount > 0:
                            promoted += 1
                            logger.info(f"   ✅ Promoted: {discovery.name} (tier {tier}, {discovery.discovery_type})")
                        
                        conn.commit()
                        
            except Exception as e:
                logger.debug(f"Error promoting {discovery.name}: {e}")
        
        return promoted
    
    def get_discoveries(self) -> List[DiscoveredCompany]:
        """Get all discoveries"""
        return self.discoveries


# =============================================================================
# MAIN ENTRY POINT (called from app.py)
# =============================================================================

async def run_self_growth(db, limit: int = 200) -> Dict:
    """
    Main entry point for self-growth analysis.
    
    Args:
        db: Database object with get_connection() method
        limit: Max companies to analyze
        
    Returns:
        Stats dictionary
    """
    engine = SelfGrowthEngine(db)
    stats = await engine.run_analysis(limit=limit)
    return stats


# =============================================================================
# CLI (for standalone testing)
# =============================================================================

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Self-Growth Intelligence System')
    parser.add_argument('--limit', type=int, default=200, help='Max companies to analyze')
    args = parser.parse_args()
    
    print("⚠️  This module requires a database connection from app.py")
    print("   Use: POST /api/self-growth/run")
    print("")
    print("   Or import and call:")
    print("   from self_growth_intelligence import run_self_growth")
    print("   stats = await run_self_growth(db, limit=200)")
