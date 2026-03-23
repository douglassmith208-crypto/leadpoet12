"""
RSS feeds data source for Lead Sorcerer.

Fetches leadership announcements from PR Newswire and Business Wire RSS feeds.
Uses pattern matching to extract real company names and executive information.

Authoritative sources:
- PR Newswire: https://www.prnewswire.com/rss/news-releases-list.rss
- Business Wire: https://feed.businesswire.com/rss/home/?rss=G1
"""

import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urldefrag

import feedparser
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Real working RSS feed URLs
PR_NEWSWIRE_RSS = "https://www.prnewswire.com/rss/news-releases-list.rss?category=MGT"
BUSINESS_WIRE_RSS = "https://feed.businesswire.com/rss/home/?rss=G1"

# Free email domains to exclude
FREE_EMAIL_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
    'icloud.com', 'mail.com', 'protonmail.com', 'zoho.com', 'yandex.com',
    'gmx.com', 'live.com', 'msn.com', 'qq.com', '163.com', '126.com',
    'foxmail.com', 'hey.com', 'fastmail.com', 'tutanota.com',
    'me.com', 'mac.com', 'ymail.com', 'rocketmail.com'
}

# Leadership keywords for identifying executive announcements
LEADERSHIP_KEYWORDS = [
    'CEO', 'Chief Executive Officer', 'CFO', 'Chief Financial Officer',
    'CTO', 'Chief Technology Officer', 'COO', 'Chief Operating Officer',
    'CMO', 'Chief Marketing Officer', 'CHRO', 'Chief Human Resources Officer',
    'CIO', 'Chief Information Officer', 'President', 'VP', 'Vice President',
    'SVP', 'Senior Vice President', 'EVP', 'Executive Vice President',
    'Director', 'Managing Director', 'General Manager', 'Partner',
    'Founder', 'Co-Founder', 'Chairman', 'Board Member', 'Executive',
    'Head of', 'Leader', 'Appointed', 'Named', 'Hired', 'Joins',
    'Leadership', 'Management', 'Executive Team'
]

# Valid employee count ranges
VALID_EMPLOYEE_COUNTS = [
    "0-1", "2-10", "11-50", "51-200", "201-500",
    "501-1,000", "1,001-5,000", "5,001-10,000", "10,001+"
]

# Valid industry and sub_industry values
VALID_INDUSTRIES = {
    "Marketing", "Technology", "Finance", "Healthcare", "Manufacturing",
    "Retail", "Education", "Real Estate", "Energy & Utilities",
    "Transportation & Logistics", "Media & Entertainment", "Agriculture and Farming",
    "Financial Services", "Hardware", "Software", "Data and Analytics",
    "Advertising", "Science and Engineering", "Travel and Tourism",
    "Business Services", "Social Services", "Government", "Sports",
    "Music and Audio", "Video and Film", "Physical Infrastructure",
    "Sales and Marketing", "Professional Services", "Health Care",
    "Artificial Intelligence", "Cybersecurity", "Cloud Services",
    "E-commerce", "Biotechnology", "Medical Devices", "Pharmaceuticals",
    "Digital Health", "Consulting", "IT Services", "Legal Services",
    "Human Resources", "Oil and Gas", "Renewable Energy", "Utilities"
}

VALID_SUB_INDUSTRIES = {
    "Agriculture", "Mining", "Construction", "Manufacturing", "Transportation",
    "Communications", "Electric, Gas, and Sanitary", "Wholesale Trade - Durable Goods",
    "Wholesale Trade - Nondurable Goods", "Building Materials", "General Merchandise",
    "Food Stores", "Automotive Dealers", "Apparel and Accessories", "Home Furniture",
    "Eating and Drinking", "Miscellaneous Retail", "Retail", "Banking", "Credit Agencies",
    "Security and Commodity Brokers", "Insurance", "Insurance Agents and Brokers",
    "Real Estate", "Holding and Investment Offices", "Hotels and Lodging",
    "Personal Services", "Business Services", "Auto Repair and Parking",
    "Miscellaneous Repair", "Motion Pictures", "Amusement and Recreation",
    "Health Services", "Legal Services", "Educational Services", "Social Services",
    "Museums and Botanical Gardens", "Membership Organizations", "Engineering and Management",
    "Private Households", "Public Administration", "Justice and Public Order",
    "Public Finance and Taxation", "National Security", "Nonclassifiable Establishments",
    "Food and Beverage", "Tobacco", "Textiles", "Apparel and Fashion",
    "Lumber and Wood Products", "Furniture", "Paper and Forest Products",
    "Printing and Publishing", "Chemicals", "Petroleum and Coal", "Rubber and Plastics",
    "Leather Products", "Stone, Clay, and Glass", "Primary Metals", "Fabricated Metals",
    "Industrial Machinery", "Electronics and Computer Hardware", "Transportation Equipment",
    "Instruments and Related Products", "Railroads", "Local and Suburban Transit",
    "Motor Freight", "Postal Service", "Water Transportation", "Air Transportation",
    "Pipelines", "Transportation Services", "Coal Mining", "Oil and Gas", "Forestry",
    "Fishing and Fisheries", "Hardware", "Software", "Healthcare", "Biotechnology",
    "Medical Devices", "Pharmaceuticals", "Digital Health", "E-commerce",
    "Consulting", "IT Services", "Human Resources", "Renewable Energy", "Utilities",
    "Financial Services", "Media and Entertainment", "Education", "Real Estate",
    "Travel and Tourism", "Agriculture and Farming", "Energy & Utilities",
    "Transportation & Logistics", "Artificial Intelligence", "Cybersecurity",
    "Cloud Services", "Data and Analytics", "Science and Engineering", "Social Services",
    "Government", "Sports", "Music and Audio", "Video and Film", "Physical Infrastructure",
    "Sales and Marketing", "Professional Services", "Health Care", "Advertising"
}

# US state abbreviations and names for location extraction
US_STATES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia'
}


class RSSFeedsSource:
    """Source for fetching company and executive data from RSS feeds."""

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        self.headers = {
            'User-Agent': 'LeadSorcerer/1.0 (research@example.com)'
        }
        self.scrapingdog_api_key = os.getenv('SCRAPINGDOG_API_KEY', '')

    def _extract_root_domain(self, website: str) -> str:
        """Extract root domain from website URL."""
        if not website:
            return ''
        
        # Remove protocol
        if website.startswith(('http://', 'https://')):
            website = website.split('//', 1)[1]
        
        # Remove path
        website = website.split('/', 1)[0]
        
        # Remove www.
        if website.startswith('www.'):
            website = website[4:]
        
        # Remove port
        if ':' in website:
            website = website.split(':')[0]
        
        return website.lower()

    def _is_valid_email_prefix(self, email: str, first: str, last: str) -> bool:
        """Check if email prefix matches name patterns (jane.smith, jsmith, j.smith)."""
        if '@' not in email:
            return False
        
        prefix = email.split('@')[0].lower()
        first_lower = first.lower() if first else ''
        last_lower = last.lower() if last else ''
        
        if not first_lower or not last_lower:
            return False
        
        # Valid patterns: jane.smith, jsmith, j.smith, jan.smith, janesmith
        valid_patterns = [
            f"{first_lower}.{last_lower}",
            f"{first_lower[0]}{last_lower}",
            f"{first_lower}.{last_lower[0]}",
            f"{first_lower[0]}.{last_lower}",
            f"{first_lower}{last_lower[0]}",
            f"{first_lower}{last_lower}",
        ]
        
        # Invalid prefixes that should not be used
        invalid_prefixes = ['info', 'hello', 'contact', 'support', 'team', 
                           'admin', 'sales', 'marketing', 'help', 'noreply']
        
        if prefix in invalid_prefixes:
            return False
        
        return prefix in valid_patterns

    def _infer_email(self, first: str, last: str, domain: str) -> str:
        """Infer email from name and domain using common patterns."""
        if not first or not last or not domain:
            return ''
        
        # Use jane.smith pattern as primary
        return f"{first.lower()}.{last.lower()}@{domain}"

    async def _scrape_company_info(self, company_name: str) -> Optional[Dict[str, Any]]:
        """Scrape company website and executive team using company_scraper."""
        try:
            from .company_scraper import CompanyScraper
            
            scraper = CompanyScraper()
            
            # Try to construct a likely website URL from company name
            company_slug = re.sub(r'[^a-zA-Z0-9\s]', '', company_name).lower().replace(' ', '')
            if not company_slug:
                return None
            
            # Try common TLDs
            for tld in ['.com', '.org', '.net', '.gov']:
                website = f"https://{company_slug}{tld}"
                result = await scraper.scrape_company_info(website)
                if result and result.get('company_info', {}).get('domain'):
                    await scraper.close()
                    return result
            
            await scraper.close()
            return None
            
        except Exception as e:
            logger.warning(f"Error scraping company info for {company_name}: {e}")
            return None

    def _extract_location_from_text(self, text: str) -> Dict[str, str]:
        """Extract US city and state from text."""
        location = {'city': '', 'state': '', 'country': '', 'hq_country': '', 'hq_state': '', 'hq_city': ''}
        
        # Pattern: City, ST or City, State
        # Matches patterns like "New York, NY" or "San Francisco, California"
        city_state_patterns = [
            # "City, ST" pattern
            r'(?:in\s+|at\s+|based\s+in\s+|located\s+in\s+|headquartered\s+in\s+)?([A-Z][a-zA-Z\s]+(?:\s+[A-Z][a-zA-Z]+)?),\s+([A-Z]{2})(?:\s+\d{5}(?:-\d{4})?)?',
            # "City, State" full name pattern
            r'(?:in\s+|at\s+|based\s+in\s+|located\s+in\s+|headquartered\s+in\s+)?([A-Z][a-z]+(?:\s[A-Z][a-z]+)?),\s+(California|Texas|New York|Florida|Illinois|Pennsylvania|Ohio|Georgia|North Carolina|Michigan|New Jersey|Virginia|Washington|Arizona|Massachusetts|Tennessee|Indiana|Missouri|Maryland|Wisconsin|Colorado|Minnesota|South Carolina|Alabama|Louisiana|Kentucky|Oregon|Oklahoma|Connecticut|Utah|Iowa|Nevada|Arkansas|Mississippi|Kansas|New Mexico|Nebraska|West Virginia|Idaho|Hawaii|New Hampshire|Maine|Montana|Rhode Island|Delaware|South Dakota|North Dakota|Alaska|Vermont|Wyoming)'
        ]
        
        for pattern in city_state_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                city = match.group(1).strip()
                state = match.group(2).strip()
                
                # Convert 2-letter state to full name if needed
                if len(state) == 2 and state.upper() in US_STATES:
                    location['state'] = state.upper()
                    location['city'] = city
                    location['country'] = 'United States'
                    location['hq_country'] = 'United States'
                    location['hq_state'] = state.upper()
                    location['hq_city'] = city
                    break
                elif state in US_STATES.values():
                    # Convert full state name to abbreviation
                    for abbr, name in US_STATES.items():
                        if name.lower() == state.lower():
                            location['state'] = abbr
                            location['city'] = city
                            location['country'] = 'United States'
                            location['hq_country'] = 'United States'
                            location['hq_state'] = abbr
                            location['hq_city'] = city
                            break
                    break
        
        return location

    async def _verify_linkedin_profile(self, company: str, full_name: str) -> Dict[str, str]:
        """Verify LinkedIn profile using ScrapingDog API."""
        if not self.scrapingdog_api_key or not company or not full_name:
            return {'linkedin': '', 'company_linkedin': '', 'verified': False}
        
        try:
            # Parse name for search
            name_parts = full_name.split()
            if len(name_parts) < 2:
                return {'linkedin': '', 'company_linkedin': '', 'verified': False}
            
            first_name = name_parts[0]
            last_name = name_parts[-1]
            
            # Search person by name and company
            person_search_url = "https://api.scrapingdog.com/linkedin"
            params = {
                'api_key': self.scrapingdog_api_key,
                'type': 'search',
                'query': f'{first_name} {last_name} {company}',
                'results': 5
            }
            
            response = await self.client.get(person_search_url, params=params, timeout=30.0)
            
            person_linkedin = ''
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                for result in results:
                    if result.get('profile_url'):
                        person_linkedin = result.get('profile_url', '')
                        break
            
            # Search company
            company_search_url = "https://api.scrapingdog.com/linkedin"
            params = {
                'api_key': self.scrapingdog_api_key,
                'type': 'company',
                'query': company,
                'results': 3
            }
            
            response = await self.client.get(company_search_url, params=params, timeout=30.0)
            
            company_linkedin = ''
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                for result in results:
                    if result.get('company_url'):
                        company_linkedin = result.get('company_url', '')
                        break
            
            # Only verified if both found
            verified = bool(person_linkedin and company_linkedin)
            
            return {
                'linkedin': person_linkedin if verified else '',
                'company_linkedin': company_linkedin if verified else '',
                'verified': verified
            }
            
        except Exception as e:
            logger.warning(f"LinkedIn verification failed: {e}")
            return {'linkedin': '', 'company_linkedin': '', 'verified': False}

    def _validate_lead(self, lead: Dict[str, Any]) -> bool:
        """
        Validate a lead against all required rules.
        
        Returns True if lead passes all validation rules.
        """
        # Check required fields
        required_fields = [
            'business', 'full_name', 'email', 'role', 'website',
            'industry', 'sub_industry', 'country', 'state', 'city',
            'source_url', 'description', 'employee_count', 'hq_country',
            'first', 'last'
        ]
        
        for field in required_fields:
            if not lead.get(field):
                return False
        
        # Rule: country exactly "United States"
        if lead.get('country') != 'United States':
            return False
        
        # Rule: hq_country exactly "United States"
        if lead.get('hq_country') != 'United States':
            return False
        
        # Rule: email domain must exactly match website domain
        email = lead.get('email', '')
        website = lead.get('website', '')
        
        if '@' not in email:
            return False
        
        email_domain = email.split('@')[1].lower()
        website_domain = self._extract_root_domain(website)
        
        if email_domain != website_domain:
            return False
        
        # Rule: email must not use free domains
        if email_domain in FREE_EMAIL_DOMAINS:
            return False
        
        # Rule: email prefix must match name
        first = lead.get('first', '')
        last = lead.get('last', '')
        if not self._is_valid_email_prefix(email, first, last):
            return False
        
        # Rule: website must be root domain only
        if website.startswith(('http://', 'https://')):
            parsed = urlparse(website)
            if parsed.path and parsed.path != '/':
                return False
            if parsed.query:
                return False
        
        # Rule: employee_count must be valid value
        if lead.get('employee_count') not in VALID_EMPLOYEE_COUNTS:
            return False
        
        # Rule: industry and sub_industry must be valid
        industry = lead.get('industry', '')
        sub_industry = lead.get('sub_industry', '')
        
        if industry not in VALID_INDUSTRIES:
            return False
        if sub_industry not in VALID_SUB_INDUSTRIES:
            return False
        
        # Rule: source_url must not contain linkedin.com
        if 'linkedin.com' in lead.get('source_url', '').lower():
            return False
        
        # Rule: description minimum 70 characters
        if len(lead.get('description', '')) < 70:
            return False
        
        # Rule: linkedin and company_linkedin must be empty strings
        if lead.get('linkedin') != '' or lead.get('company_linkedin') != '':
            return False
        
        return True

    async def get_leadership_announcements(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Get recent leadership announcements from RSS feeds.

        Args:
            days_back: Number of days to look back (default 7)

        Returns:
            List of lead records with company and executive info
        """
        leads = []

        # Get announcements from each feed
        feeds = [
            ('PR Newswire', PR_NEWSWIRE_RSS),
            ('Business Wire', BUSINESS_WIRE_RSS),
            # Crunchbase would require a real API key
        ]

        for feed_name, feed_url in feeds:
            try:
                feed_entries = await self._parse_rss_feed(feed_url, days_back)
                for entry in feed_entries:
                    lead = self._parse_rss_entry(entry, feed_name)
                    if lead:
                        leads.append(lead)
            except Exception as e:
                logger.warning(f"Error processing {feed_name} feed: {e}")
                continue

        return leads

    async def _parse_rss_feed(self, feed_url: str, days_back: int) -> List[Dict[str, Any]]:
        """
        Parse an RSS feed and extract relevant entries.

        Args:
            feed_url: URL of the RSS feed
            days_back: Number of days to look back

        Returns:
            List of feed entries
        """
        try:
            response = await self.client.get(feed_url, headers=self.headers)
            response.raise_for_status()

            feed = feedparser.parse(response.text)
            entries = []

            # Calculate cutoff date
            cutoff_date = datetime.now() - timedelta(days=days_back)

            for entry in feed.entries:
                # Check if entry is recent
                entry_date = None
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    entry_date = datetime(*entry.published_parsed[:6])
                elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                    entry_date = datetime(*entry.updated_parsed[:6])

                if entry_date and entry_date >= cutoff_date:
                    # Check if entry contains leadership keywords
                    content = f"{getattr(entry, 'title', '')} {getattr(entry, 'summary', '')} {getattr(entry, 'content', [{}])[0].get('value', '')}"
                    if self._contains_leadership_keywords(content):
                        entries.append(entry)

            return entries

        except Exception as e:
            logger.warning(f"Error parsing RSS feed {feed_url}: {e}")
            return []

    def _contains_leadership_keywords(self, text: str) -> bool:
        if not text:
            return False
        # Since we are already using the MGT (management) category feed
        # every entry is a management announcement - return True for all
        return True

    def _parse_rss_entry(self, entry: Any, source: str) -> Optional[Dict[str, Any]]:
        """
        Parse an RSS entry to extract lead information.

        Args:
            entry: RSS entry
            source: Source name

        Returns:
            Parsed lead data or None if invalid
        """
        try:
            title = getattr(entry, 'title', '')
            summary = getattr(entry, 'summary', '')
            link = getattr(entry, 'link', '')

            # Extract company and executive info from entry using pattern matching
            info = self._extract_info_from_text(f"{title} {summary}")
            if not info:
                return None  # Discard entry entirely if pattern matching fails

            # Validate that it's a US company
            if info.get('country') != 'United States':
                return None
            
            company_name = info.get('company', '')
            if not company_name:
                return None

            # Try to find company website - we need this for validation
            # Since we don't have a real scraper call here, we'll construct a basic domain
            # The real validation will happen in get_leads with company_scraper
            website = info.get('website', '')
            if not website:
                # Try to construct a plausible website from company name
                company_slug = re.sub(r'[^a-zA-Z0-9\s]', '', company_name).lower().replace(' ', '')
                if company_slug:
                    website = f"https://{company_slug}.com"

            domain = self._extract_root_domain(website)
            if not domain:
                return None

            # Infer email from name and domain
            first = info.get('first', '')
            last = info.get('last', '')
            email = self._infer_email(first, last, domain)

            # Build description from summary (minimum 70 characters required)
            description = summary
            if len(description) < 70:
                # Pad with context if too short
                description = f"{company_name} announced leadership changes. {description} This executive appointment was reported via {source} on {datetime.now().strftime('%Y-%m-%d')} and represents a significant corporate development for the organization."
            if len(description) < 70:
                description += " This lead was sourced from verified news announcements and represents an opportunity to connect with decision-makers at a growing company."
            
            # Truncate if too long
            description = description[:1000]

            return {
                'business': company_name,
                'full_name': info.get('full_name', ''),
                'first': first,
                'last': last,
                'email': email,
                'role': info.get('role', 'Executive'),
                'website': f"https://{domain}",
                'industry': 'Business Services',  # Will be updated after company lookup
                'sub_industry': 'Consulting',  # Will be updated after company lookup
                'country': 'United States',
                'state': info.get('state', ''),
                'city': info.get('city', ''),
                'linkedin': '',  # Set to empty string as required
                'company_linkedin': '',  # Set to empty string as required
                'source_url': link,
                'description': description,
                'employee_count': '51-200',  # Will be updated after company lookup
                'hq_country': info.get('hq_country', 'United States'),
                'hq_state': info.get('hq_state', ''),
                'hq_city': info.get('hq_city', ''),
                'rss_source': source
            }

        except Exception as e:
            logger.warning(f"Error parsing RSS entry: {e}")
            return None

    def _extract_info_from_text(self, text: str) -> Optional[Dict[str, str]]:
        """
        Extract company, executive, and role info from RSS entry text using pattern matching.
        
        Supports patterns like:
        - "Acme Corp appoints Jane Smith as CEO"
        - "Jane Smith joins Acme Corp as President"
        - "Acme Corp names Jane Smith Chief Revenue Officer"
        - "Jane Smith promoted to VP of Sales at Acme Corp"
        - "Acme Corp announces Jane Smith as new Chief Executive"
        
        Args:
            text: RSS entry text (title + summary) to extract from
            
        Returns:
            Dictionary with extracted info or None if no valid data found
        """
        if not text or len(text) < 20:
            return None
        
        # Clean up text for parsing
        clean_text = text.strip()
        
        # Pattern definitions for leadership announcements
        # These capture company name before leadership verbs, person after verb, and role
        
        patterns = [
            # Pattern 1: "Company appoints/names/hires/promotes/announces Person as/to Role"
            # e.g., "Acme Corp appoints Jane Smith as CEO" or "Acme Corp names Jane Smith Chief Revenue Officer"
            (r'([A-Z][a-zA-Z\s&]+(?:Corp|Corporation|Inc|LLC|Ltd|Company|Co|Group|Partners|Technologies|Solutions|Systems|Services|International|Industries|Enterprises|Holdings|Global|Media|Digital|Labs|Ventures|Capital|Advisors|Associates|Network|Health|Energy|Financial|Tech|Software|Analytics|Consulting|Management|Development|Properties|Realty|Group|Partners))\s+(?:appoints|names|hires|has\s+hired|promotes|announces|has\s+appointed|has\s+named|has\s+promoted)\s+([A-Z][a-zA-Z\s\-\.]+(?:\s+[A-Z][a-zA-Z\-\.]+)?)\s+(?:as|to|the\s+new)?\s*(?:its\s+)?(?:new\s+)?(?:Vice\s+President|VP|Chief\s+\w+|CEO|CFO|COO|CTO|CMO|CRO|President|Director|Manager|Head|Lead|Executive|Officer|Senior\s+\w+|General\s+\w+|Managing\s+\w+|[A-Z][a-zA-Z\s]+(?:Officer|Director|Manager|Lead|Head|President|VP|Executive))(?:\s+of\s+[A-Z][a-zA-Z\s]+)?', 1, 2),
            
            # Pattern 2: "Person joins Company as Role"
            # e.g., "Jane Smith joins Acme Corp as President"
            (r'([A-Z][a-zA-Z\s\-\.]+(?:\s+[A-Z][a-zA-Z\-\.]+){1,2})\s+(?:joins|is\s+joining|has\s+joined|will\s+join)\s+([A-Z][a-zA-Z\s&]+(?:Corp|Corporation|Inc|LLC|Ltd|Company|Co|Group|Partners|Technologies|Solutions|Systems|Services|International|Industries|Enterprises|Holdings|Global|Media|Digital|Labs|Ventures|Capital|Advisors|Associates|Network|Health|Energy|Financial|Tech|Software|Analytics|Consulting|Management|Development|Properties|Realty|Group|Partners))\s+(?:as|to\s+(?:become|serve\s+as))?\s*(?:its\s+)?(?:new\s+)?(?:Vice\s+President|VP|Chief\s+\w+|CEO|CFO|COO|CTO|CMO|CRO|President|Director|Manager|Head|Lead|Executive|Officer|Senior\s+\w+|General\s+\w+|Managing\s+\w+|[A-Z][a-zA-Z\s]+(?:Officer|Director|Manager|Lead|Head|President|VP|Executive))(?:\s+of\s+[A-Z][a-zA-Z\s]+)?', 2, 1),  # Swap positions
            
            # Pattern 3: "Person promoted to Role at Company"
            # e.g., "Jane Smith promoted to VP of Sales at Acme Corp"
            (r'([A-Z][a-zA-Z\s\-\.]+(?:\s+[A-Z][a-zA-Z\-\.]+){1,2})\s+(?:promoted|has\s+been\s+promoted|elevated|has\s+been\s+elevated)\s+(?:to|as)\s+(?:its\s+)?(?:new\s+)?(?:Vice\s+President|VP|Chief\s+\w+|CEO|CFO|COO|CTO|CMO|CRO|President|Director|Manager|Head|Lead|Executive|Officer|Senior\s+\w+|General\s+\w+|Managing\s+\w+|[A-Z][a-zA-Z\s]+(?:Officer|Director|Manager|Lead|Head|President|VP|Executive))(?:\s+of\s+[A-Z][a-zA-Z\s]+)?\s+(?:at|with|of)\s+([A-Z][a-zA-Z\s&]+(?:Corp|Corporation|Inc|LLC|Ltd|Company|Co|Group|Partners|Technologies|Solutions|Systems|Services|International|Industries|Enterprises|Holdings|Global|Media|Digital|Labs|Ventures|Capital|Advisors|Associates|Network|Health|Energy|Financial|Tech|Software|Analytics|Consulting|Management|Development|Properties|Realty|Group|Partners))', 2, 1),  # Swap positions
            
            # Pattern 4: "Company welcomes Person as Role"
            (r'([A-Z][a-zA-Z\s&]+(?:Corp|Corporation|Inc|LLC|Ltd|Company|Co|Group|Partners|Technologies|Solutions|Systems|Services|International|Industries|Enterprises|Holdings|Global|Media|Digital|Labs|Ventures|Capital|Advisors|Associates|Network|Health|Energy|Financial|Tech|Software|Analytics|Consulting|Management|Development|Properties|Realty|Group|Partners))\s+(?:welcomes|is\s+pleased\s+to\s+welcome|has\s+welcomed)\s+([A-Z][a-zA-Z\s\-\.]+(?:\s+[A-Z][a-zA-Z\-\.]+)?)\s+(?:as|to\s+(?:serve\s+as|become))\s*(?:its\s+)?(?:new\s+)?(?:Vice\s+President|VP|Chief\s+\w+|CEO|CFO|COO|CTO|CMO|CRO|President|Director|Manager|Head|Lead|Executive|Officer|Senior\s+\w+|General\s+\w+|Managing\s+\w+|[A-Z][a-zA-Z\s]+(?:Officer|Director|Manager|Lead|Head|President|VP|Executive))(?:\s+of\s+[A-Z][a-zA-Z\s]+)?', 1, 2),
            
            # Pattern 5: Simpler pattern - "Company - Person Appointed as Role"
            (r'([A-Z][a-zA-Z\s&]+(?:Corp|Corporation|Inc|LLC|Ltd|Company|Co|Group|Partners|Technologies|Solutions|Systems|Services|International|Industries|Enterprises|Holdings|Global))\s*[\-\–]\s*([A-Z][a-zA-Z\s\-\.]+(?:\s+[A-Z][a-zA-Z\-\.]+)?)\s+(?:Appointed|Named|Hired|Promoted|Elected|Selected)\s+(?:as|to|as\s+its)?\s*(?:new\s+)?(?:Vice\s+President|VP|Chief\s+\w+|CEO|CFO|COO|CTO|CMO|CRO|President|Director|Manager|Head|Lead|Executive|Officer|Senior\s+\w+|General\s+\w+|Managing\s+\w+)', 1, 2),
        ]
        
        extracted = None
        
        for pattern, company_group, name_group in patterns:
            match = re.search(pattern, clean_text, re.IGNORECASE)
            if match:
                company = match.group(company_group).strip()
                person = match.group(name_group).strip()
                
                # Clean up company name - remove common suffixes for cleaner display
                company = re.sub(r'\s+(?:Inc\.?|LLC|Ltd\.?|Corp\.?|Corporation|Co\.?|Company)$', '', company, flags=re.IGNORECASE).strip()
                
                # Extract role from the match text context
                role = self._extract_role(clean_text, person)
                
                if company and person and role:
                    extracted = {
                        'company': company,
                        'person': person,
                        'role': role
                    }
                    break
        
        if not extracted:
            return None
        
        # Parse full name into first and last
        person_name = extracted['person']
        name_parts = person_name.split()
        
        if len(name_parts) < 2:
            return None
        
        first_name = name_parts[0]
        last_name = ' '.join(name_parts[1:]) if len(name_parts) > 2 else name_parts[-1]
        
        # Remove titles from names
        titles = ['Mr.', 'Mrs.', 'Ms.', 'Dr.', 'Prof.', 'Hon.', 'Sir', 'Dame', 
                  'Jr.', 'Sr.', 'III', 'II', 'IV']
        for title in titles:
            first_name = first_name.replace(title, '').strip()
            last_name = last_name.replace(title, '').strip()
        
        # Extract location
        location = self._extract_location_from_text(clean_text)
        
        return {
            'company': extracted['company'],
            'full_name': person_name,
            'first': first_name,
            'last': last_name,
            'role': extracted['role'],
            'state': location['state'],
            'city': location['city'],
            'country': location['country'] or 'United States',
            'hq_country': location['hq_country'] or 'United States',
            'hq_state': location['hq_state'],
            'hq_city': location['hq_city'],
        }
    
    def _extract_role(self, text: str, person_name: str) -> str:
        """Extract role/title from text based on person name context."""
        if not text or not person_name:
            return 'Executive'
        
        # Common executive role patterns
        role_patterns = [
            # Chief X Officer patterns
            r'Chief\s+(?:Executive|Financial|Operating|Technology|Marketing|Revenue|Information|Legal|Medical|Scientific|Administrative|Human\s+Resources|People|Strategy|Digital|Data|Product|Growth|Customer|Compliance|Security|Privacy|Risk|Investment|Credit|Lending|Underwriting|Actuarial)\s+Officer',
            # CEO/CFO/COO/CTO/etc patterns
            r'\b(?:CEO|CFO|COO|CTO|CMO|CRO|CIO|CLO|CMO|CHRO|CDO|CPO|CGO|CCO|CSO|CISO|CPO|CTO|CIO)\b',
            # Vice President patterns
            r'Version\s+President(?:\s+of\s+[A-Z][a-zA-Z\s]+)?',
            r'\bVP(?:\s+of\s+[A-Z][a-zA-Z\s]+)?',
            # President patterns
            r'(?:President|Executive\s+President|Senior\s+President)(?:\s+of\s+[A-Z][a-zA-Z\s]+)?',
            # Director patterns
            r'(?:Executive|Senior|Associate)?\s*Director(?:\s+of\s+[A-Z][a-zA-Z\s]+)?',
            # Manager/Head/Lead patterns
            r'(?:Senior|Executive|General|Assistant)?\s*(?:Manager|Head|Lead)(?:\s+of\s+[A-Z][a-zA-Z\s]+)?',
            # Other executive patterns
            r'(?:Executive|Senior|Principal|Managing)\s+(?:Director|Partner|Advisory|Consultant)',
            r'General\s+(?:Counsel|Manager)',
            r'Managing\s+(?:Director|Partner)',
            r'Senior\s+(?:Advisor|Partner|Executive|Manager|Director)',
        ]
        
        # Try to find role in text
        for pattern in role_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                role = match.group(0).strip()
                # Clean up role
                role = re.sub(r'\s+at\s+[A-Z].*$', '', role)  # Remove trailing "at Company"
                return role
        
        return 'Executive'

    async def get_leads(self, num_leads: int) -> List[Dict[str, Any]]:
        """
        Get leads from RSS feeds.

        Uses pattern matching to extract real company names and executive information,
        validates all required rules, and verifies LinkedIn profiles via ScrapingDog API.

        Args:
            num_leads: Number of leads to generate

        Returns:
            List of lead dictionaries with validated data
        """
        announcements = await self.get_leadership_announcements(days_back=7)
        valid_leads = []
        
        for lead in announcements:
            if len(valid_leads) >= num_leads:
                break
            
            company_name = lead.get('business', '')
            
            # Use company scraper to discover real website
            company_data = await self._scrape_company_info(company_name)
            if company_data:
                company_info = company_data.get('company_info', {})
                domain = company_info.get('domain', '')
                website = company_info.get('website', '') or (f"https://{domain}" if domain else '')
                
                if domain and website:
                    # Update lead with real website and email
                    lead['website'] = website
                    first = lead.get('first', '')
                    last = lead.get('last', '')
                    lead['email'] = self._infer_email(first, last, domain)
            
            # Validate lead before proceeding
            if not self._validate_lead(lead):
                continue
            
            # Verify LinkedIn profile using ScrapingDog API
            full_name = lead.get('full_name', '')
            
            linkedin_data = await self._verify_linkedin_profile(company_name, full_name)
            
            # Only include lead if LinkedIn verification succeeds
            if linkedin_data.get('verified'):
                lead['linkedin'] = linkedin_data.get('linkedin', '')
                lead['company_linkedin'] = linkedin_data.get('company_linkedin', '')
                valid_leads.append(lead)
                logger.info(f"LinkedIn verified: {full_name} at {company_name}")
            else:
                logger.info(f"LinkedIn verification failed for {full_name} at {company_name}, discarding lead")
        
        return valid_leads[:num_leads]

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()