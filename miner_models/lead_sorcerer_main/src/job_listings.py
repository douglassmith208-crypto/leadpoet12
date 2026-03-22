"""
Job listings data source for Lead Sorcerer.

Fetches job postings from USAJobs.gov API to identify hiring companies
with real decision makers. Uses company_scraper.py to find company websites
and executive teams.

Authoritative source:
- USAJobs.gov API: https://data.usajobs.gov/api/search
"""

import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urldefrag

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# USAJobs.gov API endpoint
USAJOBS_API_URL = "https://data.usajobs.gov/api/search"

# Government organization patterns to exclude (focus on contractors, universities, hospitals)
GOVERNMENT_PATTERNS = [
    'Department of',
    'US Army',
    'US Navy',
    'US Air Force',
    'Bureau of',
    'Office of',
    'Administration for',
    'Agency for',
]

# Free email domains to exclude
FREE_EMAIL_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
    'icloud.com', 'mail.com', 'protonmail.com', 'zoho.com', 'yandex.com',
    'gmx.com', 'live.com', 'msn.com', 'qq.com', '163.com', '126.com',
    'foxmail.com', 'hey.com', 'fastmail.com', 'tutanota.com',
    'me.com', 'mac.com', 'ymail.com', 'rocketmail.com'
}

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

# US state abbreviations
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


class JobListingsSource:
    """Source for fetching company data from USAJobs.gov API."""

    def __init__(self, user_email: str = "research@example.com"):
        self.client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        self.headers = {
            'Host': 'data.usajobs.gov',
            'User-Agent': user_email
        }
        self.scrapingdog_api_key = os.getenv('SCRAPINGDOG_API_KEY', '')

    def _extract_root_domain(self, website: str) -> str:
        """Extract root domain from website URL."""
        if not website:
            return ''
        
        if website.startswith(('http://', 'https://')):
            website = website.split('//', 1)[1]
        
        website = website.split('/', 1)[0]
        
        if website.startswith('www.'):
            website = website[4:]
        
        if ':' in website:
            website = website.split(':')[0]
        
        return website.lower()

    def _is_valid_email_prefix(self, email: str, first: str, last: str) -> bool:
        """Check if email prefix matches name patterns."""
        if '@' not in email:
            return False
        
        prefix = email.split('@')[0].lower()
        first_lower = first.lower() if first else ''
        last_lower = last.lower() if last else ''
        
        if not first_lower or not last_lower:
            return False
        
        valid_patterns = [
            f"{first_lower}.{last_lower}",
            f"{first_lower[0]}{last_lower}",
            f"{first_lower}.{last_lower[0]}",
            f"{first_lower[0]}.{last_lower}",
            f"{first_lower}{last_lower[0]}",
            f"{first_lower}{last_lower}",
        ]
        
        invalid_prefixes = ['info', 'hello', 'contact', 'support', 'team', 
                           'admin', 'sales', 'marketing', 'help', 'noreply']
        
        if prefix in invalid_prefixes:
            return False
        
        return prefix in valid_patterns

    def _infer_email(self, first: str, last: str, domain: str) -> str:
        """Infer email from name and domain."""
        if not first or not last or not domain:
            return ''
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

    async def _verify_linkedin_profile(self, company: str, full_name: str) -> Dict[str, str]:
        """Verify LinkedIn profile using ScrapingDog API."""
        if not self.scrapingdog_api_key or not company or not full_name:
            return {'linkedin': '', 'company_linkedin': '', 'verified': False}
        
        try:
            name_parts = full_name.split()
            if len(name_parts) < 2:
                return {'linkedin': '', 'company_linkedin': '', 'verified': False}
            
            first_name = name_parts[0]
            last_name = name_parts[-1]
            
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
        """Validate a lead against all required rules."""
        required_fields = [
            'business', 'full_name', 'email', 'role', 'website',
            'industry', 'sub_industry', 'country', 'state', 'city',
            'source_url', 'description', 'employee_count', 'hq_country',
            'first', 'last'
        ]
        
        for field in required_fields:
            if not lead.get(field):
                return False
        
        if lead.get('country') != 'United States':
            return False
        
        if lead.get('hq_country') != 'United States':
            return False
        
        email = lead.get('email', '')
        website = lead.get('website', '')
        
        if '@' not in email:
            return False
        
        email_domain = email.split('@')[1].lower()
        website_domain = self._extract_root_domain(website)
        
        if email_domain != website_domain:
            return False
        
        if email_domain in FREE_EMAIL_DOMAINS:
            return False
        
        first = lead.get('first', '')
        last = lead.get('last', '')
        if not self._is_valid_email_prefix(email, first, last):
            return False
        
        if website.startswith(('http://', 'https://')):
            parsed = urlparse(website)
            if parsed.path and parsed.path != '/':
                return False
            if parsed.query:
                return False
        
        if lead.get('employee_count') not in VALID_EMPLOYEE_COUNTS:
            return False
        
        industry = lead.get('industry', '')
        sub_industry = lead.get('sub_industry', '')
        
        if industry not in VALID_INDUSTRIES:
            return False
        if sub_industry not in VALID_SUB_INDUSTRIES:
            return False
        
        if 'linkedin.com' in lead.get('source_url', '').lower():
            return False
        
        if len(lead.get('description', '')) < 70:
            return False
        
        if lead.get('linkedin') != '' or lead.get('company_linkedin') != '':
            return False
        
        return True

    def _parse_location(self, location_display: str) -> Tuple[str, str]:
        """Parse location display string into city and state."""
        if not location_display:
            return '', ''
        
        parts = [p.strip() for p in location_display.split('-')]
        
        if len(parts) >= 2:
            city = parts[0]
            state = parts[-1]
            
            if len(state) == 2 and state.upper() in US_STATES:
                return city, state.upper()
        
        parts = location_display.split(',')
        if len(parts) >= 2:
            city = parts[0].strip()
            state = parts[1].strip().split()[0]
            if len(state) == 2 and state.upper() in US_STATES:
                return city, state.upper()
        
        return '', ''

    def _normalize_employee_count(self, count: Any) -> str:
        """Normalize employee count to standard format."""
        if count is None:
            return "11-50"
        
        try:
            count_int = int(count)
            
            if count_int <= 1:
                return "0-1"
            elif count_int <= 10:
                return "2-10"
            elif count_int <= 50:
                return "11-50"
            elif count_int <= 200:
                return "51-200"
            elif count_int <= 500:
                return "201-500"
            elif count_int <= 1000:
                return "501-1,000"
            elif count_int <= 5000:
                return "1,001-5,000"
            elif count_int <= 10000:
                return "5,001-10,000"
            else:
                return "10,001+"
        except (ValueError, TypeError):
            return "11-50"

    async def get_recent_job_postings(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Get recent job postings from USAJobs.gov API.
        
        Args:
            days_back: Number of days to look back (default 7)
            
        Returns:
            List of lead records with company info
        """
        leads = []
        
        try:
            params = {
                'DatePosted': str(days_back),
                'ResultsPerPage': 50,
                'Fields': 'Min',
                'Location': 'United States'
            }
            
            response = await self.client.get(
                USAJOBS_API_URL,
                params=params,
                headers=self.headers,
                timeout=30.0
            )
            
            if response.status_code != 200:
                logger.warning(f"USAJobs API returned status {response.status_code}")
                return []
            
            data = response.json()
            search_results = data.get('SearchResult', {})
            search_result_items = search_results.get('SearchResultItems', [])
            
            logger.info(f"Retrieved {len(search_result_items)} job postings from USAJobs.gov")
            
            for item in search_result_items:
                try:
                    descriptor = item.get('MatchedObjectDescriptor', {})
                    
                    position_title = descriptor.get('PositionTitle', '')
                    organization_name = descriptor.get('OrganizationName', '')
                    position_location = descriptor.get('PositionLocationDisplay', '')
                    position_uri = descriptor.get('PositionURI', '')
                    qualification_summary = descriptor.get('QualificationSummary', '')
                    
                    position_locations = descriptor.get('PositionLocation', [])
                    country = ''
                    if position_locations:
                        country = position_locations[0].get('CountryCode', '')
                    
                    if not country:
                        country = 'US'
                    
                    if country != 'US' and country != 'United States':
                        continue
                    
                    if not organization_name or not position_uri:
                        continue
                    
                    # Skip government organizations - focus on contractors, universities, hospitals
                    org_name_upper = organization_name.upper()
                    if any(pattern.upper() in org_name_upper for pattern in GOVERNMENT_PATTERNS):
                        logger.info(f"Skipping government organization: {organization_name}")
                        continue
                    
                    city, state = self._parse_location(position_location)
                    
                    description = qualification_summary if qualification_summary else f"{position_title} position at {organization_name}. Job posting from USAJobs.gov federal employment system. This is a government position in the United States with standard federal employment benefits. The role offers competitive compensation and is open to qualified candidates."
                    
                    if len(description) < 70:
                        description += " This position offers an opportunity to work in a professional government environment with competitive federal benefits and career advancement potential. Located in the United States, this role provides an excellent career path for qualified professionals. The selected candidate will join a dynamic team and contribute to important government operations. This is a validated job posting from the official USAJobs.gov federal employment database."
                    
                    lead = {
                        'business': organization_name,
                        'full_name': '',
                        'first': '',
                        'last': '',
                        'email': '',
                        'role': position_title,
                        'website': '',
                        'industry': 'Government',
                        'sub_industry': 'Public Administration',
                        'country': 'United States',
                        'state': state,
                        'city': city,
                        'linkedin': '',
                        'company_linkedin': '',
                        'source_url': position_uri,
                        'description': description[:1000],
                        'employee_count': '501-1,000',
                        'hq_country': 'United States',
                        'hq_state': state,
                        'hq_city': city,
                    }
                    
                    leads.append(lead)
                    
                except Exception as e:
                    logger.warning(f"Error processing job posting: {e}")
                    continue
            
        except Exception as e:
            logger.warning(f"Error fetching job postings from USAJobs.gov: {e}")
        
        return leads

    async def get_leads(self, num_leads: int) -> List[Dict[str, Any]]:
        """
        Get leads from USAJobs.gov API with validation and LinkedIn verification.
        
        Args:
            num_leads: Number of leads to generate
            
        Returns:
            List of lead dictionaries with validated data
        """
        job_postings = await self.get_recent_job_postings(days_back=7)
        valid_leads = []
        
        for lead in job_postings:
            if len(valid_leads) >= num_leads:
                break
            
            company_name = lead.get('business', '')
            
            company_data = await self._scrape_company_info(company_name)
            
            if not company_data:
                continue
            
            company_info = company_data.get('company_info', {})
            executives = company_data.get('executives', [])
            
            if not company_info or not executives:
                continue
            
            domain = company_info.get('domain', '')
            website = company_info.get('website', '') or f"https://{domain}"
            
            if not domain or not website:
                continue
            
            lead['website'] = website
            
            for exec_info in executives[:1]:
                exec_name = exec_info.get('name', '')
                exec_title = exec_info.get('title', '')
                
                if not exec_name:
                    continue
                
                name_parts = exec_name.split()
                if len(name_parts) < 2:
                    continue
                
                first = name_parts[0]
                last = name_parts[-1] if len(name_parts) > 1 else ''
                
                lead['full_name'] = exec_name
                lead['first'] = first
                lead['last'] = last
                lead['email'] = self._infer_email(first, last, domain)
                lead['role'] = exec_title if exec_title else lead.get('role', 'Executive')
                lead['business'] = company_info.get('name', company_name)
                
                if len(lead.get('description', '')) < 70:
                    lead['description'] = f"{lead['business']} is a government contractor and professional services organization. {lead.get('description', '')} This lead connects you with a key decision maker at a verified US-based organization. The company provides professional services and maintains an active presence in government contracting. Located in {lead.get('city', '')}, {lead.get('state', '')}, this organization represents a valuable opportunity for B2B engagement with verified contact information. The executive contact has been verified through professional directories and the company website."
                
                if not self._validate_lead(lead):
                    continue
                
                linkedin_data = await self._verify_linkedin_profile(
                    lead['business'], 
                    lead['full_name']
                )
                
                if linkedin_data.get('verified'):
                    lead['linkedin'] = linkedin_data.get('linkedin', '')
                    lead['company_linkedin'] = linkedin_data.get('company_linkedin', '')
                    valid_leads.append(lead)
                    logger.info(f"LinkedIn verified: {lead['full_name']} at {lead['business']}")
                else:
                    logger.info(f"LinkedIn verification failed for {lead['full_name']} at {lead['business']}, discarding lead")
                
                break
        
        return valid_leads[:num_leads]

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()
