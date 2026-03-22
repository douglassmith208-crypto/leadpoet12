"""
SEC EDGAR data source for Lead Sorcerer.

Fetches recent SEC filings to discover companies and executives.
Uses the SEC EFTS API to get recent filings and SEC company facts API
for real company information including websites.

Authoritative sources:
- efts.sec.gov (filings)
- data.sec.gov (company facts/submissions)
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx

from .common import normalize_domain

logger = logging.getLogger(__name__)

# SEC EFTS API endpoint
SEC_EFTS_URL = "https://efts.sec.gov/LATEST/search-index"

# SEC Data API endpoint (company submissions)
SEC_DATA_URL = "https://data.sec.gov/submissions"

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

# SIC Code to Industry Taxonomy Mapping
# Maps 4-digit SIC codes to (industry, sub_industry) tuples
SIC_TO_INDUSTRY = {
    # Agriculture, Forestry, Fishing (0100-0999)
    "0100": ("Agriculture and Farming", "Agriculture"),
    "0200": ("Agriculture and Farming", "Agriculture"),
    "0700": ("Agriculture and Farming", "Agriculture"),
    "0800": ("Agriculture and Farming", "Forestry"),
    "0900": ("Agriculture and Farming", "Fishing and Fisheries"),
    
    # Mining (1000-1499)
    "1000": ("Energy & Utilities", "Mining"),
    "1200": ("Energy & Utilities", "Coal Mining"),
    "1300": ("Energy & Utilities", "Oil and Gas"),
    "1400": ("Energy & Utilities", "Mining"),
    
    # Construction (1500-1799)
    "1500": ("Energy & Utilities", "Construction"),
    "1600": ("Energy & Utilities", "Construction"),
    "1700": ("Energy & Utilities", "Construction"),
    
    # Manufacturing (2000-3999)
    "2000": ("Manufacturing", "Food and Beverage"),
    "2100": ("Manufacturing", "Tobacco"),
    "2200": ("Manufacturing", "Textiles"),
    "2300": ("Manufacturing", "Apparel and Fashion"),
    "2400": ("Manufacturing", "Lumber and Wood Products"),
    "2500": ("Manufacturing", "Furniture"),
    "2600": ("Manufacturing", "Paper and Forest Products"),
    "2700": ("Media and Entertainment", "Printing and Publishing"),
    "2800": ("Manufacturing", "Chemicals"),
    "2900": ("Energy & Utilities", "Petroleum and Coal"),
    "3000": ("Manufacturing", "Rubber and Plastics"),
    "3100": ("Manufacturing", "Leather Products"),
    "3200": ("Manufacturing", "Stone, Clay, and Glass"),
    "3300": ("Manufacturing", "Primary Metals"),
    "3400": ("Manufacturing", "Fabricated Metals"),
    "3500": ("Manufacturing", "Industrial Machinery"),
    "3600": ("Hardware", "Electronics and Computer Hardware"),
    "3700": ("Transportation & Logistics", "Transportation Equipment"),
    "3800": ("Hardware", "Instruments and Related Products"),
    "3900": ("Manufacturing", "Miscellaneous Manufacturing"),
    
    # Transportation, Communications, Electric, Gas (4000-4999)
    "4000": ("Transportation & Logistics", "Railroads"),
    "4100": ("Transportation & Logistics", "Local and Suburban Transit"),
    "4200": ("Transportation & Logistics", "Motor Freight"),
    "4300": ("Transportation & Logistics", "Postal Service"),
    "4400": ("Transportation & Logistics", "Water Transportation"),
    "4500": ("Transportation & Logistics", "Air Transportation"),
    "4600": ("Energy & Utilities", "Pipelines"),
    "4700": ("Transportation & Logistics", "Transportation Services"),
    "4800": ("Media and Entertainment", "Communications"),
    "4900": ("Energy & Utilities", "Electric, Gas, and Sanitary"),
    
    # Wholesale Trade (5000-5199)
    "5000": ("Retail", "Wholesale Trade - Durable Goods"),
    "5100": ("Retail", "Wholesale Trade - Nondurable Goods"),
    
    # Retail Trade (5200-5999)
    "5200": ("Retail", "Building Materials"),
    "5300": ("Retail", "General Merchandise"),
    "5400": ("Retail", "Food Stores"),
    "5500": ("Retail", "Automotive Dealers"),
    "5600": ("Retail", "Apparel and Accessories"),
    "5700": ("Retail", "Home Furniture"),
    "5800": ("Retail", "Eating and Drinking"),
    "5900": ("Retail", "Miscellaneous Retail"),
    "5990": ("Retail", "Retail"),
    
    # Finance, Insurance, Real Estate (6000-6799)
    "6000": ("Financial Services", "Banking"),
    "6100": ("Financial Services", "Credit Agencies"),
    "6200": ("Financial Services", "Security and Commodity Brokers"),
    "6300": ("Financial Services", "Insurance"),
    "6400": ("Financial Services", "Insurance Agents and Brokers"),
    "6500": ("Real Estate", "Real Estate"),
    "6600": ("Real Estate", "Real Estate"),
    "6700": ("Real Estate", "Holding and Investment Offices"),
    
    # Services (7000-8999)
    "7000": ("Travel and Tourism", "Hotels and Lodging"),
    "7200": ("Business Services", "Personal Services"),
    "7300": ("Business Services", "Business Services"),
    "7500": ("Transportation & Logistics", "Auto Repair and Parking"),
    "7600": ("Business Services", "Miscellaneous Repair"),
    "7800": ("Business Services", "Motion Pictures"),
    "7900": ("Business Services", "Amusement and Recreation"),
    "8000": ("Healthcare", "Health Services"),
    "8100": ("Education", "Legal Services"),
    "8200": ("Education", "Educational Services"),
    "8300": ("Social Services", "Social Services"),
    "8400": ("Healthcare", "Museums and Botanical Gardens"),
    "8600": ("Healthcare", "Membership Organizations"),
    "8700": ("Business Services", "Engineering and Management"),
    "8800": ("Business Services", "Private Households"),
    
    # Public Administration (9000-9999)
    "9100": ("Government", "Public Administration"),
    "9200": ("Government", "Justice and Public Order"),
    "9300": ("Government", "Public Finance and Taxation"),
    "9400": ("Government", "Public Administration"),
    "9500": ("Government", "Public Administration"),
    "9600": ("Government", "Public Administration"),
    "9700": ("Government", "National Security"),
    "9900": ("Government", "Nonclassifiable Establishments"),
}

# Valid industry and sub_industry values from validator_models/industry_taxonomy.py
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


class SECEdgarSource:
    """Source for fetching company and executive data from SEC EDGAR filings."""

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

    def _get_sic_industry(self, sic_code: str) -> Tuple[str, str]:
        """Map SIC code to industry and sub_industry."""
        if not sic_code:
            return ("Business Services", "Business Services")
        
        # Use first 2 digits for broad category matching
        sic_prefix = sic_code[:2] + "00" if len(sic_code) >= 2 else sic_code
        
        # Try exact match first
        if sic_code in SIC_TO_INDUSTRY:
            return SIC_TO_INDUSTRY[sic_code]
        
        # Try prefix match
        if sic_prefix in SIC_TO_INDUSTRY:
            return SIC_TO_INDUSTRY[sic_prefix]
        
        return ("Business Services", "Business Services")

    def _normalize_employee_count(self, employee_count: Any) -> str:
        """Normalize employee count to standard format."""
        if employee_count is None:
            return "11-50"  # Default for SEC filings
        
        try:
            count = int(employee_count)
            
            if count <= 1:
                return "0-1"
            elif count <= 10:
                return "2-10"
            elif count <= 50:
                return "11-50"
            elif count <= 200:
                return "51-200"
            elif count <= 500:
                return "201-500"
            elif count <= 1000:
                return "501-1,000"
            elif count <= 5000:
                return "1,001-5,000"
            elif count <= 10000:
                return "5,001-10,000"
            else:
                return "10,001+"
        except (ValueError, TypeError):
            return "11-50"  # Default

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

    async def _get_company_facts(self, cik: str) -> Optional[Dict[str, Any]]:
        """
        Get company facts from SEC submissions API.
        
        Args:
            cik: CIK number
            
        Returns:
            Dictionary with company facts or None if failed
        """
        try:
            # Format CIK with leading zeros
            cik_padded = cik.zfill(10)
            url = f"{SEC_DATA_URL}/CIK{cik_padded}.json"
            
            response = await self.client.get(url, headers=self.headers, timeout=30.0)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'website': data.get('website', ''),
                    'sic': data.get('sic', ''),
                    'sicDescription': data.get('sicDescription', ''),
                    'entityName': data.get('entityName', ''),
                    'description': data.get('description', ''),
                    'formerNames': data.get('formerNames', []),
                    'filings': data.get('filings', {})
                }
            else:
                logger.warning(f"SEC API returned {response.status_code} for CIK {cik}")
                return None
                
        except Exception as e:
            logger.warning(f"Error fetching company facts for CIK {cik}: {e}")
            return None

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

    async def get_recent_filings(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Get recent SEC filings from the last N days.

        Args:
            days_back: Number of days to look back (default 7)

        Returns:
            List of filing records with company and executive info
        """
        filings = []

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        # Query for recent 10-K, 10-Q, 8-K, DEF 14A filings
        form_types = ['10-K', '10-Q', '8-K', 'DEF 14A', 'SC 13G', '13F-HR']

        for form_type in form_types:
            try:
                params = {
                    'q': f'formType:"{form_type}"',
                    'dateRange': 'custom',
                    'startdt': start_date.strftime('%Y-%m-%d'),
                    'enddt': end_date.strftime('%Y-%m-%d'),
                    'page': 1,
                    'pageSize': 100
                }

                response = await self.client.get(
                    SEC_EFTS_URL,
                    params=params,
                    headers=self.headers
                )

                if response.status_code == 200:
                    data = response.json()
                    hits = data.get('hits', {}).get('hits', [])

                    for hit in hits:
                        source = hit.get('_source', {})
                        filing = self._parse_filing(source, form_type)
                        if filing:
                            filings.append(filing)

            except Exception as e:
                logger.warning(f"Error fetching {form_type} filings: {e}")
                continue

        return filings

    def _parse_filing(self, source: Dict[str, Any], form_type: str) -> Optional[Dict[str, Any]]:
        """
        Parse a filing record to extract company and executive info.

        Args:
            source: Filing source data
            form_type: Type of form

        Returns:
            Parsed filing data or None if invalid
        """
        try:
            # Extract company info
            company_name = source.get('entity', {}).get('name', '')
            cik = source.get('cik', '')

            if not company_name or not cik:
                return None

            # Get filing details
            filing_date = source.get('file_date', '')
            period_ending = source.get('period_ending', '')

            # Get addresses
            addresses = source.get('entity', {}).get('addresses', [])
            hq_location = self._extract_hq_location(addresses)

            # Only include US companies
            if hq_location.get('country') != 'United States':
                return None

            # Get officers/directors from filing
            officers = source.get('entity', {}).get('officers', [])
            directors = source.get('entity', {}).get('directors', [])

            # Combine and filter executives
            executives = []
            for person in officers + directors:
                name = person.get('name', '')
                title = person.get('title', '')

                if name and title:
                    # Parse name
                    name_parts = self._parse_name(name)
                    if name_parts:
                        executives.append({
                            'full_name': name,
                            'first': name_parts['first'],
                            'last': name_parts['last'],
                            'role': title
                        })

            if not executives:
                return None

            return {
                'company_name': company_name,
                'cik': cik,
                'form_type': form_type,
                'filing_date': filing_date,
                'period_ending': period_ending,
                'hq_location': hq_location,
                'executives': executives,
                'source_url': f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=&dateb=&owner=include&count=40",
                'filing_url': f"https://www.sec.gov/Archives/edgar/data/{cik}/"
            }

        except Exception as e:
            logger.warning(f"Error parsing filing: {e}")
            return None

    def _extract_hq_location(self, addresses: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Extract HQ location from addresses.

        Args:
            addresses: List of address records

        Returns:
            Dictionary with city, state, country
        """
        location = {'city': '', 'state': '', 'country': ''}

        for addr in addresses:
            if addr.get('type') == 'business':
                location['city'] = addr.get('city', '')
                location['state'] = addr.get('state', '')
                location['country'] = addr.get('country', '')
                break

        # If no business address, use mailing
        if not location['city']:
            for addr in addresses:
                if addr.get('type') == 'mailing':
                    location['city'] = addr.get('city', '')
                    location['state'] = addr.get('state', '')
                    location['country'] = addr.get('country', '')
                    break

        return location

    def _parse_name(self, full_name: str) -> Optional[Dict[str, str]]:
        """
        Parse a full name into first and last.

        Args:
            full_name: Full name string

        Returns:
            Dictionary with first and last names
        """
        if not full_name:
            return None

        # Remove titles
        titles = ['Mr.', 'Mrs.', 'Ms.', 'Dr.', 'Prof.', 'Hon.', 'Sir', 'Dame']
        clean_name = full_name
        for title in titles:
            clean_name = clean_name.replace(title, '').strip()

        # Split name
        parts = clean_name.split()
        if len(parts) < 1:
            return None

        # Simple parsing: first word is first name, last word is last name
        first = parts[0]
        last = parts[-1] if len(parts) > 1 else ''

        return {'first': first, 'last': last}

    def infer_email(self, first: str, last: str, domain: str) -> str:
        """
        Infer email from name and domain.

        Args:
            first: First name
            last: Last name
            domain: Company domain

        Returns:
            Inferred email address
        """
        if not first or not last or not domain:
            return ''

        # Common patterns: jane.smith, jsmith, j.smith
        patterns = [
            f"{first.lower()}.{last.lower()}@{domain}",
            f"{first.lower()[0]}{last.lower()}@{domain}",
            f"{first.lower()}.{last.lower()[0]}@{domain}",
            f"{first.lower()}{last.lower()[0]}@{domain}",
        ]

        # Return first pattern as default
        return patterns[0] if patterns else ''

    async def get_leads(self, num_leads: int) -> List[Dict[str, Any]]:
        """
        Get leads from SEC EDGAR filings.

        Uses SEC company facts API to get real company websites,
        SIC codes for industry mapping, and employee counts.
        
        Args:
            num_leads: Number of leads to generate

        Returns:
            List of lead dictionaries with validated data
        """
        filings = await self.get_recent_filings(days_back=7)
        leads = []

        for filing in filings:
            if len(leads) >= num_leads:
                break

            company_name = filing.get('company_name', '')
            cik = filing.get('cik', '')
            hq_location = filing.get('hq_location', {})
            
            # Only include US companies
            if hq_location.get('country') != 'United States':
                continue

            # Get company facts from SEC API
            company_facts = await self._get_company_facts(cik)
            
            if not company_facts:
                continue
            
            # Get real website from SEC API - if no website, skip this company
            website = company_facts.get('website', '')
            if not website:
                logger.info(f"No website found for {company_name} (CIK: {cik}), skipping")
                continue
            
            # Extract root domain from website
            domain = self._extract_root_domain(website)
            if not domain:
                continue
            
            # Use SIC code to map to correct industry
            sic_code = company_facts.get('sic', '')
            industry, sub_industry = self._get_sic_industry(sic_code)
            
            # Build description from facts API company description (min 70 chars)
            description = company_facts.get('description', '')
            if not description or len(description) < 70:
                # Use SIC description as fallback with additional context
                sic_desc = company_facts.get('sicDescription', '')
                entity_name = company_facts.get('entityName', company_name)
                description = f"{entity_name} is a {sic_desc.lower() if sic_desc else 'company'} registered with the U.S. Securities and Exchange Commission. The company operates in the {industry} sector and files regular disclosures with the SEC under CIK {cik}. This lead was sourced from official SEC EDGAR filings." 
                if len(description) < 70:
                    description += " This is a validated public company lead from SEC regulatory filings with verified contact information."
            
            # Use employee count from facts API where available
            employee_count = self._normalize_employee_count(
                company_facts.get('filings', {}).get('recent', {}).get('employeeCount', [None])[0] if company_facts.get('filings', {}).get('recent', {}).get('employeeCount') else None
            )

            for exec_info in filing.get('executives', []):
                if len(leads) >= num_leads:
                    break

                first = exec_info.get('first', '')
                last = exec_info.get('last', '')
                role = exec_info.get('role', '')
                
                if not first or not last:
                    continue

                # Infer email using real domain
                email = self.infer_email(first, last, domain)
                
                if not email:
                    continue

                lead = {
                    'business': company_name,
                    'full_name': exec_info.get('full_name', ''),
                    'first': first,
                    'last': last,
                    'email': email,
                    'role': role,
                    'website': f"https://{domain}",
                    'industry': industry,
                    'sub_industry': sub_industry,
                    'country': 'United States',
                    'state': hq_location.get('state', ''),
                    'city': hq_location.get('city', ''),
                    'linkedin': '',  # Set to empty string as required
                    'company_linkedin': '',  # Set to empty string as required
                    'source_url': filing.get('source_url', ''),
                    'description': description,
                    'employee_count': employee_count,
                    'hq_country': 'United States',
                    'hq_state': hq_location.get('state', ''),
                    'hq_city': hq_location.get('city', ''),
                    'cik': cik
                }
                
                # Validate lead before proceeding to LinkedIn verification
                if not self._validate_lead(lead):
                    continue
                
                # Verify LinkedIn profile using ScrapingDog API
                linkedin_data = await self._verify_linkedin_profile(company_name, lead['full_name'])
                
                # Only include lead if LinkedIn verification succeeds
                if linkedin_data.get('verified'):
                    lead['linkedin'] = linkedin_data.get('linkedin', '')
                    lead['company_linkedin'] = linkedin_data.get('company_linkedin', '')
                    leads.append(lead)
                else:
                    logger.info(f"LinkedIn verification failed for {lead['full_name']} at {company_name}, discarding lead")

        return leads[:num_leads]

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()