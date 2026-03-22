"""
SEC EDGAR data source for Lead Sorcerer.

Fetches recent SEC filings to discover companies and executives.
Uses the SEC EFTS API to get recent filings from the last 7 days.

Authoritative source: efts.sec.gov
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import httpx

from .common import normalize_domain

logger = logging.getLogger(__name__)

# SEC EFTS API endpoint
SEC_EFTS_URL = "https://efts.sec.gov/LATEST/search-index"

# Free email domains to exclude
FREE_EMAIL_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
    'icloud.com', 'mail.com', 'protonmail.com', 'zoho.com', 'yandex.com',
    'gmx.com', 'live.com', 'msn.com', 'qq.com', '163.com', '126.com',
    'foxmail.com', 'hey.com', 'fastmail.com', 'tutanota.com'
}

# Valid employee count ranges
VALID_EMPLOYEE_COUNTS = [
    "0-1", "2-10", "11-50", "51-200", "201-500",
    "501-1,000", "1,001-5,000", "5,001-10,000", "10,001+"
]


class SECEdgarSource:
    """Source for fetching company and executive data from SEC EDGAR filings."""

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        self.headers = {
            'User-Agent': 'LeadSorcerer/1.0 (research@example.com)'
        }

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

        Args:
            num_leads: Number of leads to generate

        Returns:
            List of lead dictionaries
        """
        filings = await self.get_recent_filings(days_back=7)
        leads = []

        for filing in filings:
            if len(leads) >= num_leads:
                break

            company_name = filing.get('company_name', '')
            cik = filing.get('cik', '')
            hq_location = filing.get('hq_location', {})

            # Get company website (we'll need to look this up or infer)
            # For now, use a placeholder that will be resolved later
            domain = f"{cik}.sec.gov"  # Placeholder

            for exec_info in filing.get('executives', []):
                if len(leads) >= num_leads:
                    break

                first = exec_info.get('first', '')
                last = exec_info.get('last', '')
                role = exec_info.get('role', '')

                # Infer email (will be updated when we get real domain)
                email = self.infer_email(first, last, domain)

                lead = {
                    'business': company_name,
                    'full_name': exec_info.get('full_name', ''),
                    'first': first,
                    'last': last,
                    'email': email,
                    'role': role,
                    'website': f"https://{domain}",
                    'industry': 'Financial Services',  # Default for SEC filings
                    'sub_industry': 'Investment Management',
                    'country': 'United States',
                    'state': hq_location.get('state', ''),
                    'city': hq_location.get('city', ''),
                    'linkedin': '',  # Will be filled by enrichment
                    'company_linkedin': '',  # Will be filled by enrichment
                    'source_url': filing.get('source_url', ''),
                    'description': f"{company_name} - SEC registrant",
                    'employee_count': '201-500',  # Default, will be updated
                    'hq_country': 'United States',
                    'hq_state': hq_location.get('state', ''),
                    'hq_city': hq_location.get('city', ''),
                    'cik': cik
                }

                leads.append(lead)

        return leads[:num_leads]

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()