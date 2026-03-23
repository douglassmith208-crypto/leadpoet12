"""
Company scraper for Lead Sorcerer.

Scrapes company websites to extract executive names and infer email addresses.

Authoritative sources:
- Company team pages
- Company about pages
- Company leadership pages
"""

import asyncio
import logging
import os
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Common team/about page paths
COMMON_TEAM_PATHS = [
    '/team', '/about', '/leadership', '/management', '/executives',
    '/about/team', '/about/leadership', '/company/team', '/company/leadership'
]

# Common email patterns
EMAIL_PATTERNS = [
    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
]

# Common name patterns
NAME_PATTERNS = [
    r'([A-Z][a-z]+)\s+([A-Z][a-z]+)',  # First Last
    r'([A-Z][a-z]+)\.\s+([A-Z][a-z]+)',  # F. Last
]


class CompanyScraper:
    """Scraper for extracting company and executive data from company websites."""

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        self.headers = {
            'User-Agent': 'LeadSorcerer/1.0 (research@example.com)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }

    async def scrape_company_info(self, website: str) -> Optional[Dict[str, Any]]:
        """
        Scrape company information from website.

        Args:
            website: Company website URL

        Returns:
            Dictionary with company information or None if failed
        """
        try:
            # Ensure website has protocol
            if not website.startswith(('http://', 'https://')):
                website = f'https://{website}'

            # Get main page
            scrapingdog_api_key = os.getenv('SCRAPINGDOG_API_KEY', '')
            response = await self.client.get(
                "https://api.scrapingdog.com/scrape",
                params={
                    'api_key': scrapingdog_api_key,
                    'url': website,
                    'dynamic': 'true'
                },
                timeout=60.0
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Try to find team/about pages
            team_pages = await self._find_team_pages(website, soup)
            
            # Extract executives from team pages
            executives = []
            for page_url in team_pages:
                execs = await self._scrape_team_page(page_url)
                executives.extend(execs)

            # Extract company info from main page
            company_info = self._extract_company_info(soup, website)
            
            return {
                'company_info': company_info,
                'executives': executives[:10],  # Limit to top 10 executives
                'team_page_urls': team_pages
            }

        except Exception as e:
            logger.warning(f"Error scraping company {website}: {e}")
            return None

    async def _find_team_pages(self, base_url: str, soup: BeautifulSoup) -> List[str]:
        """
        Find team/about/leadership page URLs.

        Args:
            base_url: Base website URL
            soup: BeautifulSoup object of main page

        Returns:
            List of team page URLs
        """
        team_pages = []

        # Look for links with team-related text
        links = soup.find_all('a', href=True)
        for link in links:
            href = link['href']
            text = link.get_text().lower().strip()
            
            # Check if link text suggests a team/leadership page
            if any(keyword in text for keyword in ['team', 'about', 'leadership', 'executive']):
                full_url = urljoin(base_url, href)
                if self._is_valid_internal_url(base_url, full_url):
                    team_pages.append(full_url)

        # Add common team paths if not already found
        parsed_base = urlparse(base_url)
        base_domain = f"{parsed_base.scheme}://{parsed_base.netloc}"
        
        for path in COMMON_TEAM_PATHS:
            full_url = urljoin(base_domain, path)
            if full_url not in team_pages:
                team_pages.append(full_url)

        return team_pages[:5]  # Limit to 5 pages

    def _is_valid_internal_url(self, base_url: str, url: str) -> bool:
        """
        Check if URL is internal to the base domain.

        Args:
            base_url: Base website URL
            url: URL to check

        Returns:
            True if URL is internal, False otherwise
        """
        try:
            base_parsed = urlparse(base_url)
            url_parsed = urlparse(url)
            return url_parsed.netloc == base_parsed.netloc
        except Exception:
            return False

    async def _scrape_team_page(self, url: str) -> List[Dict[str, str]]:
        """
        Scrape a team/leadership page for executive information.

        Args:
            url: Team page URL

        Returns:
            List of executive dictionaries
        """
        try:
            scrapingdog_api_key = os.getenv('SCRAPINGDOG_API_KEY', '')
            response = await self.client.get(
                "https://api.scrapingdog.com/scrape",
                params={
                    'api_key': scrapingdog_api_key,
                    'url': url,
                    'dynamic': 'true'
                },
                timeout=60.0
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()

            # Extract text content
            text = soup.get_text()
            
            # Find potential names
            names = self._extract_names(text)
            
            # Find potential emails
            emails = self._extract_emails(text)
            
            # Combine into executive profiles
            executives = []
            for i, name in enumerate(names[:5]):  # Limit to 5 names
                email = emails[i] if i < len(emails) else ""
                executives.append({
                    'name': name,
                    'email': email,
                    'title': self._infer_title(name, text)
                })
            
            return executives

        except Exception as e:
            logger.warning(f"Error scraping team page {url}: {e}")
            return []

    def _extract_names(self, text: str) -> List[str]:
        """
        Extract potential names from text.

        Args:
            text: Text to extract names from

        Returns:
            List of potential names
        """
        names = []
        
        # Apply name patterns
        for pattern in NAME_PATTERNS:
            matches = re.findall(pattern, text)
            for match in matches:
                if isinstance(match, tuple):
                    name = f"{match[0]} {match[1]}" 
                else:
                    name = match
                names.append(name)
        
        # Deduplicate and filter
        unique_names = list(set(names))
        filtered_names = [name for name in unique_names if self._is_valid_name(name)]
        
        return filtered_names[:10]  # Limit to 10 names

    def _is_valid_name(self, name: str) -> bool:
        """
        Check if a string looks like a valid person's name.

        Args:
            name: String to check

        Returns:
            True if valid name, False otherwise
        """
        if not name or len(name) < 5 or len(name) > 50:
            return False
            
        parts = name.split()
        if len(parts) != 2:
            return False
            
        # Check that both parts look like names (start with capital letter)
        return all(part[0].isupper() for part in parts)

    def _extract_emails(self, text: str) -> List[str]:
        """
        Extract email addresses from text.

        Args:
            text: Text to extract emails from

        Returns:
            List of email addresses
        """
        emails = []
        
        for pattern in EMAIL_PATTERNS:
            matches = re.findall(pattern, text)
            emails.extend(matches)
        
        # Deduplicate
        return list(set(emails))[:10]  # Limit to 10 emails

    def _infer_title(self, name: str, text: str) -> str:
        """
        Infer job title for a name from surrounding text.

        Args:
            name: Person's name
            text: Surrounding text

        Returns:
            Inferred job title
        """
        # Simple implementation - in practice this would be more sophisticated
        return "Executive"

    def _extract_company_info(self, soup: BeautifulSoup, website: str) -> Dict[str, str]:
        """
        Extract basic company information from page.

        Args:
            soup: BeautifulSoup object
            website: Company website

        Returns:
            Dictionary with company information
        """
        # Extract title
        title = soup.find('title')
        company_name = title.get_text().strip() if title else ""
        
        # Extract description
        description = ""
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            description = meta_desc.get('content', '')
        
        # Parse domain
        parsed_url = urlparse(website)
        domain = parsed_url.netloc
        if domain.startswith('www.'):
            domain = domain[4:]
        
        return {
            'name': company_name,
            'description': description,
            'website': website,
            'domain': domain
        }

    async def enrich_lead_with_company_data(self, lead: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a lead with company data by scraping the company website.

        Args:
            lead: Lead dictionary

        Returns:
            Enriched lead dictionary
        """
        website = lead.get('website', '')
        if not website:
            return lead

        company_data = await self.scrape_company_info(website)
        if not company_data:
            return lead

        company_info = company_data.get('company_info', {})
        executives = company_data.get('executives', [])
        
        # Update company information
        if company_info:
            lead['business'] = company_info.get('name', lead.get('business', ''))
            lead['description'] = company_info.get('description', lead.get('description', ''))
            
        # If we have executives, update lead with first executive found
        if executives:
            exec_info = executives[0]
            name = exec_info.get('name', '')
            if name:
                # Split name into first and last
                name_parts = name.split()
                if len(name_parts) >= 2:
                    lead['first'] = name_parts[0]
                    lead['last'] = ' '.join(name_parts[1:])
                    lead['full_name'] = name
                
                # Infer email if not present
                if not lead.get('email'):
                    domain = lead.get('website', '').replace('https://', '').replace('http://', '')
                    if domain.startswith('www.'):
                        domain = domain[4:]
                    if domain:
                        # Try common email formats
                        email_formats = [
                            f"{name_parts[0].lower()}.{name_parts[-1].lower()}@{domain}",
                            f"{name_parts[0][0].lower()}{name_parts[-1].lower()}@{domain}",
                            f"{name_parts[0].lower()}@{domain}"
                        ]
                        lead['email'] = email_formats[0]  # Use first format
                
                # Update role
                lead['role'] = exec_info.get('title', lead.get('role', 'Executive'))

        return lead

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()