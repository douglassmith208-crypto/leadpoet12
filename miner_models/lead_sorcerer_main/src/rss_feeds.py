"""
RSS feeds data source for Lead Sorcerer.

Fetches leadership announcements from PR Newswire, Business Wire, and Crunchbase.

Authoritative sources:
- PR Newswire RSS feeds
- Business Wire RSS feeds
- Crunchbase funding RSS feeds
"""

import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import feedparser
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# RSS feed URLs
PR_NEWSWIRE_RSS = "https://www.prnewswire.com/rss/business-technology-latest-news.rss"
BUSINESS_WIRE_RSS = "https://www.businesswire.com/portal/site/home/news/rss/\?pl=en_US"
CRUNCHBASE_RSS = "https://rss.app/feeds/1234567890.xml"  # Placeholder

# Keywords for leadership announcements
LEADERSHIP_KEYWORDS = [
    'appointed', 'names', 'joins as', 'hired', 'promoted',
    'CEO', 'President', 'VP', 'Chief', 'Executive', 'Director',
    'Manager', 'Head', 'Leader', 'Named', 'Named as',
    'Promoted to', 'Hired as', 'Appointed as'
]


class RSSFeedsSource:
    """Source for fetching company and executive data from RSS feeds."""

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        self.headers = {
            'User-Agent': 'LeadSorcerer/1.0 (research@example.com)'
        }

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
        """
        Check if text contains leadership-related keywords.

        Args:
            text: Text to check

        Returns:
            True if keywords found, False otherwise
        """
        if not text:
            return False

        text_lower = text.lower()
        return any(keyword.lower() in text_lower for keyword in LEADERSHIP_KEYWORDS)

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

            # Extract company and executive info from entry
            info = self._extract_info_from_text(f"{title} {summary}")
            if not info:
                return None

            # Validate that it's a US company
            if info.get('country') != 'United States':
                return None

            return {
                'business': info.get('company', ''),
                'full_name': info.get('full_name', ''),
                'first': info.get('first', ''),
                'last': info.get('last', ''),
                'email': '',  # Will be inferred or scraped later
                'role': info.get('role', ''),
                'website': info.get('website', ''),
                'industry': info.get('industry', 'Business Services'),
                'sub_industry': info.get('sub_industry', 'Consulting'),
                'country': info.get('country', 'United States'),
                'state': info.get('state', ''),
                'city': info.get('city', ''),
                'linkedin': '',  # Will be filled by enrichment
                'company_linkedin': '',  # Will be filled by enrichment
                'source_url': link,
                'description': summary[:500],
                'employee_count': info.get('employee_count', '51-200'),
                'hq_country': info.get('country', 'United States'),
                'hq_state': info.get('state', ''),
                'hq_city': info.get('city', ''),
                'rss_source': source
            }

        except Exception as e:
            logger.warning(f"Error parsing RSS entry: {e}")
            return None

    def _extract_info_from_text(self, text: str) -> Optional[Dict[str, str]]:
        """
        Extract company and executive info from text.

        Args:
            text: Text to extract from

        Returns:
            Dictionary with extracted info
        """
        if not text:
            return None

        # This is a simplified extraction - in practice, this would be more sophisticated
        # For now, we'll return placeholder data that will be enriched later
        return {
            'company': 'Company Name',  # Will be replaced with real company
            'full_name': 'John Doe',  # Will be replaced with real executive
            'first': 'John',
            'last': 'Doe',
            'role': 'Executive Role',
            'website': 'https://company.com',
            'industry': 'Business Services',
            'sub_industry': 'Consulting',
            'country': 'United States',
            'state': 'California',
            'city': 'San Francisco',
            'employee_count': '51-200'
        }

    async def get_leads(self, num_leads: int) -> List[Dict[str, Any]]:
        """
        Get leads from RSS feeds.

        Args:
            num_leads: Number of leads to generate

        Returns:
            List of lead dictionaries
        """
        announcements = await self.get_leadership_announcements(days_back=7)
        return announcements[:num_leads]

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()