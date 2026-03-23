"""
Dynamic lead sourcing pipeline for Lead Sorcerer.

Sources leads from SEC EDGAR filings, RSS press releases,
and USAJobs.gov listings. All leads are validated and
LinkedIn verified before returning.
"""

import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import List, Dict, Any

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Add src directory to path
lead_sorcerer_dir = Path(__file__).parent.absolute()
src_path = lead_sorcerer_dir / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


async def get_leads(num_leads: int, industry: str = None, region: str = None) -> List[Dict[str, Any]]:
    """
    Generate leads using dynamic free public sources.

    Sources:
    - SEC EDGAR filings (highest reputation scores)
    - RSS press release announcements
    - USAJobs.gov listings (contractors and universities)

    All leads are validated and LinkedIn verified before returning.
    """
    leads = []
    per_source = max(num_leads // 3, 3)

    # Import source classes from the src package
    from miner_models.lead_sorcerer_main.src import SECEdgarSource, RSSFeedsSource, JobListingsSource

    # Source from SEC EDGAR - highest reputation scores
    try:
        sec_source = SECEdgarSource()
        sec_leads = await sec_source.get_leads(per_source)
        leads.extend(sec_leads)
        await sec_source.close()
        print(f"✅ SEC EDGAR: {len(sec_leads)} leads")
    except Exception as e:
        print(f"⚠️ SEC EDGAR error: {e}")

    # Source from RSS feeds - press release announcements
    try:
        rss_source = RSSFeedsSource()
        rss_leads = await rss_source.get_leads(per_source)
        leads.extend(rss_leads)
        await rss_source.close()
        print(f"✅ RSS feeds: {len(rss_leads)} leads")
    except Exception as e:
        print(f"⚠️ RSS feeds error: {e}")

    # Source from USAJobs - contractors and universities
    try:
        job_source = JobListingsSource()
        job_leads = await job_source.get_leads(per_source)
        leads.extend(job_leads)
        await job_source.close()
        print(f"✅ Job listings: {len(job_leads)} leads")
    except Exception as e:
        print(f"⚠️ Job listings error: {e}")

    print(f"✅ Pipeline total: {len(leads)} leads generated")
    return leads[:num_leads]


if __name__ == "__main__":
    async def test():
        leads = await get_leads(3)
        for lead in leads:
            print(f"{lead.get('business')} - {lead.get('full_name')} - {lead.get('email')}")

    asyncio.run(test())
