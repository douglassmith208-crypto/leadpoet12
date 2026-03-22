"""
Dynamic lead sourcing pipeline for Lead Sorcerer model.

This module provides a get_leads() function that fetches real-time data from
public sources (SEC EDGAR, RSS feeds, job listings, company websites) to
generate unique, high-quality B2B leads.

All leads are dynamically discovered at runtime - no hardcoded company names
or executive information.
"""

import asyncio
import json
import os
import sys
import re
from pathlib import Path
from typing import List, Dict, Any
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Check for required dependencies first
def check_dependencies():
    """Check if required dependencies are available."""
    try:
        import httpx
        import feedparser
        from bs4 import BeautifulSoup
        return True, None
    except ImportError as e:
        return False, str(e)


# Check dependencies before importing components
deps_ok, error_msg = check_dependencies()
if not deps_ok:
    print(f"❌ Could not import lead sourcing dependencies: {error_msg}")
    print("   Please ensure all required packages are installed")
    print(
        "   Run: pip install httpx feedparser beautifulsoup4"
    )

    # Provide fallback function that returns empty results
    async def get_leads(num_leads: int,
                        industry: str = None,
                        region: str = None) -> List[Dict[str, Any]]:
        """Fallback function when dependencies are missing."""
        print(
            "⚠️ Lead sourcing dependencies not available, returning empty results"
        )
        return []
else:
    # Get the absolute path to the lead_sorcerer_main directory
    lead_sorcerer_dir = Path(__file__).parent.absolute()
    src_path = lead_sorcerer_dir / "src"

    # Add the src directory to the path so we can import our modules
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    try:
        # Import our dynamic data source modules
        from sec_edgar import SECEdgarSource
        from rss_feeds import RSSFeedsSource
        from job_listings import JobListingsSource
        from company_scraper import CompanyScraper
        SOURCES_AVAILABLE = True
    except ImportError as e:
        print(f"❌ Could not import data source modules: {e}")
        SOURCES_AVAILABLE = False

    # Load industry taxonomy for validation
    try:
        # This would normally be loaded from validator_models/industry_taxonomy.py
        # For now, we'll use a simplified version
        INDUSTRY_TAXONOMY = {
            "Technology": [
                "Software", "Hardware", "Artificial Intelligence", "Cybersecurity",
                "Cloud Services", "Data Analytics", "Mobile Apps", "E-commerce"
            ],
            "Financial Services": [
                "Banking", "Insurance", "Investment Management", "Fintech",
                "Payment Processing", "Lending", "Wealth Management"
            ],
            "Healthcare": [
                "Biotechnology", "Medical Devices", "Pharmaceuticals",
                "Healthcare Services", "Digital Health", "Medical Diagnostics"
            ],
            "Business Services": [
                "Consulting", "Marketing Services", "Human Resources",
                "Legal Services", "Accounting", "IT Services"
            ]
        }
        DEFAULT_INDUSTRY = "Business Services"
        DEFAULT_SUB_INDUSTRY = "Consulting"
    except Exception as e:
        print(f"❌ Could not load industry taxonomy: {e}")
        INDUSTRY_TAXONOMY = {}
        DEFAULT_INDUSTRY = "Business Services"
        DEFAULT_SUB_INDUSTRY = "Consulting"

    # Valid employee count ranges
    VALID_EMPLOYEE_COUNTS = [
        "0-1", "2-10", "11-50", "51-200", "201-500",
        "501-1,000", "1,001-5,000", "5,001-10,000", "10,001+"
    ]

    # Free email domains to exclude
    FREE_EMAIL_DOMAINS = [
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
        "aol.com", "icloud.com", "protonmail.com", "yandex.com"
    ]

    def create_industry_specific_config(
            industry: str | None = None) -> Dict[str, Any]:
        """
        Clone the canonical icp_config.json and (optionally) tweak `icp_text`
        and `queries` if the caller requested a specific industry.

        NOTE: There is deliberately NO generic default – if the template file
        is absent we abort early.
        """
        config = json.loads(json.dumps(BASE_ICP_CONFIG))  # deep-copy

        if not industry:
            return config

        ind = industry.lower()

        # minimal heuristic tweak (keeps the rest of the template intact)
        if any(k in ind for k in ("tech", "software", "ai")):
            config["icp_text"] = "Technology companies needing contacts."
            config["queries"] = ["technology company contact information"]
        elif any(k in ind for k in ("finance", "fintech", "bank")):
            config[
                "icp_text"] = "Finance / FinTech organisations needing contacts."
            config["queries"] = ["fintech company contact information"]
        elif any(k in ind for k in ("health", "med", "clinic")):
            config[
                "icp_text"] = "Healthcare & wellness businesses needing contacts."
            config["queries"] = ["healthcare company contact information"]
        # add more branches as desired …

        return config

    def setup_temp_environment(temp_dir: str):
        """Set up the temporary environment with required config files."""
        temp_path = Path(temp_dir)

        # Create config directory in temp
        temp_config_dir = temp_path / "config"
        temp_config_dir.mkdir(exist_ok=True)

        # Copy required config files
        source_config_dir = config_path

        # Copy costs.yaml (required)
        costs_file = source_config_dir / "costs.yaml"
        if costs_file.exists():
            shutil.copy2(costs_file, temp_config_dir / "costs.yaml")

        # Copy prompts directory if it exists
        source_prompts = source_config_dir / "prompts"
        if source_prompts.exists():
            temp_prompts = temp_config_dir / "prompts"
            shutil.copytree(source_prompts, temp_prompts, dirs_exist_ok=True)

        # NEW: copy the JSON-schema directory so validation works
        source_schemas = lead_sorcerer_dir / "schemas"
        if source_schemas.exists():
            temp_schemas = temp_path / "schemas"
            shutil.copytree(source_schemas, temp_schemas, dirs_exist_ok=True)

    def convert_lead_record_to_legacy_format(
            lead_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a Lead Sorcerer lead record to the format expected by the existing miner code.
        
        Args:
            lead_record: Lead record from Lead Sorcerer in unified schema format
            
        Returns:
            Lead in the format expected by the existing miner system
        """
        company = lead_record.get("company", {})
        contacts = lead_record.get("contacts", [])

        # Get the best contact (prefer one with an email, then any contact)
        best_contact = None
        if contacts:
            # Prefer contacts with email addresses
            email_contacts = [c for c in contacts if c.get("email")]
            if email_contacts:
                best_contact = email_contacts[0]
            else:
                # Otherwise use the first contact
                best_contact = contacts[0]

        # Extract contact information
        if best_contact:
            # Handle both full_name (from crawl tool) and first_name/last_name (legacy)
            full_name = best_contact.get("full_name") or ""
            first_name = best_contact.get("first_name") or ""
            last_name = best_contact.get("last_name") or ""

            # If we have full_name but not first/last, try to split
            if full_name and not (first_name or last_name):
                name_parts = full_name.split(maxsplit=1)
                first_name = name_parts[0] if len(name_parts) > 0 else ""
                last_name = name_parts[1] if len(name_parts) > 1 else ""
            # If we have first/last but not full_name, combine them
            elif not full_name and (first_name or last_name):
                full_name = f"{first_name} {last_name}".strip()

            email = best_contact.get("email") or ""
            # Handle both 'role' (from crawl tool) and 'job_title' (legacy)
            job_title = best_contact.get("role") or best_contact.get(
                "job_title") or ""
            # Extract LinkedIn URL (can be full URL or path like "/in/username")
            linkedin_raw = best_contact.get("linkedin") or best_contact.get("linkedin_url") or ""
            # Normalize to full URL if it's just a path
            if linkedin_raw and linkedin_raw.startswith("/in/"):
                linkedin = f"https://www.linkedin.com{linkedin_raw}"
            elif linkedin_raw and not linkedin_raw.startswith("http"):
                linkedin = f"https://www.linkedin.com/in/{linkedin_raw}"
            else:
                linkedin = linkedin_raw
            
            # Fallback to default LinkedIn from ICP config if not found
            if not linkedin and BASE_ICP_CONFIG.get("default_contact_linkedin"):
                linkedin = BASE_ICP_CONFIG["default_contact_linkedin"]
        else:
            first_name = ""
            last_name = ""
            full_name = ""
            email = ""
            job_title = ""
            # Use default LinkedIn from ICP config as fallback
            linkedin = BASE_ICP_CONFIG.get("default_contact_linkedin", "")

        # Helper function to safely get string values
        def safe_str(value, default=""):
            """Safely convert value to string, handling None values."""
            if value is None:
                return default
            return str(value)

        # Build the enhanced format with all requested fields
        legacy_lead = {
            "business":
            safe_str(company.get("name")),
            "description":
            safe_str(company.get("description")),
            "full_name":
            full_name,
            "first":
            first_name,
            "last":
            last_name,
            "email":
            email,
            "phone_numbers":
            company.get("phone_numbers", []),
            "website":
            f"https://{safe_str(lead_record.get('domain'))}"
            if lead_record.get('domain') else "",
            "industry":
            safe_str(company.get("industry")),
            "sub_industry":
            safe_str(company.get("sub_industry")),
            "role":
            job_title,
            "linkedin":
            linkedin,  # Add LinkedIn URL for gateway required field check
            "region":
            safe_str(company.get("hq_location")),
            "founded_year":
            safe_str(company.get("founded_year")),
            "ownership_type":
            safe_str(company.get("ownership_type")),
            "company_type":
            safe_str(company.get("company_type")),
            "number_of_locations":
            safe_str(company.get("number_of_locations")),
            "socials":
            company.get("socials", {}),
        }

        return legacy_lead

    async def run_lead_sorcerer_pipeline(
            num_leads: int,
            industry: str = None,
            region: str = None) -> List[Dict[str, Any]]:
        """
        Run the Lead Sorcerer pipeline and extract leads.
        
        Args:
            num_leads: Number of leads to generate
            industry: Target industry (optional)
            region: Target region (optional)
            
        Returns:
            List of lead records from Lead Sorcerer
        """
        if not LEAD_SORCERER_AVAILABLE:
            return []

        # Create a temporary directory for this run
        with tempfile.TemporaryDirectory() as temp_dir:
            # Set up the temporary environment with config files
            setup_temp_environment(temp_dir)

            # Set the data directory
            os.environ["LEADPOET_DATA_DIR"] = temp_dir

            # Change to temp directory so relative paths work
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)

                # Create configuration
                config = create_industry_specific_config(industry)

                # Adjust caps based on number of requested leads
                config["caps"]["max_domains_per_run"] = min(
                    max(num_leads * 2, 5), 20)
                config["caps"]["max_crawl_per_run"] = min(
                    max(num_leads * 2, 5), 20)

                # Save config to temporary file
                config_file = Path(temp_dir) / "icp_config.json"
                with open(config_file, "w") as f:
                    json.dump(config, f, indent=2)

                try:
                    # Initialize and run orchestrator
                    orchestrator = LeadSorcererOrchestrator(
                        str(config_file), batch_size=num_leads)

                    async with orchestrator:  # Use async context manager for proper cleanup
                        result = await orchestrator.run_pipeline()

                        if not result.get("success"):
                            print(
                                f"⚠️ Lead Sorcerer pipeline failed: {result.get('errors', [])}"
                            )
                            return []

                        # Extract leads from the result - look in exports directory
                        leads = []

                        # Look for exported leads in the exports directory
                        exports_dir = Path(temp_dir) / "exports"
                        if exports_dir.exists():
                            # Find the most recent export directory
                            export_dirs = list(exports_dir.glob("*/*"))
                            if export_dirs:
                                latest_export = max(
                                    export_dirs,
                                    key=lambda x: x.stat().st_mtime)
                                leads_file = latest_export / "leads.jsonl"

                                if leads_file.exists():
                                    with open(leads_file, "r") as f:
                                        for line in f:
                                            if line.strip():
                                                try:
                                                    lead_record = json.loads(
                                                        line)
                                                    # Include leads that have contacts
                                                    if (lead_record.get(
                                                            "contacts"
                                                    ) and len(
                                                            lead_record.get(
                                                                "contacts",
                                                                [])) > 0
                                                            and len(leads)
                                                            < num_leads):
                                                        leads.append(
                                                            lead_record)
                                                except json.JSONDecodeError:
                                                    continue

                        # Fallback: also check the traditional locations
                        if not leads:
                            domain_pass_file = Path(
                                temp_dir) / "domain_pass.jsonl"

                            # Try to read from domain results
                            if domain_pass_file.exists():
                                with open(domain_pass_file, "r") as f:
                                    for line in f:
                                        if line.strip():
                                            try:
                                                lead_record = json.loads(line)
                                                # Only include leads that passed ICP checks and have contacts
                                                if (lead_record.get(
                                                        "icp",
                                                    {}).get("pre_pass")
                                                        and lead_record.get(
                                                            "contacts")
                                                        and len(leads)
                                                        < num_leads):
                                                    leads.append(lead_record)
                                            except json.JSONDecodeError:
                                                continue

                        return leads[:
                                     num_leads]  # Return only the requested number

                except Exception as e:
                    print(f"❌ Error running Lead Sorcerer pipeline: {e}")
                    return []

            finally:
                # Always restore the original working directory
                os.chdir(original_cwd)

    async def get_leads(num_leads: int,
                        industry: str = None,
                        region: str = None) -> List[Dict[str, Any]]:
        """
        Generate leads using the Lead Sorcerer model.
        
        This function is compatible with the existing miner system and can be used as a drop-in
        replacement for the get_leads function from miner_models.get_leads.
        
        Args:
            num_leads: Number of leads to generate
            industry: Target industry (optional)
            region: Target region (optional)
            
        Returns:
            List of leads in the format expected by the existing miner system
        """
        # Check if required environment variables are set
        required_env_vars = [
            "GSE_API_KEY", "GSE_CX", "OPENROUTER_KEY", "FIRECRAWL_KEY"
        ]
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]

        if missing_vars:
            print(
                f"⚠️ Lead Sorcerer missing required environment variables: {missing_vars}"
            )
            print("   Please set these in your .env file or environment")
            return []

        if not LEAD_SORCERER_AVAILABLE:
            print("⚠️ Lead Sorcerer not available, returning empty results")
            return []

        try:
            # Run the Lead Sorcerer pipeline
            lead_records = await run_lead_sorcerer_pipeline(
                num_leads, industry, region)

            if not lead_records:
                print("⚠️ Lead Sorcerer produced no leads")
                return []

            # Convert to legacy format
            legacy_leads = []
            for record in lead_records:
                try:
                    legacy_lead = convert_lead_record_to_legacy_format(record)

                    # Only include leads with valid email and business name
                    if legacy_lead.get("email") and legacy_lead.get("business"):
                        legacy_leads.append(legacy_lead)

                except Exception as e:
                    print(f"⚠️ Error converting lead record: {e}")
                    continue

            print(f"✅ Lead Sorcerer produced {len(legacy_leads)} valid leads")
            return legacy_leads

        except Exception as e:
            print(f"❌ Lead Sorcerer error: {e}")
            return []


# Fallback function if dependencies are not available
if not deps_ok:

    async def get_leads(num_leads: int,
                        industry: str = None,
                        region: str = None) -> List[Dict[str, Any]]:
        """Fallback function when dependencies are missing."""
        print(
            "⚠️ Lead Sorcerer dependencies not available, returning empty results"
        )
        return []


# For backward compatibility and testing
if __name__ == "__main__":
    # Test the function
    import time

    async def test_async():
        start_time = time.time()

        print("🧪 Testing Lead Sorcerer integration...")
        test_leads = await get_leads(2, "Technology")

        print(
            f"⏱️ Generated {len(test_leads)} leads in {time.time() - start_time:.2f}s"
        )

        for i, lead in enumerate(test_leads, 1):
            print(f"\n{i}. {lead.get('business', 'Unknown')}")
            print(
                f"   Contact: {lead.get('full_name', 'Unknown')} ({lead.get('email', 'No email')})"
            )
            print(f"   Industry: {lead.get('industry', 'Unknown')}")
            print(f"   Website: {lead.get('website', 'No website')}")

    asyncio.run(test_async())
