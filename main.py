import asyncio
import json
import os
from crawl4ai import AsyncWebCrawler
from urls import BIKANER_SOURCES

# Import pipeline steps
from clean_data import clean_data_pipeline
from process_llm import process_with_llm

# File paths
RAW_FILE = "bikaner_raw.json"
CLEAN_FILE = "bikaner_clean.json"
FINAL_FILE = "bikaner_final.json"
TEMPLATE_FILE = "schema_template.json" # Lightweight schema


async def scrape_data():
    """Step 1: Scrape data from sources."""
    print("\n🚀 Step 1: Starting Scraping...")
    collected_data = []

    async with AsyncWebCrawler(
        headless=True,
        excluded_tags=["nav", "footer", "header", "aside"],
        excluded_selectors=[
            ".footer", ".header", ".navbar", ".nav", ".breadcrumbs",
            ".social-share", ".related-links", ".cookie", ".menu"
        ]
    ) as crawler:

        for site in BIKANER_SOURCES:
            print(f"   🔍 Scraping: {site['source']}")
            result = await crawler.arun(url=site["url"])
            
            collected_data.append({
                "destination": "Bikaner",
                "source": site["source"],
                "source_url": site["url"],
                "raw_content": result.markdown.raw_markdown
            })
            print(f"   ✨ Fetched {len(result.markdown.raw_markdown)} chars from {site['source']}")

    print("   🛑 Crawler context exiting...")
    
    print("   💾 Saving raw data to file...")
    with open(RAW_FILE, "w", encoding="utf-8") as f:
        json.dump(collected_data, f, indent=4, ensure_ascii=False)
    
    print(f"   ✅ Scraping Complete. Saved to {RAW_FILE}")


def main_pipeline():
    # 1. Scrape (Async)
    asyncio.run(scrape_data())

    # 2. Clean
    print("\n🧹 Step 2: Cleaning Data...")
    if clean_data_pipeline(RAW_FILE, CLEAN_FILE):
        print(f"   ✅ Cleaning Complete. Saved to {CLEAN_FILE}")
    else:
        print("   ❌ Cleaning Failed. Aborting.")
        return

    # 3. LLM Processing
    print("\n🤖 Step 3: LLM Formatting...")
    if process_with_llm(CLEAN_FILE, FINAL_FILE, TEMPLATE_FILE):
        print(f"   ✅ LLM Processing Complete. Final data in {FINAL_FILE}")
    else:
        print("   ❌ LLM Processing Failed.")

if __name__ == "__main__":
    main_pipeline()
