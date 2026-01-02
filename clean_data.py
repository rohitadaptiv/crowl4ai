import json
import re
import sys

def extract_main_text(text: str) -> str:
    """Extracts only the relevant main narrative text."""
    start_marker = "## Exploring the red city"
    end_marker = "## Attractions"

    if start_marker in text:
        text = text.split(start_marker, 1)[1]
    
    if end_marker in text:
        text = text.split(end_marker, 1)[0]
        
    return text.strip()

def clean_text_content(text: str) -> str:
    """Cleans markdown artifacts from text."""
    text = re.sub(r"!\[.*?\]\(.*?\)", "", text)          # images
    text = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", text)     # links
    text = re.sub(r"Read more", "", text, flags=re.I)
    text = re.sub(r"\n{2,}", "\n\n", text)
    return text.strip()

def generate_summary(text: str) -> str:
    """Generates a smart extractive summary."""
    sentences = re.split(r"(?<=[.!?])\s+", text)
    
    picked = []
    keywords = [
        "located", "founded", "known", "famous", 
        "heritage", "fort", "festival", 
        "October", "March"
    ]
    
    for s in sentences:
        s = s.strip()
        if len(s) < 40: 
            continue
        if any(k.lower() in s.lower() for k in keywords):
            picked.append(s)
        if len(picked) == 4:
            break
            
    if len(picked) < 3:
        picked = sentences[:3]
        
    return " ".join(picked)

def extract_attractions(text: str):
    """Extracts attractions list specifically for Incredible India structure."""
    attractions = []
    
    if "## Attractions" not in text:
        return attractions
        
    section = text.split("## Attractions", 1)[1]
    lines = section.split("\n")
    
    STOP_TITLES = {
        "a trove of",
        "experiences",
        "itineraries",
        "destinations nearby"
    }
    
    for line in lines:
        line = line.strip()
        
        if not line.startswith("## "):
            continue
            
        title = line.replace("##", "").strip().lower()
        
        # HARD STOP: attractions list ended
        if title in STOP_TITLES:
            break
            
        match = re.search(r"\[(.*?)\]\(", line)
        if match:
            name = match.group(1).strip()
        else:
            name = line.replace("##", "").strip()
            
        name = re.sub(r"\(https?://.*?\)", "", name)
        name = name.strip("[] ")
        
        if len(name) < 5:
            continue
            
        attractions.append(name)
        
    return list(dict.fromkeys(attractions))

def clean_data_pipeline(input_file: str, output_file: str):
    """
    Main function to run the cleaning pipeline.
    Reads from input_file, processes content, writes to output_file.
    """
    print(f"🧹 Starting cleaning process: {input_file} -> {output_file}")
    
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Input file {input_file} not found.")
        return False
        
    all_descriptions = []
    all_attractions = []
    sources_used = []
    
    for item in raw_data:
        raw_text = item.get("raw_content", "")
        sources_used.append(item.get("source_url"))
        
        # Clean description
        main_text = extract_main_text(raw_text)
        cleaned_text = clean_text_content(main_text)
        
        if cleaned_text:
            all_descriptions.append(cleaned_text)
            
        # Extract attractions
        attractions = extract_attractions(raw_text)
        all_attractions.extend(attractions)
        
    # Merge & deduplicate
    final_description = "\n\n".join(all_descriptions)
    final_attractions = list(dict.fromkeys(all_attractions))
    
    if not final_description:
        print("⚠️ Warning: No description content extracted.")
    
    final_output = {
        "destination": "Bikaner",
        "state": "Rajasthan",
        "summary": generate_summary(final_description),
        "full_description": final_description,
        "attractions": final_attractions,
        "sources": sources_used
    }
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=4, ensure_ascii=False)
        
    print(f"✅ Cleaned data saved successfully to {output_file}")
    return True

if __name__ == "__main__":
    # Allow running directly for testing
    clean_data_pipeline("bikaner_raw.json", "bikaner_clean.json")
