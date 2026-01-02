import json
import requests
import re
import time
import os

# Configuration
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/chat")
MODEL = "llama3.1"

def query_ollama(prompt, context_text):
    """Sends a request to local Ollama instance with streaming enabled."""
    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "system", 
                "content": "You are a helpful data extraction assistant. Your task is to extract travel information about Bikaner from the provided text and format it into a specific JSON structure. Do NOT invent information. If information is missing, output null or a generic sensible description based on the context."
            },
            {
                "role": "user",
                "content": f"""
Please generate a valid JSON object for "Bikaner" (Rajasthan, India) based on the text provided below.
The output JSON MUST have the EXACT same structure and fields as this example (for Jaipur):

{prompt}

TEXT TO EXTRACT FROM:
{context_text}

IMPORTANT RULES:
1. Output ONLY valid JSON. No markdown formatting (like ```json), no explanations.
2. Ensure all fields from the example are present.
3. For "images", use placeholder URLs or extract if available in text.
4. "details" object must have all the keys: history, bestTime, weather, attractions, forFamily, forCouples, forSolo, forAdventure, forVloggers, culture, food, safety, cost, crowd.
"""
            }
        ],
        "stream": True, # Enable streaming
        "options": {
            "temperature": 0.2, 
            "num_ctx": 40000 
        }
    }
    
    # Calculate Total Input Stats
    total_input_char = len(prompt) + len(context_text) + 500 # +500 for system instructions approx
    total_input_token = int(total_input_char / 4)
    print(f"   📊 Total LLM Input: {total_input_char:,} chars ≈ {total_input_token:,} tokens.")
    
    try:
        print("⏳ Sending request to Ollama... (Response will stream below)")
        # Use stream=True in requests
        with requests.post(OLLAMA_URL, json=payload, stream=True) as response:
            response.raise_for_status()
            
            full_response = ""
            print("🤖 LLM Output: ", end="", flush=True)

            for line in response.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')
                    try:
                        json_chunk = json.loads(decoded_line)
                        # Ollama chat endpoint structure: {"model":..., "message":{"role":"assistant", "content":"..."}, "done":false}
                        content = json_chunk.get("message", {}).get("content", "")
                        
                        # Print to console immediately
                        print(content, end="", flush=True)
                        
                        # Accumulate full text for processing
                        full_response += content
                        
                        if json_chunk.get("done", False):
                            print("\n✅ Generation Complete.")
                            
                    except json.JSONDecodeError:
                        pass
            
            return full_response

    except requests.exceptions.ConnectionError:
        print("\n❌ Could not connect to Ollama. Is it running?")
        print("Try running: ollama serve")
        return None
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return None

def process_with_llm(input_file: str, output_file: str, template_file: str):
    """
    Reads clean data, reads the schema template, and queries Ollama.
    """
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return False

    if not os.path.exists(template_file):
        print(f"❌ Template file not found: {template_file}")
        return False
        
    print(f"📖 Reading input data from {input_file}...")
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Use 'full_description' as primary context, fallback to summary
    context_text = data.get("full_description", "")
    if not context_text:

    print(f"📖 Context length: {len(context_text)} chars")

    # 2. Read Template
    try:
        with open(template_file, "r") as f:
            template_data = json.load(f)
            template_str = json.dumps(template_data, indent=2)
    except FileNotFoundError:
        print(f"❌ Template file {template_file} not found.")
        return False

    # 3. Process with LLM
    json_output_str = query_ollama(template_str, context_text)

    if json_output_str:
        # 4. Validate and Save
        try:
            # Clean up potential markdown formatting
            json_output_str = re.sub(r"^```json", "", json_output_str.strip())
            json_output_str = re.sub(r"^```", "", json_output_str.strip())
            json_output_str = re.sub(r"```$", "", json_output_str.strip())
            
            final_data = json.loads(json_output_str)
            
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(final_data, f, indent=4, ensure_ascii=False)
            print(f"✅ LLM formatted data saved successfully to {output_file}")
            return True
            
        except json.JSONDecodeError:
            print("❌ Failed to parse LLM output as JSON.")
            print("Raw Output snippet:", json_output_str[:500])
            return False
    else:
        return False

if __name__ == "__main__":
    # Allow running directly for testing
    process_with_llm("bikaner_clean.json", "bikaner_final.json", "jaipur.json")
