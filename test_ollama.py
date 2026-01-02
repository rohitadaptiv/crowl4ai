import requests
import json
import os
import sys

# Use the same env var as the main app to ensure we test the same path
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/chat")

def test_connection():
    print(f"🧪 Testing connection to Ollama at: {OLLAMA_URL}")
    print(f"   Model: llama3.1")
    
    payload = {
        "model": "llama3.1",
        "messages": [
            {"role": "user", "content": "Just say 'Hello, connection is working!' and nothing else."}
        ],
        "stream": True
    }
    
    try:
        print("⏳ Sending test request (Timeout: 60s)...")
        with requests.post(OLLAMA_URL, json=payload, stream=True, timeout=60) as response:
            if response.status_code == 200:
                print("✅ Connection Established! Streaming response:")
                print("------------------------------------------------")
                for line in response.iter_lines():
                    if line:
                        try:
                            data = json.loads(line.decode('utf-8'))
                            content = data.get("message", {}).get("content", "")
                            print(content, end="", flush=True)
                        except:
                            pass
                print("\n------------------------------------------------")
                print("✅ Test Passed.")
            else:
                print(f"❌ HTTP Error: {response.status_code}")
                print(response.text)
    except requests.exceptions.ConnectionError:
        print(f"❌ Failed to connect to {OLLAMA_URL}")
        print("   Is the ollama container running?")
        print("   Check: docker ps")
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")

if __name__ == "__main__":
    test_connection()
