#!/usr/bin/env python3
import os
import json
import urllib.request
import urllib.error
import urllib.parse

# Simple .env parser to avoid requiring external libraries if run outside a venv
def load_env():
    # Try looking in the same directory (if script is in root)
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(env_path):
        # Fallback to parent directory (if script is in scripts/)
        env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
        
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    if "=" in line:
                        k, v = line.split("=", 1)
                        # always update to let later duplicate keys override earlier ones
                        os.environ[k] = v.strip("""'" """)

load_env()

port = os.environ.get("GATEWAY_PORT", "8000")
GATEWAY_URL = os.environ.get("GATEWAY_URL", f"http://127.0.0.1:{port}")
API_KEY = os.environ.get("GATEWAY_API_KEY", "")
ADMIN_KEY = os.environ.get("ADMIN_API_KEY", "")

def make_request(method, endpoint, payload=None, use_admin=False):
    url = f"{GATEWAY_URL}{endpoint}"
    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json"
    }
    if use_admin:
        headers["X-Admin-API-Key"] = ADMIN_KEY
        
    data = json.dumps(payload).encode("utf-8") if payload else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        print(f"Error {e.code}: {e.read().decode()}")
        return None
    except Exception as e:
        print(f"Request failed: {e}")
        return None

def menu_search():
    namespace = input("Enter namespace to search: ").strip()
    query = input("Enter search query: ").strip()
    if not namespace or not query:
        print("Namespace and query cannot be empty.")
        return
    
    payload = {
        "namespace": namespace,
        "query": query,
        "top_k": 5
    }
    print("\nSearching...")
    res = make_request("POST", "/search", payload)
    if res and "results" in res:
        print(f"\nFound {len(res['results'])} results:")
        for idx, r in enumerate(res['results'], 1):
            print(f"--- Result {idx} (Distance: {r.get('distance', 'N/A')}) ---")
            print(f"ID: {r.get('id')}")
            
            # Extract and truncate text for display
            text = r.get('chunk_text', '')
            if len(text) > 200:
                text = text[:197] + "..."
            print(f"Text: {text}")
            print(f"Metadata: {r.get('metadata_json', '{}')}")
    else:
        print("No results found or search failed.")

def menu_list_namespaces():
    res = make_request("GET", "/admin/namespaces", use_admin=True)
    if res and "namespaces" in res:
        namespaces = res["namespaces"]
        if namespaces:
            print("\nAvailable Namespaces:")
            for ns in namespaces:
                print(f" - {ns}")
        else:
            print("\nNo namespaces found (Database might be empty).")

def menu_delete_namespace():
    namespace = input("Enter namespace to delete: ").strip()
    if not namespace:
        return
    confirm = input(f"Are you SURE you want to delete namespace '{namespace}'? (y/n): ")
    if confirm.lower() == 'y':
        # Need URL encoding for namespace in path
        safe_ns = urllib.parse.quote(namespace)
        res = make_request("DELETE", f"/admin/namespace/{safe_ns}", use_admin=True)
        if res:
            print(f"Success: {res}")

def menu_reinitialize():
    print("\n!!! WARNING: THIS WILL DELETE ALL VECTOR DATA ACROSS ALL SHARDS !!!")
    confirm = input("Type 'DELETE_ALL_VECTOR_DATA' to confirm: ")
    if confirm == "DELETE_ALL_VECTOR_DATA":
        payload = {"confirm": "DELETE_ALL_VECTOR_DATA"}
        res = make_request("POST", "/admin/reinitialize", payload, use_admin=True)
        if res:
            print(f"Success: {res}")
    else:
        print("Aborted.")

def menu_shards():
    res = make_request("GET", "/admin/shards", use_admin=True)
    if res:
        print(json.dumps(res, indent=2))

def main():
    if not API_KEY or not ADMIN_KEY:
        print("Warning: GATEWAY_API_KEY or ADMIN_API_KEY not found in .env. Some operations will fail.")
        
    while True:
        print("\n=== RAG Gateway Admin CLI ===")
        print("1. List All Namespaces")
        print("2. Search Vector DB (See RAG contents)")
        print("3. Delete a Namespace (Clear specific RAG contents)")
        print("4. Reinitialize Database (NUKE ALL DATA)")
        print("5. View Shard Status")
        print("6. Exit")
        
        choice = input("Select an option: ").strip()
        print()
        
        if choice == "1":
            menu_list_namespaces()
        elif choice == "2":
            menu_search()
        elif choice == "3":
            menu_delete_namespace()
        elif choice == "4":
            menu_reinitialize()
        elif choice == "5":
            menu_shards()
        elif choice == "6":
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
