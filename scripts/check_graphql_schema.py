"""Script to check GraphQL schema for Polymarket subgraph."""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from httpx import AsyncClient
from src.utils.config import THE_GRAPH_API_URL, THE_GRAPH_SUBGRAPH_ID


async def check_schema():
    """Check GraphQL schema using introspection query."""
    print("=" * 80)
    print("Checking The Graph API Schema")
    print("=" * 80)
    print(f"Subgraph ID: {THE_GRAPH_SUBGRAPH_ID}")
    print(f"API URL: {THE_GRAPH_API_URL}")
    print("-" * 80)
    
    # Introspection query to get all available fields
    introspection_query = """
    query IntrospectionQuery {
        __schema {
            queryType {
                name
                fields {
                    name
                    description
                    type {
                        name
                        kind
                        ofType {
                            name
                            kind
                        }
                    }
                }
            }
        }
    }
    """
    
    payload = {
        "query": introspection_query,
    }
    
    headers = {"Content-Type": "application/json"}
    
    async with AsyncClient(timeout=30.0) as client:
        print("\n1. Sending introspection query...")
        try:
            response = await client.post(THE_GRAPH_API_URL, json=payload, headers=headers)
            response.raise_for_status()
            result = response.json()
            
            print("\n2. Response:")
            print(json.dumps(result, indent=2))
            
            if "data" in result and result["data"]:
                schema = result["data"].get("__schema", {})
                query_type = schema.get("queryType", {})
                fields = query_type.get("fields", [])
                
                print("\n3. Available Query fields:")
                print("-" * 80)
                for field in fields:
                    field_name = field.get("name", "")
                    field_type = field.get("type", {})
                    type_name = field_type.get("name") or field_type.get("ofType", {}).get("name", "Unknown")
                    print(f"  - {field_name}: {type_name}")
                
                # Check for trade-related fields
                print("\n4. Trade-related fields:")
                print("-" * 80)
                trade_keywords = ["trade", "position", "fill", "order", "transaction", "event"]
                found_fields = []
                for field in fields:
                    field_name = field.get("name", "").lower()
                    if any(keyword in field_name for keyword in trade_keywords):
                        found_fields.append(field.get("name", ""))
                        print(f"  ✓ {field.get('name', '')}")
                
                if not found_fields:
                    print("  ✗ No trade-related fields found!")
                    
        except Exception as e:
            print(f"\n✗ Error: {e}")
            print(f"Response: {response.text if 'response' in locals() else 'No response'}")
        
        # Try to query with different field names
        print("\n5. Testing different field names...")
        print("-" * 80)
        
        test_fields = ["trades", "positions", "fills", "orders", "transactions", "events"]
        for field_name in test_fields:
            test_query = f"""
            query TestQuery {{
                {field_name}(first: 1) {{
                    id
                }}
            }}
            """
            test_payload = {"query": test_query}
            try:
                test_response = await client.post(THE_GRAPH_API_URL, json=test_payload, headers=headers)
                test_result = test_response.json()
                if "errors" in test_result:
                    error_msg = test_result["errors"][0].get("message", "")
                    if "no field" in error_msg.lower():
                        print(f"  ✗ {field_name}: Field not found")
                    else:
                        print(f"  ? {field_name}: {error_msg[:100]}")
                else:
                    print(f"  ✓ {field_name}: SUCCESS!")
                    print(f"     Response: {json.dumps(test_result, indent=2)[:200]}...")
            except Exception as e:
                print(f"  ✗ {field_name}: Exception - {e}")


if __name__ == "__main__":
    asyncio.run(check_schema())

