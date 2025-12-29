"""Test script to check GraphQL schema for Polymarket The Graph subgraph."""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from httpx import AsyncClient
from src.utils.config import THE_GRAPH_API_URL


async def test_graphql_schema():
    """Test GraphQL schema introspection to find available fields."""
    print("=" * 80)
    print("Testing The Graph API GraphQL Schema")
    print("=" * 80)
    print(f"API URL: {THE_GRAPH_API_URL}")
    print("-" * 80)
    
    # Introspection query to get schema
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
        
        # Try a simple query to see what's available
        print("\n4. Testing simple queries...")
        print("-" * 80)
        
        # Try different possible field names
        test_queries = [
            ("trades", "query { trades(first: 1) { id } }"),
            ("trade", "query { trade(id: \"test\") { id } }"),
            ("positions", "query { positions(first: 1) { id } }"),
            ("fills", "query { fills(first: 1) { id } }"),
            ("orders", "query { orders(first: 1) { id } }"),
        ]
        
        for field_name, query in test_queries:
            print(f"\n   Testing '{field_name}'...")
            test_payload = {"query": query}
            try:
                test_response = await client.post(THE_GRAPH_API_URL, json=test_payload, headers=headers)
                test_result = test_response.json()
                if "errors" in test_result:
                    print(f"     ✗ Error: {test_result['errors']}")
                else:
                    print(f"     ✓ Success! Response: {json.dumps(test_result, indent=2)[:200]}...")
            except Exception as e:
                print(f"     ✗ Exception: {e}")


if __name__ == "__main__":
    asyncio.run(test_graphql_schema())

