import os
import psycopg2
import psycopg2.extras
import uuid
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, UpdateResult, PayloadSchemaType

# Register UUID adapter
psycopg2.extras.register_uuid()

# Database connection parameters
DB_PARAMS = {
    'dbname': 'local',
    'user': 'local',
    'password': 'local',
    'host': 'localhost',
    'port': '5432'
}

# Qdrant connection
qdrant_client = QdrantClient(url="http://localhost:6333", api_key="local")
# Alternative if running in Docker: qdrant_client = QdrantClient(host="qdrant", port=6333, api_key="local")

# Source collection name
COLLECTION_NAME = "github_experts_all_temp"

# Get all personas from PostgreSQL
def get_all_personas():
    persona_map = {}
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM personas")
            for row in cur.fetchall():
                persona_id, name = row
                persona_map[name] = persona_id
        print(f"Loaded {len(persona_map)} personas from PostgreSQL")
    except Exception as e:
        print(f"Error getting personas: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
    return persona_map

# Update Qdrant documents with expert_id from PostgreSQL
def update_qdrant_with_expert_ids(persona_map):
    # Get total count of entries in collection
    collection_info = qdrant_client.get_collection(collection_name=COLLECTION_NAME)
    total_entries = collection_info.points_count
    print(f"Collection {COLLECTION_NAME} has {total_entries} entries")
    
    # Track progress
    updated_count = 0
    removed_user_id_count = 0
    skipped_count = 0
    offset = None
    run = 0
    
    while True:
        print(f"Run {run}, Updated: {updated_count}/{total_entries}, Removed user_id: {removed_user_id_count}, Skipped: {skipped_count}")
        
        # Retrieve points with payload
        search_result = qdrant_client.scroll(
            collection_name=COLLECTION_NAME,
            offset=offset,
            limit=100,
            with_payload=True,
            with_vectors=False,
        )
        
        points = search_result[0]
        next_offset = search_result[1]
        
        if not points:
            print("No more points to update")
            break
        
        # Process each point
        for point in points:
            try:
                # First check if user_id exists and remove it
                if point.payload and "user_id" in point.payload:
                    qdrant_client.delete_payload(
                        collection_name=COLLECTION_NAME,
                        points=[point.id],
                        keys=["user_id"]
                    )
                    removed_user_id_count += 1
                    print(f"Removed user_id from point {point.id}") if removed_user_id_count % 50 == 0 else None
                
                # Skip if point already has expert_id
                if point.payload and "expert_id" in point.payload:
                    skipped_count += 1
                    continue
                    
                # Get expert name from payload
                if not point.payload or "expert_name" not in point.payload:
                    print(f"Point {point.id} has no expert_name, skipping")
                    skipped_count += 1
                    continue
                
                expert_name = point.payload["expert_name"]
                
                # Find matching persona
                if expert_name in persona_map:
                    expert_id = persona_map[expert_name]
                    
                    # Update the point with expert_id
                    try:
                        # Convert UUID to string for JSON serialization
                        qdrant_client.set_payload(
                            collection_name=COLLECTION_NAME,
                            payload={"expert_id": str(expert_id)},
                            points=[point.id]
                        )
                        updated_count += 1
                        if updated_count % 100 == 0:
                            print(f"Updated {updated_count} documents")
                    except Exception as e:
                        print(f"Error updating point {point.id}: {e}")
                else:
                    print(f"No matching persona found for expert {expert_name}")
                    skipped_count += 1
            except Exception as e:
                print(f"Error processing point {point.id}: {e}")
                skipped_count += 1
        
        # Get next batch with pagination
        offset = next_offset
        run += 1
    
    print(f"Update completed. Updated {updated_count}/{total_entries} entries, Removed user_id: {removed_user_id_count}, Skipped {skipped_count} entries.")

def main():
    try:
        # Get mapping of persona names to IDs
        persona_map = get_all_personas()
        
        if not persona_map:
            print("No personas found in database. Exiting.")
            return
            
        # Update Qdrant documents
        update_qdrant_with_expert_ids(persona_map)
        
        print("Data update completed successfully")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main() 