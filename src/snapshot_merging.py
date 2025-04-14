from qdrant_client import QdrantClient
from qdrant_client.http.models import SnapshotRecover, VectorParams, Distance, PointStruct

# For connecting to Qdrant in platform_net Docker network
# If running this script from the host machine, use localhost
# If running this script from another container in the same network, use the container name
client = QdrantClient(url="http://localhost:6333", api_key="local")
# Alternative if running in Docker: client = QdrantClient(host="qdrant", port=6333, api_key="local")

# Restore second snapshot
client.recover_snapshot(
    collection_name="temp_collection_2",
    location="file:///qdrant/snapshots/github_experts/github_experts-2619813113740152-2025-04-13-16-17-30.snapshot"
)

# Restore first snapshot
client.recover_snapshot(
    collection_name="temp_collection_1",
    location='file:///qdrant/snapshots/github_experts/github_experts-7259426770861160-2025-04-12-16-51-51.snapshot'
)

# Define vector parameters (adjust according to your data)
vector_params = VectorParams(size=1536, distance=Distance.COSINE)

# Create the target collection
client.recreate_collection(
    collection_name="github_experts_all",
    vectors_config=vector_params
)

def transfer_points(source_collection, target_collection):
    # Get total count of entries in source collection
    collection_info = client.get_collection(collection_name=source_collection)
    total_source_entries = collection_info.points_count
    print(f"Source collection {source_collection} has {total_source_entries} entries")
    
    # Track how many we've transferred
    transferred_count = 0
    offset = None
    run = 0
    
    while transferred_count < total_source_entries:
        print(f"Collection: {source_collection}, Run {run}, Transferred: {transferred_count}/{total_source_entries}")
        # Step 1: Retrieve points with explicit vector and payload retrieval
        search_result = client.scroll(
            collection_name=source_collection,
            offset=offset,
            limit=100,
            with_payload=True,
            with_vectors=True,
        )
        
        points = search_result[0]
        next_offset = search_result[1]
        
        if not points:
            print(f"No more points to transfer")
            break
        
        try:
            # Step 2: Prepare points for upsert
            points_to_transfer = [
                PointStruct(
                    id=point.id,
                    vector=point.vector,
                    payload=point.payload
                )
                for point in points
                if point.vector is not None
            ]
            
            valid_points = len(points_to_transfer)
            print(f"Prepared {valid_points} points for transfer")
            
            # Step 3: Upsert into target collection
            if points_to_transfer:
                client.upsert(
                    collection_name=target_collection,
                    points=points_to_transfer
                )
                transferred_count += valid_points
                print(f"Successfully transferred {valid_points} points. Total: {transferred_count}/{total_source_entries}")
                
                # Break if we've transferred all entries or reached the total
                if transferred_count >= total_source_entries:
                    print(f"All entries transferred from {source_collection}")
                    break
        except Exception as e:
            print(f"Error during transfer: {e}")
            break
        
        # Get next batch with pagination
        offset = next_offset
        run += 1
    
    print(f"Transfer completed for {source_collection}. Transferred {transferred_count}/{total_source_entries} entries.")

# Transfer points from both temporary collections
print("Starting transfer from temp_collection_1")
transfer_points("temp_collection_1", "github_experts_all")
print("Starting transfer from temp_collection_2")
transfer_points("temp_collection_2", "github_experts_all")

client.delete_collection(collection_name="temp_collection_1")
client.delete_collection(collection_name="temp_collection_2")
