from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams, Distance, PointStruct

# Connect to Qdrant
client = QdrantClient(url="http://localhost:6333", api_key="local")
# Alternative if running in Docker: client = QdrantClient(host="qdrant", port=6333, api_key="local")

def rename_vector(collection_name, new_vector_name, vector_size=1536, distance=Distance.COSINE):
    """
    Rename the default unnamed vector to a named vector in a Qdrant collection
    
    Args:
        collection_name: Name of the collection to modify
        new_vector_name: New name for the vector
        vector_size: Size of the vector (must match existing)
        distance: Distance metric to use (must match existing)
    """
    print(f"Starting vector rename process for collection: {collection_name}")
    
    # Get collection info to confirm vector size
    collection_info = client.get_collection(collection_name=collection_name)
    total_points = collection_info.points_count
    print(f"Collection has {total_points} points")
    
    # Step 1: Fetch all points with current vector name
    print("Fetching all points...")
    
    all_points = []
    offset = None
    batch_size = 100
    
    while True:
        search_result = client.scroll(
            collection_name=collection_name,
            offset=offset,
            limit=batch_size,
            with_payload=True,
            with_vectors=True,
        )
        
        points = search_result[0]
        next_offset = search_result[1]
        
        if not points:
            break
            
        print(f"Fetched batch of {len(points)} points")
        all_points.extend(points)
        
        if len(all_points) >= total_points:
            break
            
        offset = next_offset
    
    print(f"Total points fetched: {len(all_points)}")
    
    # Step 2: Create new points with renamed vector
    print(f"Preparing points with new vector name: {new_vector_name}")
    new_points = []
    
    for point in all_points:
        if point.vector is not None:
            new_point = PointStruct(
                id=point.id,
                vector={new_vector_name: point.vector},  # Rename the vector here
                payload=point.payload
            )
            new_points.append(new_point)
    
    print(f"Prepared {len(new_points)} points with renamed vector")
    
    # Step 3: Create a temporary collection name
    temp_collection = f"{collection_name}_temp"
    
    # Step 4: Create the new collection with named vector
    print(f"Creating temporary collection: {temp_collection}")
    client.recreate_collection(
        collection_name=temp_collection,
        vectors_config={
            new_vector_name: VectorParams(size=vector_size, distance=distance)
        }
    )
    
    # Step 5: Insert the points into temporary collection
    print("Inserting points into temporary collection...")
    batch_size = 100
    for i in range(0, len(new_points), batch_size):
        batch = new_points[i:i+batch_size]
        client.upsert(
            collection_name=temp_collection,
            points=batch
        )
        print(f"Inserted batch {i//batch_size + 1}/{(len(new_points) + batch_size - 1)//batch_size}")
    
    # Step: 6: Confirm points were transferred correctly
    temp_info = client.get_collection(collection_name=temp_collection)
    print(f"Temporary collection has {temp_info.points_count} points")
    
    # If you want to replace the original collection
    if input(f"Replace original collection '{collection_name}' with temporary collection? (y/n): ").lower() == 'y':
        # Delete original collection
        print(f"Deleting original collection: {collection_name}")
        client.delete_collection(collection_name=collection_name)
        
        # Recreate with the same configuration as temp
        print(f"Creating new collection: {collection_name}")
        client.recreate_collection(
            collection_name=collection_name,
            vectors_config={
                new_vector_name: VectorParams(size=vector_size, distance=distance)
            }
        )
        
        # Transfer all points from temp to new
        print(f"Transferring points from temporary to original collection name...")
        offset = None
        transferred = 0
        
        while True:
            search_result = client.scroll(
                collection_name=temp_collection,
                offset=offset,
                limit=batch_size,
                with_payload=True,
                with_vectors=True,
            )
            
            points = search_result[0]
            next_offset = search_result[1]
            
            if not points:
                break
                
            client.upsert(
                collection_name=collection_name,
                points=points
            )
            
            transferred += len(points)
            print(f"Transferred {transferred}/{temp_info.points_count} points")
            
            if transferred >= temp_info.points_count:
                break
                
            offset = next_offset
        
        # Delete temporary collection
        print(f"Deleting temporary collection: {temp_collection}")
        client.delete_collection(collection_name=temp_collection)
        
        print(f"Successfully renamed vector in collection: {collection_name}")
    else:
        print(f"Kept both collections. Original: {collection_name}, New: {temp_collection}")

if __name__ == "__main__":
    # Get user input
    # collection_name = input("Enter the collection name: ")
    # new_vector_name = input("Enter the new vector name (e.g., 'dense_vector'): ")
    # vector_size = int(input("Enter vector size (e.g., 1536): "))
    
    # Execute the vector renaming
    rename_vector("github_experts_all", "dense_vector", 1536) 