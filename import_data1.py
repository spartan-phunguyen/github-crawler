import os
import psycopg2
from psycopg2 import sql
import uuid
import psycopg2.extras

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

# Create table if not exists
def create_table(conn):
    with conn.cursor() as cur:
        # Create the uuid-ossp extension if it doesn't exist
        cur.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS personas (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                name VARCHAR(255) NOT NULL,
                domain VARCHAR(255) NOT NULL,
                style TEXT
            );
        """)
    conn.commit()
    print("Table created successfully")

# Insert data into table
def insert_data(conn, name, domain, comment):
    # Skip persona with "No analysis available" comments
    if "No analysis available" in comment:
        print(f"Skipping expert {name} - No analysis available")
        return None
        
    with conn.cursor() as cur:
        # Generate a random UUID
        expert_id = uuid.uuid4()
        
        cur.execute("""
            INSERT INTO personas (id, name, domain, style)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
        """, (expert_id, name, domain, comment))
        returned_id = cur.fetchone()[0]
    conn.commit()
    print(f"Inserted expert {name} with ID {returned_id}")
    return returned_id

# Process directory and insert data
def process_directory(base_dir, conn):
    valid_persona_count = 0
    skipped_persona_count = 0
    
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('_tone_analysis.txt'):
                # Extract name and domain from path
                path_parts = root.split(os.sep)
                
                # Find the indices of key parts in the path
                try:
                    domain_index = path_parts.index('tone_analysis') + 1
                    name = os.path.basename(root)  # Get the expert name from the folder name
                    
                    if domain_index < len(path_parts):
                        domain = path_parts[domain_index]  # e.g., 'android'
                    else:
                        domain = "unknown"
                    
                    # Read the comment from the file
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            comment = f.read()
                        
                        # Insert into database, but only if it's valid
                        expert_id = insert_data(conn, name, domain, comment)
                        if expert_id:
                            valid_persona_count += 1
                            print(f"Processed {file_path}")
                        else:
                            skipped_persona_count += 1
                    except Exception as e:
                        print(f"Error processing {file_path}: {e}")
                except ValueError:
                    print(f"Could not determine domain and name from path: {root}")
    
    print(f"\nSummary: {valid_persona_count} persona imported, {skipped_persona_count} persona skipped")

def main():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        print("Connected to PostgreSQL")
        
        # Create table
        create_table(conn)
        
        # Process the directory structure
        base_dir = 'data'  # Start from the data directory
        if not os.path.exists(base_dir):
            base_dir = '.'  # If data doesn't exist at root, start from current directory
            
        process_directory(base_dir, conn)
        
        print("Data import completed successfully")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    main() 