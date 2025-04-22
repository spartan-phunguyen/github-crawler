import psycopg2
import argparse

# Database connection parameters
DB_PARAMS = {
    'dbname': 'local',
    'user': 'local',
    'password': 'local',
    'host': 'localhost',
    'port': '5432'
}

def list_experts(conn):
    """List all experts in the database"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, name, domain FROM experts
            ORDER BY domain, name;
        """)
        rows = cur.fetchall()
        
    print(f"\n{'ID':<5} {'Name':<25} {'Domain':<15}")
    print('-' * 45)
    for row in rows:
        print(f"{row[0]:<5} {row[1]:<25} {row[2]:<15}")
    
    print(f"\nTotal experts: {len(rows)}")

def view_expert_comment(conn, expert_id):
    """View the comment for a specific expert"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT name, domain, comment FROM experts
            WHERE id = %s;
        """, (expert_id,))
        row = cur.fetchone()
        
    if row:
        print(f"\nExpert: {row[0]} (Domain: {row[1]})")
        print('-' * 60)
        print(row[2])
    else:
        print(f"No expert found with ID {expert_id}")

def search_comments(conn, search_term):
    """Search for comments containing a specific term"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, name, domain FROM experts
            WHERE comment ILIKE %s
            ORDER BY domain, name;
        """, (f'%{search_term}%',))
        rows = cur.fetchall()
        
    if rows:
        print(f"\nExperts with comments containing '{search_term}':")
        print(f"{'ID':<5} {'Name':<25} {'Domain':<15}")
        print('-' * 45)
        for row in rows:
            print(f"{row[0]:<5} {row[1]:<25} {row[2]:<15}")
        print(f"\nTotal matches: {len(rows)}")
    else:
        print(f"No comments found containing '{search_term}'")

def delete_expert(conn, expert_id):
    """Delete an expert from the database"""
    # First, get the expert details to confirm deletion
    with conn.cursor() as cur:
        cur.execute("""
            SELECT name, domain FROM experts
            WHERE id = %s;
        """, (expert_id,))
        row = cur.fetchone()
    
    if not row:
        print(f"No expert found with ID {expert_id}")
        return
    
    # Delete the expert
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM experts
            WHERE id = %s;
        """, (expert_id,))
    
    conn.commit()
    print(f"Deleted expert {row[0]} (ID: {expert_id}) from domain {row[1]}")
    
    # Reset the sequence to ensure proper ID ordering for future inserts
    with conn.cursor() as cur:
        cur.execute("""
            SELECT setval('experts_id_seq', COALESCE((SELECT MAX(id) FROM experts), 0));
        """)
    
    conn.commit()
    print("Database sequence updated to maintain ID ordering")

def main():
    parser = argparse.ArgumentParser(description='Query expert comments database')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all experts')
    
    # View command
    view_parser = subparsers.add_parser('view', help='View a specific expert comment')
    view_parser.add_argument('id', type=int, help='Expert ID to view')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search for comments containing a term')
    search_parser.add_argument('term', help='Term to search for in comments')
    
    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete an expert from the database')
    delete_parser.add_argument('id', type=int, help='Expert ID to delete')
    
    args = parser.parse_args()
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        
        if args.command == 'list':
            list_experts(conn)
        elif args.command == 'view' and hasattr(args, 'id'):
            view_expert_comment(conn, args.id)
        elif args.command == 'search' and hasattr(args, 'term'):
            search_comments(conn, args.term)
        elif args.command == 'delete' and hasattr(args, 'id'):
            delete_expert(conn, args.id)
        else:
            parser.print_help()
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main() 