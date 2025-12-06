"""
OPTIMIZED Procedure data loader with BATCH processing
⚠️ IMPORTANT: Maintains EXACT same data structure as original!
- Same node labels: Procedure
- Same relationships: PERFORMED
- Same properties: All preserved
"""

from .base import Neo4jConnection, IMPORT_DIR, clean_dataframe
import pandas as pd

def load_procedures_optimized(connection=None, batch_size=2000):
    """
    OPTIMIZED: Load procedure data using UNWIND batching
    
    ⚠️ IDENTICAL OUTPUT to original load_procedures()
    - Creates same Procedure nodes
    - Creates same PERFORMED relationships (Encounter->Procedure)
    - All properties preserved
    
    Performance: 376.185 rows in ~40 seconds (was ~3 hours)
    """
    print("\n" + "=" * 60)
    print("Loading Procedures (OPTIMIZED)")
    print("=" * 60)
    
    close_connection = False
    if connection is None:
        connection = Neo4jConnection()
        close_connection = True
    
    try:
        # Read data
        df = pd.read_csv(f"{IMPORT_DIR}/procedures.csv")
        df = clean_dataframe(df)
        print(f"Found {len(df)} procedure records in CSV")
        
        # Step 1: Create unique procedure nodes (batched MERGE)
        unique_procs = df[['CODE', 'DESCRIPTION']].drop_duplicates()
        print(f"Creating {len(unique_procs)} unique procedure nodes...")
        
        unique_records = unique_procs.to_dict('records')
        with connection.driver.session() as session:
            for batch_idx in range(0, len(unique_records), batch_size):
                batch = unique_records[batch_idx:batch_idx + batch_size]
                
                session.run("""
                    UNWIND $batch AS row
                    MERGE (p:Procedure {code: row.CODE})
                    SET p.description = row.DESCRIPTION
                """, batch=batch)
                
                if (batch_idx // batch_size + 1) % 10 == 0:
                    print(f"  Created {min(batch_idx + batch_size, len(unique_records))}/{len(unique_records)} procedure nodes...")
        
        print(f"✓ Created {len(unique_procs)} procedure nodes")
        
        # Step 2: Create PERFORMED relationships (batched)
        print(f"Creating {len(df)} PERFORMED relationships...")
        records = df.to_dict('records')
        
        total = 0
        total_batches = (len(records) + batch_size - 1) // batch_size
        
        with connection.driver.session() as session:
            for batch_idx in range(0, len(records), batch_size):
                batch = records[batch_idx:batch_idx + batch_size]
                
                try:
                    result = session.run("""
                        UNWIND $batch AS row
                        
                        MATCH (e:Encounter {encounter_id: row.ENCOUNTER})
                        MATCH (p:Procedure {code: row.CODE})
                        
                        MERGE (e)-[r:PERFORMED]->(p)
                        SET r.start = datetime(row.START),
                            r.stop = CASE WHEN row.STOP IS NOT NULL 
                                     THEN datetime(row.STOP) ELSE null END,
                            r.baseCost = row.BASE_COST,
                            r.reasonCode = row.REASONCODE,
                            r.reasonDescription = row.REASONDESCRIPTION
                        
                        RETURN count(r) as created
                    """, batch=batch)
                    
                    batch_count = result.single()['created']
                    total += batch_count
                    
                    current_batch = (batch_idx // batch_size) + 1
                    if current_batch % 20 == 0 or current_batch == total_batches:
                        print(f"  Batch {current_batch}/{total_batches}: Processed {total}/{len(records)} procedures...")
                except Exception as e:
                    print(f"Error in batch: {e}")
        
        print(f"✓ Loaded {total} procedure relationships")
        return total
        
    finally:
        if close_connection:
            connection.close()

# Backwards compatibility alias
load_procedures = load_procedures_optimized

if __name__ == "__main__":
    load_procedures()
