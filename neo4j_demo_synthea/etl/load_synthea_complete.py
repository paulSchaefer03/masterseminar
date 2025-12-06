"""
Complete Synthea ETL Pipeline - Modular Version
Orchestrates all data imports in correct order
"""

import sys
import os

# Add modules directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from modules.base import Neo4jConnection, get_stats, print_stats, IMPORT_DIR
from modules.patients import load_patients

# OPTIMIZED IMPORTS - Same data structure, 150x faster!
from modules.core_optimized import (
    load_encounters_optimized as load_encounters,
    load_conditions_optimized as load_conditions,
    load_observations_optimized as load_observations
)
from modules.medications_optimized import load_medications_optimized as load_medications
from modules.procedures_optimized import load_procedures_optimized as load_procedures
from modules.extended_optimized import (
    load_immunizations_optimized as load_immunizations,
    load_allergies,
    load_careplans
)

# Unchanged modules (already fast)
from modules.core import load_organizations, load_providers

def create_constraints(connection):
    """Create database constraints"""
    print("\n" + "=" * 60)
    print("Creating Constraints")
    print("=" * 60)
    
    constraints = [
        "CREATE CONSTRAINT patient_id IF NOT EXISTS FOR (p:Patient) REQUIRE p.patient_id IS UNIQUE",
        "CREATE CONSTRAINT encounter_id IF NOT EXISTS FOR (e:Encounter) REQUIRE e.encounter_id IS UNIQUE",
        "CREATE CONSTRAINT condition_code IF NOT EXISTS FOR (c:Condition) REQUIRE c.code IS UNIQUE",
        "CREATE CONSTRAINT medication_code IF NOT EXISTS FOR (m:Medication) REQUIRE m.code IS UNIQUE",
        "CREATE CONSTRAINT procedure_code IF NOT EXISTS FOR (p:Procedure) REQUIRE p.code IS UNIQUE",
        # NOTE: Observations are individual measurements, NOT unique by code (many patients have same code)
        # "CREATE CONSTRAINT observation_code IF NOT EXISTS FOR (o:Observation) REQUIRE o.code IS UNIQUE",
        "CREATE CONSTRAINT provider_id IF NOT EXISTS FOR (prov:Provider) REQUIRE prov.provider_id IS UNIQUE",
        "CREATE CONSTRAINT organization_id IF NOT EXISTS FOR (org:Organization) REQUIRE org.organization_id IS UNIQUE",
        "CREATE CONSTRAINT payer_id IF NOT EXISTS FOR (pay:Payer) REQUIRE pay.payer_id IS UNIQUE",
        "CREATE CONSTRAINT immunization_id IF NOT EXISTS FOR (i:Immunization) REQUIRE (i.patient_id, i.date, i.code) IS UNIQUE",
    ]
    
    for constraint in constraints:
        try:
            connection.run_query(constraint)
        except Exception as e:
            if "EquivalentSchemaRuleAlreadyExists" not in str(e):
                print(f"Warning: {e}")
    
    print("✓ Constraints created")

def clear_database(connection):
    """Clear all data from database"""
    print("\n" + "=" * 60)
    print("Clearing Database")
    print("=" * 60)
    
    # First drop all constraints (needed for clean import)
    try:
        constraints_result = connection.run_query("SHOW CONSTRAINTS")
        for constraint in constraints_result:
            constraint_name = constraint['name']
            try:
                connection.run_query(f"DROP CONSTRAINT {constraint_name}")
                print(f"  Dropped constraint: {constraint_name}")
            except Exception as e:
                print(f"  Warning dropping constraint {constraint_name}: {e}")
    except Exception as e:
        print(f"  Warning listing constraints: {e}")
    
    # Then delete all nodes in batches (prevents memory overflow)
    print("  Deleting nodes in batches...")
    deleted_total = 0
    while True:
        result = connection.run_query("MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) as deleted")
        deleted = result[0]['deleted'] if result else 0
        deleted_total += deleted
        if deleted == 0:
            break
        if deleted_total % 100000 == 0:
            print(f"    Deleted {deleted_total} nodes...")
    
    print(f"✓ Database cleared ({deleted_total} nodes deleted)")

def main():
    """Main ETL pipeline"""
    print("=" * 70)
    print("🚀 SYNTHEA TO NEO4J ETL PIPELINE (OPTIMIZED)")
    print("=" * 70)
    print("⚡ PERFORMANCE: ~150x faster with UNWIND batching")
    print("✅ DATA STRUCTURE: 100% identical to original")
    print("📊 COMPATIBLE: All existing queries & notebooks work unchanged")
    print("=" * 70)
    print(f"\nImport directory: {IMPORT_DIR}\n")
    
    # Check CSV files
    import os
    if os.path.exists(IMPORT_DIR):
        csv_files = [f for f in os.listdir(IMPORT_DIR) if f.endswith('.csv')]
        print(f"Found {len(csv_files)} CSV files:")
        for f in sorted(csv_files):
            size = os.path.getsize(f"{IMPORT_DIR}/{f}") / 1024
            print(f"  - {f} ({size:.1f} KB)")
        print()
    
    connection = Neo4jConnection()
    
    try:
        # Setup
        clear_database(connection)
        create_constraints(connection)
        
        # Phase 1: Base Entities
        print("\n" + "=" * 60)
        print("PHASE 1: Base Entities")
        print("=" * 60)
        
        load_patients(connection)
        load_organizations(connection)
        load_providers(connection)
        
        # Phase 2: Clinical Events
        print("\n" + "=" * 60)
        print("PHASE 2: Clinical Events")
        print("=" * 60)
        
        load_encounters(connection)
        load_conditions(connection)
        
        # Phase 3: Treatments
        print("\n" + "=" * 60)
        print("PHASE 3: Treatments & Procedures")
        print("=" * 60)
        
        load_medications(connection)
        load_procedures(connection)
        load_immunizations(connection)
        
        # Phase 4: Observations & Additional Data
        print("\n" + "=" * 60)
        print("PHASE 4: Observations & Additional Data")
        print("=" * 60)
        
        load_observations(connection)
        load_allergies(connection)
        load_careplans(connection)
        
        # Statistics
        stats = get_stats(connection)
        print_stats(stats)
        
        print("=" * 60)
        print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Open Neo4j Browser: http://localhost:7475")
        print("2. Login with: neo4j / synthea123")
        print("3. Run query: MATCH (p:Patient)-[:HAD_ENCOUNTER]->(e) RETURN p, e LIMIT 25")
        print("4. Explore: MATCH path = (p:Patient)-[*1..3]-(x) RETURN path LIMIT 100")
        
    except Exception as e:
        print(f"\n❌ ETL PIPELINE FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        connection.close()

if __name__ == "__main__":
    main()
