#!/usr/bin/env python3
"""
Verification script for DrugBank integration
Checks that data was correctly loaded into Neo4j
"""

import sys
sys.path.insert(0, '/etl')

from neo4j import GraphDatabase
import os

# Neo4j connection
uri = os.getenv("NEO4J_URI", "bolt://neo4j-synthea:7687")
user = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASS", "synthea123")

print("="*80)
print("🔍 DRUGBANK INTEGRATION VERIFICATION")
print("="*80)

try:
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    with driver.session() as session:
        # 1. Check DrugBankDrug nodes
        print("\n📦 Checking DrugBankDrug nodes...")
        result = session.run("MATCH (d:DrugBankDrug) RETURN count(d) as count")
        drug_count = result.single()["count"]
        print(f"   Found: {drug_count:,} DrugBankDrug nodes")
        
        if drug_count == 0:
            print("   ❌ ERROR: No DrugBankDrug nodes found!")
            sys.exit(1)
        else:
            print(f"   ✅ OK: {drug_count:,} drugs loaded")
        
        # 2. Sample drug data
        print("\n📋 Sample DrugBankDrug nodes:")
        result = session.run("""
            MATCH (d:DrugBankDrug) 
            RETURN d.drugbank_id as id, d.common_name as name
            LIMIT 5
        """)
        for record in result:
            print(f"   - {record['id']}: {record['name']}")
        
        # 3. Check MAPPED_TO relationships
        print("\n🔗 Checking MAPPED_TO relationships...")
        result = session.run("""
            MATCH ()-[r:MAPPED_TO]->()
            RETURN count(r) as count
        """)
        mapped_count = result.single()["count"]
        print(f"   Found: {mapped_count} MAPPED_TO relationships")
        
        if mapped_count == 0:
            print("   ⚠️  WARNING: No MAPPED_TO relationships found!")
            print("   💡 Check if Synthea medications exist in database")
        else:
            print(f"   ✅ OK: {mapped_count} medications mapped to DrugBank")
        
        # 4. Sample mappings
        if mapped_count > 0:
            print("\n📋 Sample medication mappings:")
            result = session.run("""
                MATCH (m:Medication)-[r:MAPPED_TO]->(d:DrugBankDrug)
                RETURN m.description as med, d.common_name as drug, r.confidence as conf
                LIMIT 5
            """)
            for record in result:
                med_short = record['med'][:40] if record['med'] else "N/A"
                print(f"   - {med_short:<40} → {record['drug']:<20} (conf: {record['conf']:.2f})")
        
        # 5. Check INTERACTS_WITH relationships
        print("\n🔗 Checking INTERACTS_WITH relationships...")
        result = session.run("""
            MATCH ()-[r:INTERACTS_WITH]->()
            RETURN count(r) as count
        """)
        interaction_count = result.single()["count"]
        print(f"   Found: {interaction_count:,} INTERACTS_WITH relationships")
        
        if interaction_count == 0:
            print("   ⚠️  WARNING: No INTERACTS_WITH relationships found!")
            print("   💡 Check if XML parsing completed successfully")
        else:
            print(f"   ✅ OK: {interaction_count:,} interactions loaded")
        
        # 6. Severity distribution
        if interaction_count > 0:
            print("\n📊 Severity distribution:")
            result = session.run("""
                MATCH ()-[r:INTERACTS_WITH]->()
                RETURN r.severity as severity, count(r) as count
                ORDER BY count DESC
            """)
            for record in result:
                severity = record['severity'] or 'UNKNOWN'
                count = record['count']
                percentage = (count / interaction_count) * 100
                print(f"   - {severity:<10}: {count:>8,} ({percentage:>5.1f}%)")
        
        # 7. Sample interactions
        if interaction_count > 0:
            print("\n📋 Sample HIGH severity interactions:")
            result = session.run("""
                MATCH (d1:DrugBankDrug)-[r:INTERACTS_WITH]->(d2:DrugBankDrug)
                WHERE r.severity = 'HIGH'
                RETURN d1.common_name as drug1, d2.common_name as drug2, 
                       substring(r.description, 0, 60) as desc
                LIMIT 3
            """)
            found = False
            for record in result:
                found = True
                print(f"   - {record['drug1']} ↔ {record['drug2']}")
                print(f"     {record['desc']}...")
            if not found:
                print("   ⚠️  No HIGH severity interactions in current dataset")
    
    driver.close()
    
    # Summary
    print("\n" + "="*80)
    print("📊 VERIFICATION SUMMARY")
    print("="*80)
    print(f"✅ DrugBankDrug nodes: {drug_count:,}")
    print(f"✅ MAPPED_TO relationships: {mapped_count}")
    print(f"✅ INTERACTS_WITH relationships: {interaction_count:,}")
    
    if drug_count > 0 and mapped_count > 0 and interaction_count > 0:
        print("\n🎉 ALL CHECKS PASSED!")
        print("💡 DrugBank integration is working correctly")
        sys.exit(0)
    else:
        print("\n⚠️  SOME CHECKS FAILED")
        print("💡 Review errors above")
        sys.exit(1)
        
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
