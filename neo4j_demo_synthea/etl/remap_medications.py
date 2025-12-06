"""
Re-map Medications to DrugBank - Quick Fix for WOW Demo

This script fixes orphaned MAPPED_TO relationships by:
1. Deleting old mappings to unused Medication nodes
2. Mapping current Medications (connected to Patients) to DrugBank
3. Creating new MAPPED_TO relationships

Does NOT modify:
- Synthea data (Medications, Patients, PRESCRIBED relationships)
- DrugBank data (DrugBankDrug nodes, INTERACTS_WITH relationships)

Only adds: MAPPED_TO relationships for demo purposes
"""

from neo4j import GraphDatabase
from modules.drugbank_csv_loader import DrugBankCSVLoader
import os
import sys


def remap_medications():
    """
    Re-map current Medications to DrugBank drugs
    """
    print("="*80)
    print("🎯 MEDICATION RE-MAPPING - Quick Fix")
    print("="*80)
    
    # Check if DrugBank CSV exists
    csv_path = "/data/drugbank/drugbank vocabulary.csv"
    if not os.path.exists(csv_path):
        print("⚠️  DrugBank CSV not found - Cannot perform mapping")
        print(f"   Expected: {csv_path}")
        print("💡 Demo will work without DrugBank integration")
        return
    
    # Neo4j connection
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://neo4j-synthea:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_pass = os.getenv("NEO4J_PASS", "synthea123")
    
    print(f"\n🔌 Connecting to Neo4j: {neo4j_uri}")
    
    try:
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_pass))
        
        with driver.session() as session:
            
            # Step 1: Check current state
            print("\n" + "="*80)
            print("📊 Step 1: Analyze Current State")
            print("="*80)
            
            result = session.run("""
                MATCH (m:Medication)-[old:MAPPED_TO]->()
                RETURN count(old) as old_mappings
            """)
            old_count = result.single()["old_mappings"]
            print(f"   Old MAPPED_TO relationships: {old_count}")
            
            result = session.run("""
                MATCH (p:Patient)-[:TAKES_MEDICATION]->(m:Medication)
                RETURN count(DISTINCT m) as current_meds
            """)
            current_meds = result.single()["current_meds"]
            print(f"   Current Medications (with Patients): {current_meds}")
            
            result = session.run("""
                MATCH (db:DrugBankDrug)
                RETURN count(db) as drugbank_drugs
            """)
            drugbank_count = result.single()["drugbank_drugs"]
            print(f"   DrugBank drugs available: {drugbank_count}")
            
            if drugbank_count == 0:
                print("\n❌ No DrugBank drugs found!")
                print("💡 Please run load_drugbank.py first")
                return
            
            # Step 2: Delete old mappings
            print("\n" + "="*80)
            print("🗑️  Step 2: Delete Old Mappings")
            print("="*80)
            
            if old_count > 0:
                result = session.run("""
                    MATCH ()-[old:MAPPED_TO]->()
                    DELETE old
                    RETURN count(*) as deleted
                """)
                deleted = result.single()["deleted"]
                print(f"   ✅ Deleted {deleted} old MAPPED_TO relationships")
            else:
                print("   ℹ️  No old mappings to delete")
            
            # Step 3: Load DrugBank CSV Loader
            print("\n" + "="*80)
            print("📦 Step 3: Load DrugBank CSV")
            print("="*80)
            
            csv_loader = DrugBankCSVLoader(csv_path)
            csv_loader.load_csv()
            
            stats = csv_loader.get_stats()
            print(f"   Loaded {stats['total_drugs']:,} drugs from CSV")
            
            # Step 4: Get current Medications
            print("\n" + "="*80)
            print("🎯 Step 4: Map Current Medications to DrugBank")
            print("="*80)
            
            result = session.run("""
                MATCH (p:Patient)-[:TAKES_MEDICATION]->(m:Medication)
                WITH DISTINCT m
                RETURN m.code as code, 
                       m.description as description
                ORDER BY m.description
            """)
            
            medications = [{"code": r["code"], "description": r["description"]} for r in result]
            print(f"   Found {len(medications)} unique medications to map\n")
            
            # Step 5: Map each medication
            mapped_count = 0
            high_confidence = 0
            medium_confidence = 0
            low_confidence = 0
            unmapped = []
            
            for med in medications:
                # Extract drug name
                extracted_name = csv_loader.extract_drug_name_from_synthea(med["description"])
                
                # Search in DrugBank
                matches = csv_loader.search_by_name(extracted_name, threshold=0.75)
                
                if matches:
                    # Take best match
                    best_drugbank_id, confidence = matches[0]
                    
                    # Create MAPPED_TO relationship
                    session.run("""
                        MATCH (m:Medication {code: $med_code})
                        MATCH (d:DrugBankDrug {drugbank_id: $drugbank_id})
                        MERGE (m)-[r:MAPPED_TO]->(d)
                        SET r.confidence = $confidence,
                            r.method = 'csv_lookup',
                            r.extracted_name = $extracted_name,
                            r.created = datetime()
                    """, med_code=med["code"], 
                         drugbank_id=best_drugbank_id,
                         confidence=confidence,
                         extracted_name=extracted_name)
                    
                    mapped_count += 1
                    
                    # Count by confidence level
                    if confidence >= 0.95:
                        high_confidence += 1
                        indicator = "✅"
                    elif confidence >= 0.85:
                        medium_confidence += 1
                        indicator = "🟡"
                    else:
                        low_confidence += 1
                        indicator = "🟠"
                    
                    # Get drug info for display
                    drug_info = csv_loader.get_drug_by_id(best_drugbank_id)
                    synthea_short = med['description'][:45]
                    drugbank_name = drug_info['Common name'][:20]
                    
                    print(f"   {indicator} {synthea_short:<45} → {drugbank_name:<20} (conf: {confidence:.2f})")
                else:
                    unmapped.append((med["description"], extracted_name))
            
            # Step 6: Summary
            print("\n" + "="*80)
            print("📊 Mapping Summary")
            print("="*80)
            print(f"   Total medications: {len(medications)}")
            print(f"   ✅ Mapped: {mapped_count} ({mapped_count/len(medications)*100:.1f}%)")
            print(f"      High confidence (≥0.95): {high_confidence}")
            print(f"      Medium confidence (0.85-0.95): {medium_confidence}")
            print(f"      Low confidence (0.75-0.85): {low_confidence}")
            print(f"   ❌ Unmapped: {len(unmapped)}")
            
            if unmapped:
                print("\n   Unmapped medications (for manual review):")
                for desc, extracted in unmapped[:15]:  # Show first 15
                    print(f"      - {desc[:60]:<60} (extracted: {extracted})")
                if len(unmapped) > 15:
                    print(f"      ... and {len(unmapped) - 15} more")
            
            # Step 7: Verify mapping works for interaction queries
            print("\n" + "="*80)
            print("🧪 Step 7: Verify Interaction Queries Work")
            print("="*80)
            
            result = session.run("""
                MATCH (p:Patient)-[:TAKES_MEDICATION]->(m1:Medication)-[:MAPPED_TO]->(db1:DrugBankDrug),
                      (p)-[:TAKES_MEDICATION]->(m2:Medication)-[:MAPPED_TO]->(db2:DrugBankDrug),
                      (db1)-[i:INTERACTS_WITH]-(db2)
                WHERE id(m1) < id(m2)
                RETURN count(*) as total_interactions,
                       sum(CASE WHEN i.severity = 'HIGH' THEN 1 ELSE 0 END) as high_severity,
                       sum(CASE WHEN i.severity = 'MODERATE' THEN 1 ELSE 0 END) as moderate_severity,
                       sum(CASE WHEN i.severity = 'LOW' THEN 1 ELSE 0 END) as low_severity
            """)
            
            verification = result.single()
            if verification:
                total = verification["total_interactions"]
                high = verification["high_severity"]
                moderate = verification["moderate_severity"]
                low = verification["low_severity"]
                
                print(f"   Total patient drug interactions found: {total:,}")
                print(f"      🔴 HIGH severity: {high:,}")
                print(f"      🟡 MODERATE severity: {moderate:,}")
                print(f"      🟢 LOW severity: {low:,}")
                
                if total > 0:
                    print("\n   ✅ Mapping successful! Drug interactions can be queried.")
                    
                    # Show example
                    result = session.run("""
                        MATCH (p:Patient)-[:TAKES_MEDICATION]->(m1:Medication)-[:MAPPED_TO]->(db1:DrugBankDrug),
                              (p)-[:TAKES_MEDICATION]->(m2:Medication)-[:MAPPED_TO]->(db2:DrugBankDrug),
                              (db1)-[i:INTERACTS_WITH]-(db2)
                        WHERE i.severity IN ['HIGH', 'MODERATE']
                        RETURN p.firstName + ' ' + p.lastName as patient,
                               db1.common_name as drug1,
                               db2.common_name as drug2,
                               i.severity as severity
                        LIMIT 3
                    """)
                    
                    examples = list(result)
                    if examples:
                        print("\n   Example interactions found:")
                        for ex in examples:
                            sev_icon = "🔴" if ex["severity"] == "HIGH" else "🟡"
                            print(f"      {sev_icon} {ex['patient']}: {ex['drug1']} ⚠️ {ex['drug2']} ({ex['severity']})")
                else:
                    print("\n   ⚠️  No interactions found")
                    print("   This could mean:")
                    print("      - Patients don't have medications with known interactions")
                    print("      - More patients needed for statistical significance")
                    print("      - Mapping threshold could be lowered (currently 0.75)")
            
        driver.close()
        
        print("\n" + "="*80)
        print("✅ Re-mapping Complete!")
        print("="*80)
        print("🎯 Next steps:")
        print("   1. Open Neo4j Browser: http://localhost:7475")
        print("   2. Run: MATCH (p:Patient)-[:TAKES_MEDICATION]->(m)-[:MAPPED_TO]->(db) RETURN p,m,db LIMIT 25")
        print("   3. Test interactions: See docs/WOW_DEMO_PLAN.md")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ Error during re-mapping: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    remap_medications()
