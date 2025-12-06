"""
Advanced Medication Mapper - Uses extended matching strategies

This script uses the advanced search_by_name_advanced() method
with multiple matching strategies:
- Exact matching
- Synonym matching
- Stemming
- Levenshtein distance
- Fuzzy matching

Run with different confidence thresholds to optimize mapping rate.
"""

from neo4j import GraphDatabase
from modules.drugbank_csv_loader import DrugBankCSVLoader
from modules.medication_mapper import MedicationMapper
import os
import sys


def analyze_mapping_quality(csv_loader: DrugBankCSVLoader, medications: list):
    """
    Analyze mapping quality with different strategies
    """
    print("\n" + "="*80)
    print("🔬 Mapping Strategy Analysis")
    print("="*80)
    
    strategies_count = {
        'exact_match': 0,
        'synonym_exact': 0,
        'stemming': 0,
        'levenshtein': 0,
        'fuzzy_match': 0,
        'fuzzy_synonym': 0,
        'unmapped': 0
    }
    
    print("\nAnalyzing each medication with advanced search...\n")
    
    for med in medications[:20]:  # Analyze first 20 for display
        extracted = csv_loader.extract_drug_name_from_synthea(med['description'])
        matches = csv_loader.search_by_name_advanced(extracted, threshold=0.75)
        
        if matches:
            best_id, confidence, method = matches[0]
            strategies_count[method] += 1
            
            drug_info = csv_loader.get_drug_by_id(best_id)
            
            # Icon by method
            icons = {
                'exact_match': '✅',
                'synonym_exact': '✅',
                'stemming': '🟢',
                'levenshtein': '🟡',
                'fuzzy_match': '🟠',
                'fuzzy_synonym': '🟠'
            }
            icon = icons.get(method, '❓')
            
            print(f"   {icon} {med['description'][:40]:<40} → {drug_info['Common name'][:20]:<20} ({confidence:.2f}, {method})")
        else:
            strategies_count['unmapped'] += 1
            print(f"   ❌ {med['description'][:40]:<40} → UNMAPPED (extracted: {extracted})")
    
    if len(medications) > 20:
        print(f"\n   ... analyzing remaining {len(medications) - 20} medications ...")
        
        for med in medications[20:]:
            extracted = csv_loader.extract_drug_name_from_synthea(med['description'])
            matches = csv_loader.search_by_name_advanced(extracted, threshold=0.75)
            
            if matches:
                _, _, method = matches[0]
                strategies_count[method] += 1
            else:
                strategies_count['unmapped'] += 1
    
    print("\n" + "="*80)
    print("📊 Strategy Distribution")
    print("="*80)
    for strategy, count in sorted(strategies_count.items(), key=lambda x: x[1], reverse=True):
        if count > 0:
            percentage = count / len(medications) * 100
            print(f"   {strategy:<20}: {count:>3} ({percentage:>5.1f}%)")
    
    total_mapped = len(medications) - strategies_count['unmapped']
    print(f"\n   Total Mapped: {total_mapped}/{len(medications)} ({total_mapped/len(medications)*100:.1f}%)")


def advanced_remap_medications(confidence_threshold: float = 0.75, 
                               use_advanced: bool = True,
                               analyze_only: bool = False):
    """
    Re-map medications using advanced strategies
    
    Args:
        confidence_threshold: Minimum confidence (0.75 recommended)
        use_advanced: Use search_by_name_advanced() instead of search_by_name()
        analyze_only: Only analyze, don't create mappings
    """
    print("="*80)
    print("🔬 ADVANCED MEDICATION MAPPING")
    print("="*80)
    print(f"   Confidence threshold: {confidence_threshold}")
    print(f"   Advanced strategies: {use_advanced}")
    print(f"   Analyze only: {analyze_only}")
    
    # Check DrugBank CSV
    csv_path = "/data/drugbank/drugbank vocabulary.csv"
    if not os.path.exists(csv_path):
        print("\n⚠️  DrugBank CSV not found")
        return
    
    # Neo4j connection
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://neo4j-synthea:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_pass = os.getenv("NEO4J_PASS", "synthea123")
    
    print(f"\n🔌 Connecting to Neo4j: {neo4j_uri}")
    
    try:
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_pass))
        
        with driver.session() as session:
            # Load CSV
            print("\n📦 Loading DrugBank CSV...")
            csv_loader = DrugBankCSVLoader(csv_path)
            csv_loader.load_csv()
            
            # Get medications
            result = session.run("""
                MATCH (p:Patient)-[:TAKES_MEDICATION]->(m:Medication)
                WITH DISTINCT m
                RETURN m.code as code,
                       m.description as description
                ORDER BY m.description
            """)
            medications = [{"code": r["code"], "description": r["description"]} for r in result]
            
            print(f"   Found {len(medications)} medications to analyze")
            
            # Analyze mapping strategies
            if use_advanced:
                analyze_mapping_quality(csv_loader, medications)
            
            if analyze_only:
                print("\n✅ Analysis complete (no mappings created)")
                return
            
            # Delete old mappings
            print("\n🗑️  Deleting old mappings...")
            result = session.run("""
                MATCH ()-[old:MAPPED_TO]->()
                DELETE old
                RETURN count(*) as deleted
            """)
            deleted = result.single()["deleted"]
            print(f"   Deleted {deleted} old mappings")
            
            # Map with advanced or standard strategy
            print("\n🎯 Creating new mappings...")
            mapped_count = 0
            unmapped = []
            
            # Strategy counters
            strategy_stats = {}
            
            for med in medications:
                extracted = csv_loader.extract_drug_name_from_synthea(med['description'])
                
                # Use advanced or standard search
                if use_advanced:
                    matches = csv_loader.search_by_name_advanced(extracted, threshold=confidence_threshold)
                    if matches:
                        drugbank_id, confidence, method = matches[0]
                        strategy_stats[method] = strategy_stats.get(method, 0) + 1
                    else:
                        matches = []
                else:
                    matches = csv_loader.search_by_name(extracted, threshold=confidence_threshold)
                    if matches:
                        drugbank_id, confidence = matches[0]
                        method = 'standard_fuzzy'
                        strategy_stats[method] = strategy_stats.get(method, 0) + 1
                
                if matches:
                    if use_advanced:
                        drugbank_id, confidence, method = matches[0]
                    else:
                        drugbank_id, confidence = matches[0]
                        method = 'standard_fuzzy'
                    
                    # Create mapping
                    session.run("""
                        MATCH (m:Medication {code: $med_code})
                        MATCH (d:DrugBankDrug {drugbank_id: $drugbank_id})
                        MERGE (m)-[r:MAPPED_TO]->(d)
                        SET r.confidence = $confidence,
                            r.method = $method,
                            r.extracted_name = $extracted_name,
                            r.created = datetime()
                    """, med_code=med["code"],
                         drugbank_id=drugbank_id,
                         confidence=confidence,
                         method=method,
                         extracted_name=extracted)
                    
                    mapped_count += 1
                else:
                    unmapped.append((med['description'], extracted))
            
            # Summary
            print("\n" + "="*80)
            print("📊 Mapping Results")
            print("="*80)
            print(f"   Total medications: {len(medications)}")
            print(f"   ✅ Mapped: {mapped_count} ({mapped_count/len(medications)*100:.1f}%)")
            print(f"   ❌ Unmapped: {len(unmapped)}")
            
            if strategy_stats:
                print("\n   Strategy breakdown:")
                for strategy, count in sorted(strategy_stats.items(), key=lambda x: x[1], reverse=True):
                    print(f"      {strategy:<20}: {count}")
            
            if unmapped:
                print(f"\n   Unmapped medications ({len(unmapped)}):")
                for desc, extracted in unmapped[:10]:
                    print(f"      - {desc[:50]} (extracted: {extracted})")
                if len(unmapped) > 10:
                    print(f"      ... and {len(unmapped) - 10} more")
            
            # Verify
            print("\n🧪 Verifying interaction queries...")
            result = session.run("""
                MATCH (p:Patient)-[:TAKES_MEDICATION]->(m1:Medication)-[:MAPPED_TO]->(db1:DrugBankDrug),
                      (p)-[:TAKES_MEDICATION]->(m2:Medication)-[:MAPPED_TO]->(db2:DrugBankDrug),
                      (db1)-[i:INTERACTS_WITH]-(db2)
                WHERE id(m1) < id(m2)
                RETURN count(*) as total,
                       sum(CASE WHEN i.severity = 'HIGH' THEN 1 ELSE 0 END) as high
            """)
            
            stats = result.single()
            if stats and stats['total'] > 0:
                print(f"   ✅ Found {stats['total']:,} interactions ({stats['high']:,} HIGH severity)")
            else:
                print("   ℹ️  No interactions found (normal for small datasets)")
        
        driver.close()
        print("\n✅ Advanced mapping complete!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Advanced Medication Mapping')
    parser.add_argument('--threshold', type=float, default=0.75,
                       help='Confidence threshold (0.0-1.0, default: 0.75)')
    parser.add_argument('--standard', action='store_true',
                       help='Use standard fuzzy matching instead of advanced')
    parser.add_argument('--analyze-only', action='store_true',
                       help='Only analyze strategies, don\'t create mappings')
    
    args = parser.parse_args()
    
    advanced_remap_medications(
        confidence_threshold=args.threshold,
        use_advanced=not args.standard,
        analyze_only=args.analyze_only
    )
