"""
DrugBank ETL - Main loader for DrugBank data integration
Combines CSV loader (fast drug loading) with XML parser (interactions)
"""

from neo4j import GraphDatabase
from modules.drugbank_csv_loader import DrugBankCSVLoader
from modules.drugbank_interaction_parser import DrugBankInteractionParser
from modules.medication_mapper import MedicationMapper
import os
import sys

# Pfade (im Docker Container)
DRUGBANK_CSV = "/data/drugbank/drugbank vocabulary.csv"
DRUGBANK_XML = "/data/drugbank/full database.xml"


def drugbank_available() -> tuple[bool, bool]:
    """
    Check if DrugBank files are available
    
    Returns:
        Tuple of (csv_exists, xml_exists)
    """
    csv_exists = os.path.exists(DRUGBANK_CSV)
    xml_exists = os.path.exists(DRUGBANK_XML)
    
    return csv_exists, xml_exists


def create_drugbank_constraints(tx):
    """
    Create constraints and indexes for DrugBank data
    CRITICAL for performance!
    """
    # Unique constraint on drugbank_id (creates index automatically)
    tx.run("""
        CREATE CONSTRAINT drugbank_id IF NOT EXISTS
        FOR (d:DrugBankDrug) REQUIRE d.drugbank_id IS UNIQUE
    """)
    
    print("   ✅ Created constraint on DrugBankDrug.drugbank_id")


def load_drugbank_drugs(tx, drug_data: dict):
    """
    Load DrugBankDrug nodes into Neo4j
    
    Args:
        tx: Neo4j transaction
        drug_data: Dict with drug properties
    """
    query = """
    MERGE (d:DrugBankDrug {drugbank_id: $drugbank_id})
    SET d.common_name = $common_name,
        d.cas_number = $cas_number,
        d.unii = $unii,
        d.synonyms = $synonyms,
        d.inchi_key = $inchi_key
    """
    tx.run(query, **drug_data)


def load_interactions(tx, source_id: str, target_id: str, description: str):
    """
    Load INTERACTS_WITH relationships with severity detection
    
    Args:
        tx: Neo4j transaction
        source_id: Source DrugBank ID
        target_id: Target DrugBank ID
        description: Interaction description
    """
    # Severity Detection from description text
    severity = "LOW"
    desc_lower = description.lower()
    
    # HIGH severity keywords
    high_keywords = ['contraindicated', 'avoid', 'life-threatening', 'severe', 
                     'dangerous', 'fatal', 'death', 'emergency', 'should not',
                     'do not', 'never', 'serious', 'critical']
    if any(word in desc_lower for word in high_keywords):
        severity = "HIGH"
    
    # MODERATE severity keywords
    elif any(word in desc_lower for word in 
             ['increase', 'decrease', 'risk', 'toxicity', 'may cause',
              'monitor', 'caution', 'adverse', 'affect', 'may', 'potential',
              'recommended', 'consider', 'adjustment', 'dose']):
        severity = "MODERATE"
    
    query = """
    MATCH (d1:DrugBankDrug {drugbank_id: $source_id})
    MATCH (d2:DrugBankDrug {drugbank_id: $target_id})
    MERGE (d1)-[i:INTERACTS_WITH]-(d2)
    SET i.description = $description,
        i.severity = $severity
    """
    tx.run(query, source_id=source_id, target_id=target_id, 
           description=description, severity=severity)


def load_interactions_batch(tx, interactions: list):
    """
    Load multiple interactions in one transaction (MUCH FASTER!)
    
    Args:
        tx: Neo4j transaction
        interactions: List of (source_id, target_id, description, severity) tuples
    """
    query = """
    UNWIND $batch as item
    MATCH (d1:DrugBankDrug {drugbank_id: item.source_id})
    MATCH (d2:DrugBankDrug {drugbank_id: item.target_id})
    MERGE (d1)-[i:INTERACTS_WITH]-(d2)
    SET i.description = item.description,
        i.severity = item.severity
    """
    
    # Convert tuples to dicts
    batch_data = [
        {
            'source_id': source_id,
            'target_id': target_id,
            'description': description,
            'severity': severity
        }
        for source_id, target_id, description, severity in interactions
    ]
    
    tx.run(query, batch=batch_data)


def map_synthea_to_drugbank(tx, csv_loader: DrugBankCSVLoader) -> tuple[int, list]:
    """
    Map Synthea Medications to DrugBankDrugs using CSV loader
    
    Args:
        tx: Neo4j transaction
        csv_loader: Loaded CSV loader instance
    
    Returns:
        Tuple of (mapped_count, unmapped_list)
    """
    print("🎯 Mapping Synthea Medications to DrugBank...")
    
    # 1. Get all Synthea Medications from Neo4j
    result = tx.run("""
        MATCH (m:Medication) 
        RETURN m.description as description, 
               m.code as code
        ORDER BY m.description
    """)
    medications = [{"description": r["description"], "code": r["code"]} for r in result]
    
    print(f"   Found {len(medications)} Synthea medications")
    
    # 2. Prepare mapping query
    mapping_query = """
    MATCH (m:Medication {code: $med_code})
    MATCH (d:DrugBankDrug {drugbank_id: $drugbank_id})
    MERGE (m)-[r:MAPPED_TO]->(d)
    SET r.confidence = $confidence,
        r.method = $method,
        r.extracted_name = $extracted_name
    """
    
    mapped_count = 0
    unmapped = []
    
    # 3. Map each medication
    for med in medications:
        # Extract drug name from Synthea description
        extracted_name = csv_loader.extract_drug_name_from_synthea(med["description"])
        
        # Search in DrugBank CSV
        matches = csv_loader.search_by_name(extracted_name, threshold=0.85)
        
        if matches:
            # Take best match (highest confidence)
            best_drugbank_id, confidence = matches[0]
            
            # Create MAPPED_TO relationship
            tx.run(mapping_query,
                   med_code=med["code"],
                   drugbank_id=best_drugbank_id,
                   confidence=confidence,
                   method="csv_lookup",
                   extracted_name=extracted_name)
            
            mapped_count += 1
            
            # Debug output (show mapping)
            drug_info = csv_loader.get_drug_by_id(best_drugbank_id)
            synthea_short = med['description'][:40]
            drugbank_name = drug_info['Common name']
            print(f"   ✅ {synthea_short:<40} → {drugbank_name:<20} (conf: {confidence:.2f})")
        else:
            unmapped.append((med["description"], extracted_name))
    
    return mapped_count, unmapped


def main():
    """Main ETL pipeline"""
    
    # Check for TEST MODE environment variable
    test_mode = os.getenv("DRUGBANK_TEST_MODE", "false").lower() == "true"
    test_interactions = int(os.getenv("DRUGBANK_TEST_INTERACTIONS", "100"))
    
    if test_mode:
        print("="*80)
        print("🧪 DRUGBANK TEST MODE")
        print(f"   Loading only first {test_interactions} interactions for testing")
        print("="*80)
    
    # Check availability
    csv_exists, xml_exists = drugbank_available()
    
    if not csv_exists and not xml_exists:
        print("⚠️  DrugBank files not found - Skip DrugBank integration")
        print("💡 Synthea demo works completely without DrugBank")
        print("📖 See: docs/DRUGBANK_SETUP_INSTRUCTIONS.md")
        return
    
    if csv_exists and not xml_exists:
        print("⚠️  CSV found, but XML missing")
        print("💡 Drugs can be loaded, but no interactions")
    
    print("="*80)
    print("🎉 DrugBank Integration - CSV-Based (FAST!)")
    print("="*80)
    
    # Neo4j connection
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://neo4j-synthea:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_pass = os.getenv("NEO4J_PASS", "synthea123")
    
    print(f"\n🔌 Connecting to Neo4j: {neo4j_uri}")
    
    try:
        driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_pass))
        
        with driver.session() as session:
            # Phase 0: Create constraints (CRITICAL for performance!)
            print("\n" + "="*80)
            print("🔧 Phase 0: Creating Database Constraints")
            print("="*80)
            session.execute_write(create_drugbank_constraints)
            
            # Phase 1: Load Drugs from CSV (FAST!)
            if csv_exists:
                print("\n" + "="*80)
                print("📦 Phase 1: Load DrugBank Drugs from CSV")
                print("="*80)
                
                csv_loader = DrugBankCSVLoader(DRUGBANK_CSV)
                drugs_df = csv_loader.load_csv()
                
                # Show statistics
                stats = csv_loader.get_stats()
                print(f"\n📊 CSV Statistics:")
                for key, value in stats.items():
                    print(f"   {key}: {value:,}")
                
                print(f"\n💾 Loading drugs into Neo4j...")
                drug_count = 0
                total_drugs = len(drugs_df)
                
                for _, row in drugs_df.iterrows():
                    drug_data = {
                        'drugbank_id': row['DrugBank ID'],
                        'common_name': row['Common name'],
                        'cas_number': str(row.get('CAS', '')),
                        'unii': str(row.get('UNII', '')),
                        'synonyms': str(row.get('Synonyms', '')),
                        'inchi_key': str(row.get('Standard InChI Key', ''))
                    }
                    
                    session.execute_write(load_drugbank_drugs, drug_data)
                    drug_count += 1
                    
                    if drug_count % 1000 == 0:
                        progress = (drug_count / total_drugs) * 100
                        print(f"   📊 Progress: {drug_count:,}/{total_drugs:,} drugs ({progress:.1f}%)", flush=True)
                
                print(f"✅ {drug_count:,} drugs loaded from CSV")
                
                # Phase 2: Map Synthea Medications
                print("\n" + "="*80)
                print("🎯 Phase 2: Map Synthea Medications to DrugBank")
                print("="*80)
                
                mapped_count, unmapped = session.execute_write(
                    map_synthea_to_drugbank, csv_loader
                )
                
                print(f"\n✅ Mapped: {mapped_count}/{len(unmapped) + mapped_count} medications")
                
                if unmapped:
                    print(f"⚠️  Unmapped: {len(unmapped)} medications")
                    print("\nUnmapped medications (for review):")
                    for desc, extracted in unmapped[:10]:  # Show first 10
                        print(f"   - {desc[:50]:<50} (extracted: {extracted})")
                    if len(unmapped) > 10:
                        print(f"   ... and {len(unmapped) - 10} more")
            
            # Phase 3: Load Interactions from XML (SLOW but necessary)
            if xml_exists:
                print("\n" + "="*80)
                print("🔗 Phase 3: Load Drug-Drug Interactions from XML")
                print("="*80)
                print("⏳ This takes 10-15 minutes (2.8M interactions)")
                print("☕ Time for a coffee break!")
                
                interaction_parser = DrugBankInteractionParser(DRUGBANK_XML)
                interaction_count = 0
                estimated_total = 2_800_000  # Estimated total interactions
                batch_size = 5000  # Process 5000 interactions per transaction (MUCH FASTER!)
                batch = []
                
                if test_mode:
                    print(f"   🧪 TEST MODE: Loading only first {test_interactions} interactions")
                    interaction_generator = interaction_parser.parse_first_n_interactions(test_interactions)
                    batch_size = 10  # Smaller batches for testing
                else:
                    print(f"   🚀 Starting interaction import (estimated: {estimated_total:,})")
                    print(f"   💡 Using batch processing ({batch_size:,} interactions per transaction)")
                    interaction_generator = interaction_parser.parse_interactions()
                
                import time
                start_time = time.time()
                
                for source_id, target_id, description in interaction_generator:
                    # Add to batch with severity detection
                    desc_lower = description.lower()
                    if any(kw in desc_lower for kw in ['contraindicated', 'avoid', 'life-threatening', 'severe', 'dangerous', 'fatal', 'death']):
                        severity = "HIGH"
                    elif any(kw in desc_lower for kw in ['increase', 'decrease', 'risk', 'toxicity', 'monitor', 'caution', 'may cause']):
                        severity = "MODERATE"
                    else:
                        severity = "LOW"
                    
                    batch.append((source_id, target_id, description, severity))
                    interaction_count += 1
                    
                    # Process batch when full
                    if len(batch) >= batch_size:
                        session.execute_write(load_interactions_batch, batch)
                        batch = []
                    
                    # Progress reporting
                    if test_mode and interaction_count % 10 == 0:
                        print(f"   📊 Test Progress: {interaction_count}/{test_interactions} interactions", flush=True)
                    elif not test_mode and interaction_count % 50000 == 0:
                        elapsed = time.time() - start_time
                        rate = interaction_count / elapsed if elapsed > 0 else 0
                        remaining = (estimated_total - interaction_count) / rate if rate > 0 else 0
                        progress = (interaction_count / estimated_total) * 100
                        print(f"   📊 Progress: {interaction_count:,}/{estimated_total:,} ({progress:.1f}%) | Rate: {rate:,.0f}/sec | ETA: {remaining/60:.1f} min", flush=True)
                
                # Process remaining batch
                if batch:
                    session.execute_write(load_interactions_batch, batch)
                
                total_time = time.time() - start_time
                print(f"✅ {interaction_count:,} interactions loaded in {total_time/60:.1f} minutes")
            else:
                print("\n⚠️  XML not found - Skipping interactions")
                print("💡 Drugs are loaded, but no interaction checking possible")
        
            # Phase 4: Auto-Map Medications (NEW - Option B)
            if csv_exists:
                print("\n" + "="*80)
                print("🎯 Phase 4: Auto-Map Current Medications")
                print("="*80)
                print("💡 Mapping Synthea Medications to DrugBank for interaction queries")
                
                # Check for manual mappings file
                manual_mappings_file = "/data/drugbank/manual_mappings.csv"
                if not os.path.exists(manual_mappings_file):
                    manual_mappings_file = None
                
                # Create mapper
                mapper = MedicationMapper(session, csv_loader, manual_mappings_file)
                
                # Map all medications
                mapping_result = mapper.map_all_medications(
                    confidence_threshold=0.75,
                    delete_old=True,
                    verbose=True
                )
                
                # Print summary
                print(f"\n📊 Mapping Summary:")
                print(f"   Total: {mapping_result.total_medications}")
                
                if mapping_result.total_medications > 0:
                    print(f"   ✅ Mapped: {mapping_result.mapped} ({mapping_result.mapped/mapping_result.total_medications*100:.1f}%)")
                    print(f"      High confidence (≥0.95): {mapping_result.high_confidence}")
                    print(f"      Medium confidence (0.85-0.95): {mapping_result.medium_confidence}")
                    print(f"      Low confidence (0.75-0.85): {mapping_result.low_confidence}")
                    print(f"   ❌ Unmapped: {mapping_result.unmapped}")
                else:
                    print(f"   ⚠️  No medications found - Synthea data may not be loaded yet")
                    print(f"   💡 This is normal if running DrugBank load before Synthea ETL")
                
                # Export unmapped for review
                if mapping_result.unmapped_list:
                    unmapped_file = "/app/unmapped_medications.csv"
                    mapper.export_unmapped_for_review(unmapped_file, mapping_result.unmapped_list)
                
                # Verify interactions work
                print("\n🧪 Verifying interaction queries...")
                interaction_stats = mapper.verify_interactions()
                
                if interaction_stats['total'] > 0:
                    print(f"   ✅ Found {interaction_stats['total']:,} drug interactions in patient data!")
                    print(f"      🔴 HIGH: {interaction_stats['high']:,}")
                    print(f"      🟡 MODERATE: {interaction_stats['moderate']:,}")
                    print(f"      🟢 LOW: {interaction_stats['low']:,}")
                    
                    # Show examples
                    examples = mapper.get_interaction_examples(limit=3)
                    if examples:
                        print("\n   Example interactions:")
                        for ex in examples:
                            icon = "🔴" if ex['severity'] == 'HIGH' else "🟡"
                            print(f"      {icon} {ex['patient']}: {ex['drug1']} ⚠️ {ex['drug2']}")
                else:
                    print("   ℹ️  No interactions found in current patient data")
                    print("   💡 This is normal with small datasets")
        
        driver.close()
        
        # Final summary
        print("\n" + "="*80)
        print("🎉 DrugBank Integration Complete!")
        print("="*80)
        if csv_exists:
            print(f"✅ Drugs loaded: {drug_count:,}")
            if 'mapping_result' in locals():
                print(f"✅ Medications mapped: {mapping_result.mapped}/{mapping_result.total_medications}")
        if xml_exists:
            print(f"✅ Interactions loaded: {interaction_count:,}")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ Error during ETL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
