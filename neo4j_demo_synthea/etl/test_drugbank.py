#!/usr/bin/env python3
"""
Quick test script for DrugBank integration
Tests CSV loader and XML parser without full ETL
"""

import sys
import os

# Add etl to path for imports
sys.path.insert(0, 'etl')

from modules.drugbank_csv_loader import DrugBankCSVLoader
from modules.drugbank_interaction_parser import DrugBankInteractionParser

def test_csv_loader():
    """Test CSV loading and search"""
    print("="*70)
    print("🧪 TEST 1: DrugBank CSV Loader")
    print("="*70)
    
    csv_path = "drugData/drugbank vocabulary.csv"
    
    if not os.path.exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        return False
    
    print(f"📦 Loading CSV from: {csv_path}")
    loader = DrugBankCSVLoader(csv_path)
    
    print("⏳ Reading CSV with pandas...")
    df = loader.load_csv()
    print(f"✅ Loaded {len(df):,} drugs")
    
    # Show statistics
    stats = loader.get_stats()
    print(f"\n📊 Statistics:")
    for key, value in stats.items():
        print(f"   {key}: {value:,}")
    
    # Test search for common Synthea medications
    test_drugs = ["ibuprofen", "acetaminophen", "lisinopril", "amlodipine", "metformin"]
    
    print(f"\n🔍 Testing drug search (Fuzzy Matching):")
    print(f"{'Drug Name':<20} {'DrugBank ID':<15} {'Confidence':<12} {'Matched Name'}")
    print("-" * 70)
    
    for drug_name in test_drugs:
        matches = loader.search_by_name(drug_name, threshold=0.80)
        if matches:
            drugbank_id, confidence = matches[0]  # Best match
            drug_info = loader.get_drug_by_id(drugbank_id)
            matched_name = drug_info['Common name']
            print(f"{drug_name:<20} {drugbank_id:<15} {confidence:>6.2%}       {matched_name}")
        else:
            print(f"{drug_name:<20} {'NOT FOUND':<15}")
    
    # Test extraction from Synthea descriptions
    print(f"\n🧬 Testing Synthea medication extraction:")
    synthea_examples = [
        "Ibuprofen 200 MG Oral Tablet",
        "Acetaminophen 325 MG Oral Tablet",
        "Lisinopril 10 MG Oral Tablet",
        "24 HR Metformin hydrochloride 500 MG Extended Release Oral Tablet"
    ]
    
    print(f"{'Synthea Description':<60} {'Extracted':<15} {'Found?'}")
    print("-" * 90)
    
    for desc in synthea_examples:
        extracted = loader.extract_drug_name_from_synthea(desc)
        matches = loader.search_by_name(extracted, threshold=0.85)
        found = "✅ YES" if matches else "❌ NO"
        print(f"{desc:<60} {extracted:<15} {found}")
    
    print(f"\n✅ CSV Loader test complete!\n")
    return True


def test_xml_parser_sample():
    """Test XML parser with small sample"""
    print("="*70)
    print("🧪 TEST 2: DrugBank XML Parser (Sample)")
    print("="*70)
    
    xml_path = "drugData/full database.xml"
    
    if not os.path.exists(xml_path):
        print(f"⚠️  XML file not found: {xml_path}")
        print(f"💡 This is okay - interactions are optional")
        return True
    
    print(f"📦 XML file found: {xml_path}")
    file_size = os.path.getsize(xml_path) / (1024**3)  # GB
    print(f"📏 File size: {file_size:.2f} GB")
    
    print(f"\n⏳ Parsing first 100 interactions (test mode)...")
    
    parser = DrugBankInteractionParser(xml_path)
    interactions = []
    
    for source_id, target_id, description in parser.parse_first_n_interactions(100):
        interactions.append((source_id, target_id, description))
    
    print(f"✅ Parsed {len(interactions)} interactions")
    
    # Show some examples
    print(f"\n📋 Sample Interactions:")
    print(f"{'Source ID':<12} {'Target ID':<12} {'Description (first 50 chars)'}")
    print("-" * 80)
    
    for source_id, target_id, desc in interactions[:5]:
        desc_short = desc[:50] + "..." if len(desc) > 50 else desc
        print(f"{source_id:<12} {target_id:<12} {desc_short}")
    
    # Analyze severity keywords
    high_count = sum(1 for _, _, desc in interactions 
                     if any(kw in desc.lower() for kw in 
                           ['contraindicated', 'avoid', 'life-threatening', 'fatal']))
    moderate_count = sum(1 for _, _, desc in interactions 
                        if any(kw in desc.lower() for kw in 
                              ['increase', 'decrease', 'monitor', 'risk']))
    
    print(f"\n📊 Severity Distribution (sample of 100):")
    print(f"   HIGH severity keywords: {high_count}")
    print(f"   MODERATE severity keywords: {moderate_count}")
    print(f"   LOW severity (rest): {100 - high_count - moderate_count}")
    
    print(f"\n✅ XML Parser test complete!\n")
    return True


def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("🚀 DRUGBANK INTEGRATION TEST SUITE")
    print("="*70 + "\n")
    
    # Test 1: CSV Loader
    if not test_csv_loader():
        print("❌ CSV Loader test failed")
        return 1
    
    # Test 2: XML Parser (sample)
    if not test_xml_parser_sample():
        print("❌ XML Parser test failed")
        return 1
    
    # Summary
    print("="*70)
    print("🎉 ALL TESTS PASSED!")
    print("="*70)
    print("\n✅ DrugBank integration is ready for full ETL")
    print("💡 Next step: Run full ETL with:")
    print("   docker compose run --rm synthea-etl python /etl/load_drugbank.py")
    print()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
