#!/usr/bin/env python3
"""
Minimal DrugBank test - runs in Docker container
"""

import sys
sys.path.insert(0, '/etl')

from modules.drugbank_csv_loader import DrugBankCSVLoader
from modules.drugbank_interaction_parser import DrugBankInteractionParser
from neo4j import GraphDatabase
import os

print("="*80)
print("🧪 DRUGBANK INTEGRATION TEST")
print("="*80)

# Test 1: CSV Loader
print("\n📦 TEST 1: CSV Loader")
print("-" * 80)

csv_path = "/data/drugbank/drugbank vocabulary.csv"
if not os.path.exists(csv_path):
    print(f"❌ CSV not found: {csv_path}")
    sys.exit(1)

loader = DrugBankCSVLoader(csv_path)
df = loader.load_csv()
print(f"✅ Loaded {len(df):,} drugs from CSV")

stats = loader.get_stats()
for key, value in stats.items():
    print(f"   {key}: {value:,}")

# Test search
print("\n🔍 Test Drug Search:")
test_drugs = ["ibuprofen", "acetaminophen", "lisinopril"]
for drug in test_drugs:
    matches = loader.search_by_name(drug)
    if matches:
        dbid, conf = matches[0]
        info = loader.get_drug_by_id(dbid)
        print(f"   {drug:<15} → {dbid} ({info['Common name']}, conf: {conf:.2%})")

# Test 2: XML Parser (sample)
print("\n📦 TEST 2: XML Parser (Sample)")
print("-" * 80)

xml_path = "/data/drugbank/full database.xml"
if not os.path.exists(xml_path):
    print("⚠️  XML not found - skipping")
else:
    parser = DrugBankInteractionParser(xml_path)
    interactions = list(parser.parse_first_n_interactions(10))
    print(f"✅ Parsed {len(interactions)} sample interactions")
    for src, tgt, desc in interactions[:3]:
        print(f"   {src} ↔ {tgt}: {desc[:50]}...")

# Test 3: Neo4j Connection
print("\n📦 TEST 3: Neo4j Connection")
print("-" * 80)

uri = os.getenv("NEO4J_URI", "bolt://neo4j-synthea:7687")
user = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASS", "synthea123")

try:
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        result = session.run("RETURN 1 as test")
        assert result.single()["test"] == 1
        print(f"✅ Connected to Neo4j at {uri}")
        
        # Check existing data
        result = session.run("MATCH (m:Medication) RETURN count(m) as count")
        med_count = result.single()["count"]
        print(f"✅ Found {med_count} Medication nodes in database")
    
    driver.close()
except Exception as e:
    print(f"❌ Neo4j connection failed: {e}")
    sys.exit(1)

print("\n" + "="*80)
print("🎉 ALL TESTS PASSED!")
print("="*80)
print("\n💡 DrugBank integration is ready!")
print("📝 Run full ETL: python /etl/load_drugbank.py")
