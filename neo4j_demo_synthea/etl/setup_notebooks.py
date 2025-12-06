#!/usr/bin/env python3
"""
Neo4j Cypher Script Executor
=============================
Automatically executes Cypher scripts needed for notebook functionality.

This script runs:
1. 01_synthea_setup.cypher - Database indices and constraints
2. 02_categorize_conditions.cypher - Label conditions (ChronicDisease, SocialDeterminant)
3. 03_comorbidity_analysis.cypher - Create comorbidity relationships and run GDS

Author: GitHub Copilot + Paul Schäfer
Date: November 17, 2025
"""

import sys
import time
from pathlib import Path
from neo4j import GraphDatabase


class Neo4jScriptExecutor:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        # Cypher scripts sind unter /neo4j gemountet (von docker-compose.yml: ./neo4j:/neo4j)
        self.cypher_dir = Path('/neo4j')
    
    def close(self):
        self.driver.close()
    
    def execute_cypher_file(self, filename, description):
        """Execute a Cypher script file statement by statement"""
        print(f"\n{'='*70}")
        print(f"🔧 {description}")
        print(f"   File: {filename}")
        print(f"{'='*70}")
        
        filepath = self.cypher_dir / filename
        
        if not filepath.exists():
            print(f"❌ File not found: {filepath}")
            return False
        
        # Read file and split into statements
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Remove comments and split by semicolon
        statements = []
        current_statement = []
        
        for line in content.split('\n'):
            # Skip full-line comments
            if line.strip().startswith('//'):
                continue
            
            # Remove inline comments
            if '//' in line:
                line = line.split('//')[0]
            
            # Add non-empty lines
            if line.strip():
                current_statement.append(line)
                
                # Check if statement ends with semicolon
                if line.strip().endswith(';'):
                    statement = '\n'.join(current_statement)
                    statements.append(statement)
                    current_statement = []
        
        # Add last statement if exists
        if current_statement:
            statements.append('\n'.join(current_statement))
        
        print(f"📝 Found {len(statements)} statements to execute\n")
        
        executed = 0
        skipped = 0
        errors = 0
        
        with self.driver.session() as session:
            for i, statement in enumerate(statements, 1):
                # Skip empty statements
                if not statement.strip():
                    continue
                
                # Skip pure comment blocks
                if all(line.strip().startswith('//') or not line.strip() 
                       for line in statement.split('\n')):
                    continue
                
                # Get first line for display (limit length)
                first_line = statement.strip().split('\n')[0][:60]
                
                try:
                    # Execute statement
                    result = session.run(statement)
                    
                    # Try to consume results
                    try:
                        summary = result.consume()
                        
                        # Show progress for important operations
                        if any(keyword in statement.upper() for keyword in 
                               ['CREATE INDEX', 'CREATE CONSTRAINT', 'SET', 'MERGE', 
                                'CALL GDS', 'MATCH', 'RETURN']):
                            counters = summary.counters
                            
                            info_parts = []
                            if counters.nodes_created > 0:
                                info_parts.append(f"{counters.nodes_created} nodes")
                            if counters.relationships_created > 0:
                                info_parts.append(f"{counters.relationships_created} rels")
                            if counters.properties_set > 0:
                                info_parts.append(f"{counters.properties_set} props")
                            if counters.labels_added > 0:
                                info_parts.append(f"{counters.labels_added} labels")
                            
                            info = f" ({', '.join(info_parts)})" if info_parts else ""
                            print(f"  ✓ [{i:3d}] {first_line}...{info}")
                        
                        executed += 1
                    except:
                        # Statement didn't return results (e.g., SET, MERGE without RETURN)
                        print(f"  ✓ [{i:3d}] {first_line}...")
                        executed += 1
                
                except Exception as e:
                    error_msg = str(e)
                    
                    # Skip certain expected errors/warnings
                    if any(skip_pattern in error_msg for skip_pattern in [
                        'already exists',
                        'Unable to create',
                        'An equivalent'
                    ]):
                        print(f"  ⊙ [{i:3d}] {first_line}... (already exists)")
                        skipped += 1
                    else:
                        print(f"  ✗ [{i:3d}] {first_line}...")
                        print(f"       Error: {error_msg}")
                        errors += 1
                
                # Small delay to avoid overwhelming Neo4j
                time.sleep(0.01)
        
        print(f"\n{'─'*70}")
        print(f"✅ Executed: {executed} | ⊙ Skipped: {skipped} | ✗ Errors: {errors}")
        print(f"{'─'*70}")
        
        return errors == 0
    
    def verify_setup(self):
        """Verify that all necessary labels and relationships exist"""
        print(f"\n{'='*70}")
        print("🔍 VERIFYING SETUP")
        print(f"{'='*70}\n")
        
        with self.driver.session() as session:
            # Check for ChronicDisease label
            result = session.run("MATCH (c:ChronicDisease) RETURN count(c) as count")
            chronic_count = result.single()['count']
            
            # Check for SocialDeterminant label
            result = session.run("MATCH (s:SocialDeterminant) RETURN count(s) as count")
            social_count = result.single()['count']
            
            # Check for CO_OCCURS_WITH relationships
            result = session.run("MATCH ()-[r:CO_OCCURS_WITH]->() RETURN count(r) as count")
            comorbidity_count = result.single()['count']
            
            # Check for GDS graph
            try:
                result = session.run("CALL gds.graph.exists('comorbidity-network') YIELD exists")
                gds_exists = result.single()['exists']
            except:
                gds_exists = False
            
            # Check for PageRank property
            result = session.run("""
                MATCH (c:ChronicDisease) 
                WHERE c.pageRank IS NOT NULL 
                RETURN count(c) as count
            """)
            pagerank_count = result.single()['count']
            
            print(f"ChronicDisease nodes:          {chronic_count:,}")
            print(f"SocialDeterminant nodes:       {social_count:,}")
            print(f"CO_OCCURS_WITH relationships:  {comorbidity_count:,}")
            print(f"GDS graph exists:              {'Yes ✓' if gds_exists else 'No ✗'}")
            print(f"Nodes with PageRank:           {pagerank_count:,}")
            
            print(f"\n{'─'*70}")
            
            # Verify all required components
            all_good = (
                chronic_count > 0 and
                social_count > 0 and
                comorbidity_count > 0 and
                gds_exists and
                pagerank_count > 0
            )
            
            if all_good:
                print("✅ ALL CHECKS PASSED - Notebooks should work correctly!")
            else:
                print("⚠️  SOME CHECKS FAILED - Notebooks may not work correctly!")
                if chronic_count == 0:
                    print("   → Missing ChronicDisease labels")
                if social_count == 0:
                    print("   → Missing SocialDeterminant labels")
                if comorbidity_count == 0:
                    print("   → Missing CO_OCCURS_WITH relationships")
                if not gds_exists:
                    print("   → Missing GDS graph projection")
                if pagerank_count == 0:
                    print("   → Missing PageRank calculations")
            
            print(f"{'─'*70}\n")
            
            return all_good


def main():
    print("""
╔═══════════════════════════════════════════════════════════════════╗
║                                                                   ║
║     Neo4j Cypher Script Executor                                 ║
║     Prepares database for Jupyter notebooks                      ║
║                                                                   ║
╚═══════════════════════════════════════════════════════════════════╝
""")
    
    # Connection settings
    NEO4J_URI = "bolt://neo4j-synthea:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "synthea123"
    
    executor = Neo4jScriptExecutor(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        # Test connection
        print("🔌 Testing connection to Neo4j...")
        with executor.driver.session() as session:
            result = session.run("RETURN 1 as test")
            result.single()
        print("✅ Connection successful!\n")
        
        # Execute scripts in order
        scripts = [
            ("01_synthea_setup.cypher", "Step 1: Database Setup (Indices & Constraints)"),
            ("02_categorize_conditions.cypher", "Step 2: Categorize Conditions (Labels)"),
            ("03_comorbidity_analysis.cypher", "Step 3: Comorbidity Analysis & GDS")
        ]
        
        all_success = True
        for filename, description in scripts:
            success = executor.execute_cypher_file(filename, description)
            if not success:
                all_success = False
                print(f"\n⚠️  Script {filename} had errors, but continuing...")
        
        # Verify setup
        verification_passed = executor.verify_setup()
        
        if all_success and verification_passed:
            print("\n🎉 SUCCESS - All scripts executed and verified!")
            print("   You can now use the Jupyter notebooks.\n")
            return 0
        else:
            print("\n⚠️  PARTIAL SUCCESS - Some issues encountered")
            print("   Notebooks may still work, but check the logs above.\n")
            return 1
    
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        executor.close()


if __name__ == "__main__":
    sys.exit(main())
