"""
Base utilities for ETL modules
Provides common functions and connection handling
"""

import os
import time
import pandas as pd
from neo4j import GraphDatabase

# Configuration
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j-synthea:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "synthea123")
IMPORT_DIR = "/import"  # Changed from "/import/synthea" to "/import"

class Neo4jConnection:
    """Manages Neo4j database connection with retry logic"""
    
    def __init__(self, uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = None
        self.connect()
    
    def connect(self, max_retries=5):
        """Connect to Neo4j with retry logic"""
        for attempt in range(max_retries):
            try:
                self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
                self.driver.verify_connectivity()
                print(f"✓ Connected to Neo4j at {self.uri}")
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Connection attempt {attempt+1} failed, retrying in 5s...")
                    time.sleep(5)
                else:
                    raise Exception(f"Failed to connect after {max_retries} attempts: {e}")
    
    def close(self):
        """Close the connection"""
        if self.driver:
            self.driver.close()
    
    def run_query(self, query, parameters=None):
        """Execute a Cypher query"""
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

def clean_dataframe(df):
    """Replace NaN values with None for proper Neo4j null handling"""
    return df.where(pd.notna(df), None)

def safe_date(date_str):
    """Safely convert date string, handling None/empty values"""
    if date_str is None or date_str == '' or pd.isna(date_str):
        return None
    return str(date_str).split('T')[0] if 'T' in str(date_str) else str(date_str)

def safe_datetime(datetime_str):
    """Safely convert datetime string, handling None/empty values"""
    if datetime_str is None or datetime_str == '' or pd.isna(datetime_str):
        return None
    return str(datetime_str)

def get_stats(connection):
    """Get database statistics"""
    stats = {}
    
    # Node counts
    node_labels = ["Patient", "Encounter", "Condition", "Medication", "Procedure", 
                   "Observation", "Provider", "Organization", "Payer", "Allergy", 
                   "CarePlan", "Device", "ImagingStudy", "Immunization"]
    
    for label in node_labels:
        result = connection.run_query(f"MATCH (n:{label}) RETURN count(n) as count")
        stats[f"{label}_count"] = result[0]['count'] if result else 0
    
    # Relationship counts
    rel_types = ["HAD_ENCOUNTER", "HAS_CONDITION", "TAKES_MEDICATION", "PRESCRIBED", 
                 "DIAGNOSED", "PERFORMED", "ORDERED", "SEEN_BY", "AT_ORGANIZATION", 
                 "WORKS_AT", "HAS_ALLERGY", "HAS_CAREPLAN", "HAS_DEVICE", 
                 "HAD_IMAGING", "RECEIVED_IMMUNIZATION", "COVERED_BY"]
    
    for rel_type in rel_types:
        result = connection.run_query(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count")
        stats[f"{rel_type}_count"] = result[0]['count'] if result else 0
    
    return stats

def print_stats(stats):
    """Pretty print statistics"""
    print("\n" + "=" * 60)
    print("DATABASE STATISTICS")
    print("=" * 60)
    
    print("\nNode Counts:")
    for key, value in sorted(stats.items()):
        if key.endswith('_count') and not any(rel in key for rel in ["HAD_", "HAS_", "TAKES_", "PRESCRIBED", "DIAGNOSED", "PERFORMED", "ORDERED", "SEEN_", "AT_", "WORKS_", "COVERED_", "RECEIVED_"]):
            label = key.replace('_count', '')
            if value > 0:
                print(f"  {label}: {value}")
    
    print("\nRelationship Counts:")
    for key, value in sorted(stats.items()):
        if any(rel in key for rel in ["HAD_", "HAS_", "TAKES_", "PRESCRIBED", "DIAGNOSED", "PERFORMED", "ORDERED", "SEEN_", "AT_", "WORKS_", "COVERED_", "RECEIVED_"]):
            rel = key.replace('_count', '')
            if value > 0:
                print(f"  {rel}: {value}")
    
    print("\n" + "=" * 60 + "\n")
