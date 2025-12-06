"""
Medication Mapper Module - Robust Synthea → DrugBank Mapping

This module provides a high-level interface for mapping Synthea medications
to DrugBank drugs, with comprehensive error handling and logging.

Features:
- Automatic mapping of all current Medications
- Confidence-based matching strategies
- Manual override support
- Detailed logging and statistics
"""

from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import csv
import os
from pathlib import Path
from neo4j import Session
from .drugbank_csv_loader import DrugBankCSVLoader


@dataclass
class MappingResult:
    """Result of medication mapping operation"""
    total_medications: int
    mapped: int
    unmapped: int
    high_confidence: int  # ≥0.95
    medium_confidence: int  # 0.85-0.94
    low_confidence: int  # 0.75-0.84
    skipped: int  # Manual override or already mapped
    unmapped_list: List[Tuple[str, str]]  # (description, extracted_name)


@dataclass
class ManualMapping:
    """Manual override for medication mapping"""
    synthea_code: str
    drugbank_id: str
    confidence: float
    reason: str


class MedicationMapper:
    """
    High-level mapper for Synthea → DrugBank medication mapping
    
    Usage:
        mapper = MedicationMapper(session, csv_loader)
        result = mapper.map_all_medications()
        print(f"Mapped {result.mapped}/{result.total_medications}")
    """
    
    def __init__(self, 
                 session: Session, 
                 csv_loader: DrugBankCSVLoader,
                 manual_mappings_file: Optional[str] = None):
        """
        Initialize mapper
        
        Args:
            session: Neo4j session
            csv_loader: Loaded DrugBankCSVLoader instance
            manual_mappings_file: Optional CSV file with manual overrides
        """
        self.session = session
        self.csv_loader = csv_loader
        self.manual_mappings = {}
        
        if manual_mappings_file and os.path.exists(manual_mappings_file):
            self._load_manual_mappings(manual_mappings_file)
    
    def _load_manual_mappings(self, filepath: str):
        """Load manual mappings from CSV file"""
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                mapping = ManualMapping(
                    synthea_code=row['synthea_code'],
                    drugbank_id=row['drugbank_id'],
                    confidence=float(row['confidence']),
                    reason=row['reason']
                )
                self.manual_mappings[mapping.synthea_code] = mapping
        
        print(f"   📋 Loaded {len(self.manual_mappings)} manual mappings")
    
    def get_current_medications(self) -> List[Dict]:
        """
        Get all Medications currently connected to Patients
        
        Returns:
            List of medication dicts with code and description
        """
        result = self.session.run("""
            MATCH (p:Patient)-[:TAKES_MEDICATION]->(m:Medication)
            WITH DISTINCT m
            RETURN m.code as code,
                   m.description as description
            ORDER BY m.description
        """)
        
        return [{"code": r["code"], "description": r["description"]} for r in result]
    
    def delete_old_mappings(self) -> int:
        """
        Delete all existing MAPPED_TO relationships
        
        Returns:
            Number of deleted relationships
        """
        result = self.session.run("""
            MATCH ()-[old:MAPPED_TO]->()
            DELETE old
            RETURN count(*) as deleted
        """)
        
        return result.single()["deleted"]
    
    def map_medication(self, 
                       med_code: str, 
                       med_description: str,
                       confidence_threshold: float = 0.75) -> Optional[Tuple[str, float, str]]:
        """
        Map a single medication to DrugBank
        
        Args:
            med_code: Synthea medication code
            med_description: Synthea medication description
            confidence_threshold: Minimum confidence score
        
        Returns:
            Tuple of (drugbank_id, confidence, method) or None if no match
        """
        # Check manual override first
        if med_code in self.manual_mappings:
            manual = self.manual_mappings[med_code]
            return (manual.drugbank_id, manual.confidence, 'manual_override')
        
        # Extract drug name from description
        extracted_name = self.csv_loader.extract_drug_name_from_synthea(med_description)
        
        # Search in DrugBank
        matches = self.csv_loader.search_by_name(extracted_name, threshold=confidence_threshold)
        
        if matches:
            best_drugbank_id, confidence = matches[0]
            return (best_drugbank_id, confidence, 'csv_lookup')
        
        return None
    
    def create_mapping_relationship(self,
                                   med_code: str,
                                   drugbank_id: str,
                                   confidence: float,
                                   method: str,
                                   extracted_name: str = ""):
        """
        Create MAPPED_TO relationship in Neo4j
        
        Args:
            med_code: Synthea medication code
            drugbank_id: DrugBank drug ID
            confidence: Confidence score (0.0-1.0)
            method: Mapping method (csv_lookup, manual_override, etc.)
            extracted_name: Extracted drug name from Synthea description
        """
        self.session.run("""
            MATCH (m:Medication {code: $med_code})
            MATCH (d:DrugBankDrug {drugbank_id: $drugbank_id})
            MERGE (m)-[r:MAPPED_TO]->(d)
            SET r.confidence = $confidence,
                r.method = $method,
                r.extracted_name = $extracted_name,
                r.created = datetime()
        """, med_code=med_code,
             drugbank_id=drugbank_id,
             confidence=confidence,
             method=method,
             extracted_name=extracted_name)
    
    def map_all_medications(self, 
                           confidence_threshold: float = 0.75,
                           delete_old: bool = True,
                           verbose: bool = True) -> MappingResult:
        """
        Map all current medications to DrugBank
        
        Args:
            confidence_threshold: Minimum confidence score (0.0-1.0)
            delete_old: Whether to delete old mappings first
            verbose: Print detailed progress
        
        Returns:
            MappingResult with statistics
        """
        # Delete old mappings
        if delete_old:
            deleted = self.delete_old_mappings()
            if verbose and deleted > 0:
                print(f"   🗑️  Deleted {deleted} old mappings")
        
        # Get current medications
        medications = self.get_current_medications()
        total = len(medications)
        
        if verbose:
            print(f"   🎯 Mapping {total} medications...")
        
        # Counters
        mapped_count = 0
        high_conf = 0
        medium_conf = 0
        low_conf = 0
        unmapped = []
        
        # Map each medication
        for med in medications:
            result = self.map_medication(
                med["code"], 
                med["description"],
                confidence_threshold
            )
            
            if result:
                drugbank_id, confidence, method = result
                
                # Extract name for relationship
                extracted_name = self.csv_loader.extract_drug_name_from_synthea(med["description"])
                
                # Create relationship
                self.create_mapping_relationship(
                    med["code"],
                    drugbank_id,
                    confidence,
                    method,
                    extracted_name
                )
                
                mapped_count += 1
                
                # Categorize by confidence
                if confidence >= 0.95:
                    high_conf += 1
                    indicator = "✅"
                elif confidence >= 0.85:
                    medium_conf += 1
                    indicator = "🟡"
                else:
                    low_conf += 1
                    indicator = "🟠"
                
                # Verbose output
                if verbose:
                    drug_info = self.csv_loader.get_drug_by_id(drugbank_id)
                    synthea_short = med['description'][:45]
                    drugbank_name = drug_info['Common name'][:20]
                    method_icon = "📋" if method == 'manual_override' else "🔍"
                    print(f"   {indicator} {method_icon} {synthea_short:<45} → {drugbank_name:<20} ({confidence:.2f})")
            else:
                extracted_name = self.csv_loader.extract_drug_name_from_synthea(med["description"])
                unmapped.append((med["description"], extracted_name))
        
        return MappingResult(
            total_medications=total,
            mapped=mapped_count,
            unmapped=len(unmapped),
            high_confidence=high_conf,
            medium_confidence=medium_conf,
            low_confidence=low_conf,
            skipped=0,
            unmapped_list=unmapped
        )
    
    def verify_interactions(self) -> Dict[str, int]:
        """
        Verify that mapped medications can query interactions
        
        Returns:
            Dict with interaction statistics
        """
        result = self.session.run("""
            MATCH (p:Patient)-[:TAKES_MEDICATION]->(m1:Medication)-[:MAPPED_TO]->(db1:DrugBankDrug),
                  (p)-[:TAKES_MEDICATION]->(m2:Medication)-[:MAPPED_TO]->(db2:DrugBankDrug),
                  (db1)-[i:INTERACTS_WITH]-(db2)
            WHERE id(m1) < id(m2)
            RETURN count(*) as total,
                   sum(CASE WHEN i.severity = 'HIGH' THEN 1 ELSE 0 END) as high,
                   sum(CASE WHEN i.severity = 'MODERATE' THEN 1 ELSE 0 END) as moderate,
                   sum(CASE WHEN i.severity = 'LOW' THEN 1 ELSE 0 END) as low
        """)
        
        stats = result.single()
        return {
            'total': stats['total'],
            'high': stats['high'],
            'moderate': stats['moderate'],
            'low': stats['low']
        } if stats else {'total': 0, 'high': 0, 'moderate': 0, 'low': 0}
    
    def get_interaction_examples(self, limit: int = 5) -> List[Dict]:
        """
        Get example drug interactions for testing
        
        Args:
            limit: Maximum number of examples
        
        Returns:
            List of interaction examples
        """
        result = self.session.run("""
            MATCH (p:Patient)-[:TAKES_MEDICATION]->(m1:Medication)-[:MAPPED_TO]->(db1:DrugBankDrug),
                  (p)-[:TAKES_MEDICATION]->(m2:Medication)-[:MAPPED_TO]->(db2:DrugBankDrug),
                  (db1)-[i:INTERACTS_WITH]-(db2)
            WHERE i.severity IN ['HIGH', 'MODERATE']
            RETURN p.firstName + ' ' + p.lastName as patient,
                   m1.description as med1,
                   m2.description as med2,
                   db1.common_name as drug1,
                   db2.common_name as drug2,
                   i.severity as severity,
                   i.description as description
            LIMIT $limit
        """, limit=limit)
        
        return [dict(r) for r in result]
    
    def export_unmapped_for_review(self, filepath: str, unmapped_list: List[Tuple[str, str]]):
        """
        Export unmapped medications to CSV for manual review
        
        Args:
            filepath: Output CSV file path
            unmapped_list: List of (description, extracted_name) tuples
        """
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['synthea_description', 'extracted_name', 'suggested_drugbank_id', 'confidence', 'notes'])
            
            for desc, extracted in unmapped_list:
                writer.writerow([desc, extracted, '', '', ''])
        
        print(f"   📝 Exported {len(unmapped_list)} unmapped medications to: {filepath}")
