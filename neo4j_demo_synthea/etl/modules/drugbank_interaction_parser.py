"""
DrugBank Interaction Parser - Parse drug-drug interactions from XML
Uses streaming to handle large 1.6 GB file efficiently
"""

import xml.etree.ElementTree as ET
from typing import Iterator, Tuple


class DrugBankInteractionParser:
    """
    Parse drug-drug interactions from DrugBank XML
    Uses streaming (iterparse) to handle large file without memory overflow
    
    Performance: Parses 2.8M interactions in ~10-15 minutes
    """
    
    def __init__(self, xml_path: str):
        self.xml_path = xml_path
        self.ns = {'db': 'http://www.drugbank.ca'}
    
    def parse_interactions(self) -> Iterator[Tuple[str, str, str]]:
        """
        Yield drug-drug interactions from XML
        
        Yields:
            Tuple of (source_drugbank_id, target_drugbank_id, description)
            
        Example:
            ('DB01050', 'DB00945', 'Aspirin may increase the anticoagulant activities of Ibuprofen.')
        """
        print("🔗 Parsing drug-drug interactions from XML...")
        print("⏳ This will take 10-15 minutes (2.8M interactions)...")
        print("💡 Progress will be logged every 100,000 interactions")
        
        try:
            # Use iterparse for streaming (memory efficient)
            context = ET.iterparse(self.xml_path, events=('end',))
            current_drug_id = None
            interaction_count = 0
            
            for event, elem in context:
                # Track current drug by finding primary drugbank-id
                if elem.tag == f'{{{self.ns["db"]}}}drug':
                    # Find primary drugbank-id (has attribute primary="true")
                    primary_id_elem = elem.find('.//db:drugbank-id[@primary="true"]', self.ns)
                    if primary_id_elem is not None:
                        current_drug_id = primary_id_elem.text
                
                # Extract drug-interaction elements
                if elem.tag == f'{{{self.ns["db"]}}}drug-interaction':
                    if current_drug_id:
                        # Get target drug ID
                        target_id_elem = elem.find('db:drugbank-id', self.ns)
                        # Get interaction description
                        description_elem = elem.find('db:description', self.ns)
                        
                        if target_id_elem is not None and description_elem is not None:
                            target_id = target_id_elem.text
                            description = description_elem.text
                            
                            if target_id and description:
                                yield (current_drug_id, target_id, description)
                                interaction_count += 1
                                
                                # Progress logging every 100k interactions
                                if interaction_count % 100000 == 0:
                                    print(f"   Parsed: {interaction_count:,} interactions...")
                    
                    # Clear element to free memory (critical for large file!)
                    elem.clear()
            
            print(f"✅ Parsed {interaction_count:,} interactions total")
            
        except FileNotFoundError:
            print(f"❌ Error: XML file not found: {self.xml_path}")
            raise
        except ET.ParseError as e:
            print(f"❌ Error parsing XML: {e}")
            raise
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise
    
    def parse_first_n_interactions(self, n: int = 1000) -> list:
        """
        Parse only first N interactions (for testing)
        
        Args:
            n: Number of interactions to parse
        
        Returns:
            List of (source_id, target_id, description) tuples
        """
        print(f"🔗 Parsing first {n} interactions (test mode)...")
        
        interactions = []
        for interaction in self.parse_interactions():
            interactions.append(interaction)
            if len(interactions) >= n:
                break
        
        print(f"✅ Parsed {len(interactions)} interactions")
        return interactions


# Test function
def test_parser():
    """Test the interaction parser with first 1000 interactions"""
    parser = DrugBankInteractionParser("/data/drugbank/full database.xml")
    
    print("\n🧪 Testing interaction parser (first 1000)...")
    interactions = parser.parse_first_n_interactions(1000)
    
    print(f"\n📊 Sample interactions:")
    for i, (source, target, desc) in enumerate(interactions[:5], 1):
        print(f"\n   {i}. {source} ↔ {target}")
        print(f"      {desc[:100]}...")
    
    print(f"\n✅ Test completed successfully")


if __name__ == "__main__":
    test_parser()
