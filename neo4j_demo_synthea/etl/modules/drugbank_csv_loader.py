"""
DrugBank CSV Loader - Fast drug data loading from vocabulary CSV
Uses pandas for simple and fast CSV parsing
"""

import pandas as pd
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher


class DrugBankCSVLoader:
    """
    Simplified DrugBank Loader using vocabulary CSV
    Much faster and simpler than XML parsing!
    
    Performance: Loads 17,430 drugs in < 1 second
    """
    
    def __init__(self, csv_path: str):
        self.csv_path = Path(csv_path)
        self.drugs_df = None
    
    def load_csv(self) -> pd.DataFrame:
        """Load vocabulary CSV into pandas DataFrame"""
        print("📦 Loading DrugBank vocabulary CSV...")
        
        # CSV columns: DrugBank ID, Accession Numbers, Common name, CAS, UNII, Synonyms, Standard InChI Key
        self.drugs_df = pd.read_csv(
            self.csv_path,
            dtype=str,  # All as string to avoid type issues
            na_values=['', 'N/A', 'NULL'],
            keep_default_na=False  # Don't interpret 'NA' as NaN
        )
        
        print(f"✅ Loaded {len(self.drugs_df)} drugs")
        return self.drugs_df
    
    def get_drug_by_id(self, drugbank_id: str) -> Optional[Dict]:
        """
        Get drug data by DrugBank ID
        
        Args:
            drugbank_id: DrugBank ID (e.g., 'DB01050')
        
        Returns:
            Drug data dict or None if not found
        """
        if self.drugs_df is None:
            raise RuntimeError("CSV not loaded. Call load_csv() first.")
        
        row = self.drugs_df[self.drugs_df['DrugBank ID'] == drugbank_id]
        if row.empty:
            return None
        
        return row.iloc[0].to_dict()
    
    def search_by_name(self, name: str, threshold: float = 0.85) -> List[Tuple[str, float]]:
        """
        Search drugs by name (fuzzy matching)
        
        Searches in:
        1. Common name (exact match)
        2. Synonyms (exact match in list)
        3. Common name (fuzzy match)
        4. Synonyms (fuzzy match)
        
        Args:
            name: Drug name to search for
            threshold: Minimum similarity score (0.0-1.0)
        
        Returns:
            List of (drugbank_id, confidence_score) tuples, sorted by confidence
        """
        if self.drugs_df is None:
            raise RuntimeError("CSV not loaded. Call load_csv() first.")
        
        name_lower = name.lower().strip()
        matches = []
        
        for _, row in self.drugs_df.iterrows():
            drugbank_id = row['DrugBank ID']
            common_name = str(row['Common name']).lower()
            synonyms = str(row['Synonyms']).lower()
            
            # Exact match in common name
            if name_lower == common_name:
                matches.append((drugbank_id, 1.0))
                continue
            
            # Exact match in synonyms (check each synonym)
            synonym_list = [s.strip() for s in synonyms.split('|') if s.strip()]
            if name_lower in synonym_list:
                matches.append((drugbank_id, 0.99))
                continue
            
            # Fuzzy match in common name
            score = self._fuzzy_match(name_lower, common_name)
            if score >= threshold:
                matches.append((drugbank_id, score))
                continue
            
            # Fuzzy match in synonyms (check each synonym)
            for synonym in synonym_list:
                if synonym:
                    score = self._fuzzy_match(name_lower, synonym)
                    if score >= threshold:
                        matches.append((drugbank_id, score))
                        break
        
        # Sort by confidence (highest first)
        matches.sort(key=lambda x: x[1], reverse=True)
        
        # Remove duplicates (keep highest confidence)
        seen = set()
        unique_matches = []
        for drugbank_id, confidence in matches:
            if drugbank_id not in seen:
                unique_matches.append((drugbank_id, confidence))
                seen.add(drugbank_id)
        
        return unique_matches
    
    def _fuzzy_match(self, str1: str, str2: str) -> float:
        """
        Calculate string similarity using SequenceMatcher
        
        Returns:
            Similarity score (0.0 - 1.0)
        """
        return SequenceMatcher(None, str1, str2).ratio()
    
    def search_by_name_advanced(self, name: str, threshold: float = 0.75) -> List[Tuple[str, float, str]]:
        """
        Advanced search with multiple matching strategies (Option C)
        
        Strategies (in order):
        1. Exact match (score: 1.0)
        2. Synonym exact match (score: 0.99)
        3. Stemming match (score: 0.95)
        4. Levenshtein distance (score: 0.80-0.95)
        5. Fuzzy match (score: threshold - 1.0)
        
        Args:
            name: Drug name to search for
            threshold: Minimum similarity score
        
        Returns:
            List of (drugbank_id, confidence, method) tuples
        """
        if self.drugs_df is None:
            raise RuntimeError("CSV not loaded. Call load_csv() first.")
        
        name_lower = name.lower().strip()
        matches = []
        
        # Helper: Simple stemming (remove common suffixes)
        def stem(word: str) -> str:
            suffixes = ['ing', 'ed', 'ine', 'ate', 'ol', 'il']
            for suffix in suffixes:
                if word.endswith(suffix) and len(word) > len(suffix) + 3:
                    return word[:-len(suffix)]
            return word
        
        # Helper: Levenshtein distance
        def levenshtein_distance(s1: str, s2: str) -> int:
            if len(s1) < len(s2):
                return levenshtein_distance(s2, s1)
            if len(s2) == 0:
                return len(s1)
            
            previous_row = range(len(s2) + 1)
            for i, c1 in enumerate(s1):
                current_row = [i + 1]
                for j, c2 in enumerate(s2):
                    insertions = previous_row[j + 1] + 1
                    deletions = current_row[j] + 1
                    substitutions = previous_row[j] + (c1 != c2)
                    current_row.append(min(insertions, deletions, substitutions))
                previous_row = current_row
            
            return previous_row[-1]
        
        name_stem = stem(name_lower)
        
        for _, row in self.drugs_df.iterrows():
            drugbank_id = row['DrugBank ID']
            common_name = str(row['Common name']).lower()
            synonyms = str(row['Synonyms']).lower()
            
            # Strategy 1: Exact match
            if name_lower == common_name:
                matches.append((drugbank_id, 1.0, 'exact_match'))
                continue
            
            # Strategy 2: Exact synonym match
            synonym_list = [s.strip() for s in synonyms.split('|') if s.strip()]
            if name_lower in synonym_list:
                matches.append((drugbank_id, 0.99, 'synonym_exact'))
                continue
            
            # Strategy 3: Stemming match
            common_stem = stem(common_name)
            if name_stem == common_stem and len(name_stem) > 3:
                matches.append((drugbank_id, 0.95, 'stemming'))
                continue
            
            # Strategy 4: Levenshtein distance (for close typos)
            max_len = max(len(name_lower), len(common_name))
            if max_len > 0:
                distance = levenshtein_distance(name_lower, common_name)
                lev_score = 1.0 - (distance / max_len)
                
                if lev_score >= 0.85:  # Very close match
                    matches.append((drugbank_id, lev_score, 'levenshtein'))
                    continue
            
            # Strategy 5: Fuzzy match (existing logic)
            score = self._fuzzy_match(name_lower, common_name)
            if score >= threshold:
                matches.append((drugbank_id, score, 'fuzzy_match'))
                continue
            
            # Also check synonyms with fuzzy match
            for synonym in synonym_list:
                if synonym:
                    score = self._fuzzy_match(name_lower, synonym)
                    if score >= threshold:
                        matches.append((drugbank_id, score, 'fuzzy_synonym'))
                        break
        
        # Sort by confidence (highest first)
        matches.sort(key=lambda x: x[1], reverse=True)
        
        # Remove duplicates (keep highest confidence)
        seen = set()
        unique_matches = []
        for drugbank_id, confidence, method in matches:
            if drugbank_id not in seen:
                unique_matches.append((drugbank_id, confidence, method))
                seen.add(drugbank_id)
        
        return unique_matches
    
    def extract_drug_name_from_synthea(self, description: str) -> str:
        """
        Extract drug name from Synthea medication description
        
        Examples:
            "Ibuprofen 200 MG Oral Tablet" → "ibuprofen"
            "Acetaminophen 325 MG Oral Tablet" → "acetaminophen"
            "lisinopril 10 MG Oral Tablet" → "lisinopril"
            "Acetaminophen 300 MG / Hydrocodone Bitartrate 5 MG" → "acetaminophen"
            "Yaz 28 Day Pack" → "yaz"
            "Natazia 28 Day Pack" → "natazia"
        
        Args:
            description: Synthea medication description
        
        Returns:
            Extracted drug name (lowercase)
        """
        # Remove brand names in brackets [Tylenol]
        description = re.sub(r'\[.*?\]', '', description)
        
        # Extract first word/phrase before dosage (numbers) or slash
        match = re.match(r'^([A-Za-z\s\-]+?)(?:\s+\d+|\s+/)', description)
        if match:
            name = match.group(1).strip()
        else:
            # Fallback: first word
            name = description.split()[0] if description.split() else description
        
        return name.lower().strip()
    
    def get_all_drugs(self) -> pd.DataFrame:
        """
        Get complete drugs DataFrame
        
        Returns:
            Complete DataFrame with all drugs
        """
        if self.drugs_df is None:
            raise RuntimeError("CSV not loaded. Call load_csv() first.")
        
        return self.drugs_df
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get statistics about loaded drugs
        
        Returns:
            Dict with statistics
        """
        if self.drugs_df is None:
            raise RuntimeError("CSV not loaded. Call load_csv() first.")
        
        return {
            'total_drugs': len(self.drugs_df),
            'drugs_with_cas': self.drugs_df['CAS'].notna().sum(),
            'drugs_with_unii': self.drugs_df['UNII'].notna().sum(),
            'drugs_with_synonyms': self.drugs_df['Synonyms'].notna().sum(),
            'drugs_with_inchi': self.drugs_df['Standard InChI Key'].notna().sum()
        }


# Test function
def test_loader():
    """Test the CSV loader with sample queries"""
    loader = DrugBankCSVLoader("/data/drugbank/drugbank vocabulary.csv")
    df = loader.load_csv()
    
    print(f"\n📊 Statistics:")
    stats = loader.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value:,}")
    
    print(f"\n🔍 Test searches:")
    
    # Test exact matches
    test_names = ["ibuprofen", "acetaminophen", "lisinopril"]
    for name in test_names:
        matches = loader.search_by_name(name)
        if matches:
            drugbank_id, confidence = matches[0]
            drug = loader.get_drug_by_id(drugbank_id)
            print(f"   ✅ {name:<15} → {drugbank_id} ({drug['Common name']}, conf: {confidence:.2f})")
        else:
            print(f"   ❌ {name:<15} → No match found")
    
    # Test Synthea extraction
    print(f"\n🧪 Test Synthea extraction:")
    synthea_examples = [
        "Ibuprofen 200 MG Oral Tablet",
        "Acetaminophen 325 MG Oral Tablet",
        "lisinopril 10 MG Oral Tablet",
        "Acetaminophen 325 MG Oral Tablet [Tylenol]"
    ]
    
    for desc in synthea_examples:
        extracted = loader.extract_drug_name_from_synthea(desc)
        matches = loader.search_by_name(extracted)
        if matches:
            drugbank_id, confidence = matches[0]
            drug = loader.get_drug_by_id(drugbank_id)
            print(f"   ✅ {desc[:40]:<40} → {extracted:<15} → {drug['Common name']}")
        else:
            print(f"   ⚠️  {desc[:40]:<40} → {extracted:<15} → No match")


if __name__ == "__main__":
    test_loader()
