// ============================================================================
// 01_synthea_setup.cypher - Datenbank-Setup
// ============================================================================
// Zweck: Initiales Setup für Synthea Health Demo
// - Constraints und Indizes erstellen (Performance-Optimierung)
// - Datenbank-Statistiken anzeigen
// 
// Voraussetzungen: Datenbank mit load_synthea_complete.py gefüllt
// Wird automatisch ausgeführt von: etl/setup_notebooks.py
// ============================================================================

// ----------------------------------------------------------------------------
// SCHRITT 1: Constraints prüfen
// ----------------------------------------------------------------------------

SHOW CONSTRAINTS;

// ----------------------------------------------------------------------------
// SCHRITT 2: Performance-Indizes erstellen
// ----------------------------------------------------------------------------

// Condition code index (SNOMED-CT Lookups)
CREATE INDEX condition_code_idx IF NOT EXISTS FOR (c:Condition) ON (c.code);

// Medication code index (RxNorm Lookups)
CREATE INDEX medication_code_idx IF NOT EXISTS FOR (m:Medication) ON (m.code);

// Patient birthDate index (Altersbasierte Queries)
CREATE INDEX patient_birthdate_idx IF NOT EXISTS FOR (p:Patient) ON (p.birthDate);

// Patient deathDate index (Mortalitätsanalyse)
CREATE INDEX patient_deathdate_idx IF NOT EXISTS FOR (p:Patient) ON (p.deathDate);

// Encounter zeitliche Indizes
CREATE INDEX encounter_start_idx IF NOT EXISTS FOR (e:Encounter) ON (e.start);
CREATE INDEX encounter_stop_idx IF NOT EXISTS FOR (e:Encounter) ON (e.stop);

// Provider Spezialisierung
CREATE INDEX provider_specialty_idx IF NOT EXISTS FOR (pr:Provider) ON (pr.specialties);

// Organization Name
CREATE INDEX organization_name_idx IF NOT EXISTS FOR (o:Organization) ON (o.name);

// ----------------------------------------------------------------------------
// SCHRITT 3: Datenbank-Statistiken
// ----------------------------------------------------------------------------

// Knoten-Anzahl nach Label
CALL db.labels() YIELD label
CALL apoc.cypher.run('MATCH (n:'+label+') RETURN count(n) as count', {}) 
YIELD value 
RETURN label, value.count as count 
ORDER BY count DESC;

// Beziehungs-Anzahl nach Typ
CALL db.relationshipTypes() YIELD relationshipType as type
CALL apoc.cypher.run('MATCH ()-[r:'+type+']->() RETURN count(r) as count', {}) 
YIELD value 
RETURN type, value.count as count 
ORDER BY count DESC;

// ----------------------------------------------------------------------------
// SCHRITT 4: Datenqualität prüfen
// ----------------------------------------------------------------------------

// Prüfe verwaiste Knoten (sollte minimal sein)
MATCH (n) WHERE NOT (n)--() 
RETURN labels(n) as nodeType, count(n) as orphanCount;

// Prüfe dass alle Patienten Encounters haben
MATCH (p:Patient)
OPTIONAL MATCH (p)-[:HAD_ENCOUNTER]->(e:Encounter)
WITH p, count(e) as encounterCount
WHERE encounterCount = 0
RETURN count(p) as patientsWithoutEncounters;

// Condition-Abdeckung
MATCH (p:Patient)
OPTIONAL MATCH (p)-[:HAS_CONDITION]->(c:Condition)
WITH p, count(c) as conditionCount
RETURN 
  count(p) as totalPatients,
  round(avg(conditionCount), 1) as avgConditions,
  min(conditionCount) as minConditions,
  max(conditionCount) as maxConditions;

// ============================================================================
// SETUP ABGESCHLOSSEN
// ----------------------------------------------------------------------------

// Next step: Run 02_categorize_conditions.cypher
