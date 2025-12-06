// ============================================================================
// 03_comorbidity_analysis.cypher - Komorbiditäts-Netzwerk & GDS
// ============================================================================
// Zweck: Vollständige Komorbiditäts-Netzwerk-Analyse mit Graph Data Science
// - CO_OCCURS_WITH Beziehungen erstellen
// - RISK_FACTOR_FOR Beziehungen erstellen (Sozial → Klinisch)
// - GDS-Algorithmen: Louvain, PageRank, Betweenness Centrality
// - Validierungs- und Analyse-Queries
//
// HINWEIS: Dataset-agnostisch - Zahlen variieren je nach Patientenanzahl
//
// Wird automatisch ausgeführt von: etl/setup_notebooks.py
// Benötigt: 01_synthea_setup.cypher und 02_categorize_conditions.cypher
// ============================================================================

// ============================================================================
// TEIL 1: KOMORBIDITÄTS-BEZIEHUNGEN ERSTELLEN
// ============================================================================

// 1.1 Erstelle CO_OCCURS_WITH Beziehungen zwischen chronischen Erkrankungen
// Schwellwert: Mindestens 3 Patienten mit beiden Erkrankungen
MATCH (p:Patient)-[:HAS_CONDITION]->(c1:ChronicDisease),
      (p)-[:HAS_CONDITION]->(c2:ChronicDisease)
WHERE id(c1) < id(c2)
WITH c1, c2, count(DISTINCT p) as patient_count
WHERE patient_count >= 3
MERGE (c1)-[r:CO_OCCURS_WITH]->(c2)
SET r.weight = patient_count,
    r.cooccurrenceCount = patient_count,
    r.created = datetime();

// 1.2 Füge Prävalenz-Eigenschaft zu ChronicDisease Knoten hinzu
MATCH (c:ChronicDisease)<-[:HAS_CONDITION]-(p:Patient)
WITH c, count(DISTINCT p) as prevalence
SET c.prevalence = prevalence;

// 1.3 Füge durchschnittliches Diagnosealter zu ChronicDisease Knoten hinzu
MATCH (p:Patient)-[r:HAS_CONDITION]->(c:ChronicDisease)
WHERE r.onset IS NOT NULL AND p.birthDate IS NOT NULL
WITH c, 
     avg(duration.between(date(p.birthDate), date(r.onset)).years) as avg_age
SET c.avgAgeAtDiagnosis = round(avg_age, 1);

// ============================================================================
// TEIL 2: SOZIALE DETERMINANTEN (Risikofaktoren)
// ============================================================================

// 2.1 Erstelle RISK_FACTOR_FOR Beziehungen (Sozial → Klinisch)
// Schwellwert: Mindestens 5 Patienten mit sowohl sozialem Faktor als auch chronischer Erkrankung
MATCH (p:Patient)-[:HAS_CONDITION]->(social:SocialDeterminant),
      (p)-[:HAS_CONDITION]->(chronic:ChronicDisease)
WITH social, chronic, count(DISTINCT p) as patient_count
WHERE patient_count >= 5
MERGE (social)-[r:RISK_FACTOR_FOR]->(chronic)
SET r.weight = patient_count,
    r.created = datetime();

// ============================================================================
// TEIL 3: GRAPH DATA SCIENCE (GDS) ALGORITHMEN
// ============================================================================

// 3.1 GDS Graph-Projektion erstellen
CALL gds.graph.project(
  'comorbidity-network',
  'ChronicDisease',
  {
    CO_OCCURS_WITH: {
      type: 'CO_OCCURS_WITH',
      orientation: 'UNDIRECTED',
      properties: 'weight'
    }
  },
  {
    nodeProperties: ['prevalence', 'avgAgeAtDiagnosis']
  }
)
YIELD graphName, nodeCount, relationshipCount
RETURN graphName, nodeCount, relationshipCount;

// 3.2 Louvain Community Detection (Erkrankungs-Cluster erkennen)
CALL gds.louvain.write(
  'comorbidity-network',
  {
    writeProperty: 'communityId',
    relationshipWeightProperty: 'weight',
    includeIntermediateCommunities: false
  }
)
YIELD communityCount, modularity, ranLevels
RETURN communityCount, round(modularity, 3) as modularity, ranLevels;

// 3.3 PageRank (Hub-Erkrankungen identifizieren)
CALL gds.pageRank.write(
  'comorbidity-network',
  {
    writeProperty: 'pageRank',
    relationshipWeightProperty: 'weight',
    maxIterations: 20,
    dampingFactor: 0.85
  }
)
YIELD ranIterations, didConverge
RETURN ranIterations, didConverge;

// 3.4 Betweenness Centrality (Brücken-Erkrankungen finden)
CALL gds.betweenness.write(
  'comorbidity-network',
  {
    writeProperty: 'betweenness'
  }
)
YIELD centralityDistribution
RETURN centralityDistribution.min as min,
       round(centralityDistribution.mean, 2) as mean,
       round(centralityDistribution.max, 2) as max;

// ============================================================================
// TEIL 4: VALIDIERUNGS- UND ANALYSE-QUERIES
// ============================================================================

// 4.1 Datenbank-Übersicht
MATCH (p:Patient) WITH count(p) as total_patients
MATCH (c:ChronicDisease) WITH total_patients, count(c) as total_chronic_diseases
MATCH ()-[co:CO_OCCURS_WITH]->() WITH total_patients, total_chronic_diseases, count(co) as comorbidity_pairs
MATCH ()-[rf:RISK_FACTOR_FOR]->() WITH total_patients, total_chronic_diseases, comorbidity_pairs, count(rf) as risk_factors
MATCH (p:Patient)-[:HAS_CONDITION]->(c:ChronicDisease)
WITH total_patients, total_chronic_diseases, comorbidity_pairs, risk_factors,
     p, count(DISTINCT c) as chronic_count
WHERE chronic_count >= 2
WITH total_patients, total_chronic_diseases, comorbidity_pairs, risk_factors,
     count(p) as comorbidity_patients,
     avg(chronic_count) as avg_chronic_per_patient,
     max(chronic_count) as max_chronic_per_patient
MATCH (c:ChronicDisease)
WITH total_patients, total_chronic_diseases, comorbidity_pairs, risk_factors,
     comorbidity_patients, avg_chronic_per_patient, max_chronic_per_patient,
     c.communityId as communityId, count(c) as comm_size
ORDER BY comm_size DESC
RETURN 
  total_patients,
  total_chronic_diseases,
  comorbidity_pairs,
  risk_factors,
  comorbidity_patients,
  round(avg_chronic_per_patient, 2) as avg_chronic_per_patient,
  max_chronic_per_patient,
  count(DISTINCT communityId) as disease_communities,
  round(100.0 * comorbidity_patients / total_patients, 1) as comorbidity_percentage;

// 4.2 Top 10 Disease Communities
MATCH (c:ChronicDisease)
WITH c.communityId as communityId, 
     collect(c.description) as diseases,
     count(c) as disease_count,
     sum(c.prevalence) as total_patients
ORDER BY disease_count DESC
LIMIT 10
RETURN communityId, disease_count, total_patients, 
       diseases[0..5] as sample_diseases;

// 4.3 Top 15 Hub Diseases (PageRank)
MATCH (c:ChronicDisease)
RETURN c.description as disease,
       c.prevalence as prevalence,
       round(c.pageRank, 4) as pageRank,
       c.communityId as community
ORDER BY c.pageRank DESC
LIMIT 15;

// 4.4 Top 15 Bridge Diseases (Betweenness Centrality)
MATCH (c:ChronicDisease)
WHERE c.betweenness > 0
RETURN c.description as disease,
       c.prevalence as prevalence,
       round(c.betweenness, 2) as betweenness,
       c.communityId as community
ORDER BY c.betweenness DESC
LIMIT 15;

// 4.5 Top 40 Comorbidity Pairs
MATCH (p:Patient)-[:HAS_CONDITION]->(c1:ChronicDisease),
      (p)-[:HAS_CONDITION]->(c2:ChronicDisease)
WHERE id(c1) < id(c2)
WITH c1.description as disease1, 
     c2.description as disease2, 
     count(p) as patient_count
WHERE patient_count >= 5
RETURN disease1, disease2, patient_count
ORDER BY patient_count DESC
LIMIT 40;

// 4.6 Top Social Risk Factors
MATCH (social:SocialDeterminant)-[r:RISK_FACTOR_FOR]->(chronic:ChronicDisease)
WITH social.description as social_factor,
     count(DISTINCT chronic) as diseases_affected,
     sum(r.weight) as total_impact
ORDER BY total_impact DESC
LIMIT 15
RETURN social_factor, diseases_affected, total_impact;

// 4.7 Strongest Social → Clinical Links
MATCH (social:SocialDeterminant)-[r:RISK_FACTOR_FOR]->(chronic:ChronicDisease)
RETURN social.description as social_factor,
       chronic.description as chronic_disease,
       r.weight as patients_affected
ORDER BY r.weight DESC
LIMIT 20;

// 4.8 Beispiel: Größte Community anzeigen
MATCH (c:ChronicDisease)
WITH c.communityId as communityId, count(c) as size
ORDER BY size DESC
LIMIT 1
WITH communityId
MATCH (c:ChronicDisease)
WHERE c.communityId = communityId
RETURN c.description as disease,
       c.prevalence as patients,
       round(c.pageRank, 3) as importance
ORDER BY c.prevalence DESC
LIMIT 15;

// ============================================================================
// TEIL 5: CLEANUP (optional, bei Neustart)
// ============================================================================

// GDS Graph-Projektion löschen (zum Neuerstellen oder Memory freigeben)
// CALL gds.graph.drop('comorbidity-network') YIELD graphName;

// Alle Komorbiditäts-Analyse-Artefakte entfernen
// MATCH ()-[r:CO_OCCURS_WITH]->() DELETE r;
// MATCH ()-[r:RISK_FACTOR_FOR]->() DELETE r;
// MATCH (c:ChronicDisease) REMOVE c.communityId, c.pageRank, c.betweenness, c.prevalence, c.avgAgeAtDiagnosis;

// ============================================================================
// KOMORBIDITÄTS-ANALYSE ABGESCHLOSSEN
// ============================================================================
