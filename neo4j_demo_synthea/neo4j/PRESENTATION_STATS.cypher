// ============================================================================
// PRESENTATION_STATS.cypher
// Quick reference queries for presentation - November 8, 2025
// ============================================================================

// ============================================================================
// 1. DATABASE OVERVIEW - Main Statistics
// ============================================================================
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
     c.communityId as communityId
RETURN 
  total_patients,
  total_chronic_diseases,
  comorbidity_pairs,
  risk_factors,
  comorbidity_patients,
  round(100.0 * comorbidity_patients / total_patients, 1) as comorbidity_percentage,
  round(avg_chronic_per_patient, 2) as avg_chronic_per_patient,
  max_chronic_per_patient,
  count(DISTINCT communityId) as disease_communities;

// Expected Output:
// total_patients: 497
// total_chronic_diseases: 77
// comorbidity_pairs: 432
// risk_factors: 848
// comorbidity_patients: 316
// comorbidity_percentage: 63.6%
// avg_chronic_per_patient: 5.31
// max_chronic_per_patient: 22
// disease_communities: 29


// ============================================================================
// 2. TOP 5 HUB DISEASES (PageRank)
// ============================================================================
MATCH (c:ChronicDisease)
RETURN c.description as disease,
       c.prevalence as patients,
       round(c.pageRank, 3) as pageRank_score,
       c.communityId as community
ORDER BY c.pageRank DESC
LIMIT 5;

// Expected:
// 1. Prediabetes (198 patients, PageRank: 4.328)
// 2. Anemia (184 patients, PageRank: 4.221)
// 3. Essential hypertension (109 patients, PageRank: 3.261)
// 4. Ischemic heart disease (79 patients, PageRank: 2.362)
// 5. Chronic sinusitis (114 patients, PageRank: 2.354)


// ============================================================================
// 3. TOP 5 BRIDGE DISEASES (Betweenness Centrality)
// ============================================================================
MATCH (c:ChronicDisease)
WHERE c.betweenness > 0
RETURN c.description as disease,
       c.prevalence as patients,
       round(c.betweenness, 1) as betweenness_score,
       c.communityId as community
ORDER BY c.betweenness DESC
LIMIT 5;

// Expected:
// 1. Prediabetes (198 patients, Betweenness: 232.8)
// 2. Anemia (184 patients, Betweenness: 229.9)
// 3. Chronic pain (100 patients, Betweenness: 183.7)
// 4. Essential hypertension (109 patients, Betweenness: 101.1)
// 5. Childhood asthma (13 patients, Betweenness: 100.0)


// ============================================================================
// 4. TOP 3 DISEASE COMMUNITIES (Louvain Clustering)
// ============================================================================

// Community 52: METABOLIC-RENAL CLUSTER
MATCH (c:ChronicDisease)
WHERE c.communityId = 52
RETURN 'Community 52: Metabolic-Renal' as cluster_name,
       count(c) as diseases,
       sum(c.prevalence) as total_patients,
       collect(c.description)[0..5] as sample_diseases;

// Community 42: CARDIOVASCULAR-METABOLIC CLUSTER  
MATCH (c:ChronicDisease)
WHERE c.communityId = 42
RETURN 'Community 42: Cardiovascular-Metabolic' as cluster_name,
       count(c) as diseases,
       sum(c.prevalence) as total_patients,
       collect(c.description)[0..5] as sample_diseases;

// Community 29: CHRONIC PAIN-NEUROLOGICAL CLUSTER
MATCH (c:ChronicDisease)
WHERE c.communityId = 29
RETURN 'Community 29: Chronic Pain-Neurological' as cluster_name,
       count(c) as diseases,
       sum(c.prevalence) as total_patients,
       collect(c.description)[0..5] as sample_diseases;


// ============================================================================
// 5. TOP 5 COMORBIDITY PAIRS
// ============================================================================
MATCH (c1:ChronicDisease)-[r:CO_OCCURS_WITH]-(c2:ChronicDisease)
WHERE id(c1) < id(c2)
RETURN c1.description as disease1,
       c2.description as disease2,
       r.weight as shared_patients
ORDER BY r.weight DESC
LIMIT 5;

// Expected:
// 1. Prediabetes + Anemia (154 patients)
// 2. Chronic Pain + Chronic Low Back Pain (82 patients)
// 3. Prediabetes + Essential Hypertension (69 patients)
// 4. Anemia + Essential Hypertension (65 patients)
// 5. Chronic Pain + Chronic Neck Pain (62 patients)


// ============================================================================
// 6. TOP 5 SOCIAL RISK FACTORS
// ============================================================================
MATCH (social:SocialDeterminant)-[r:RISK_FACTOR_FOR]->(chronic:ChronicDisease)
WITH social.description as social_factor,
     count(DISTINCT chronic) as diseases_affected,
     sum(r.weight) as total_impact
ORDER BY total_impact DESC
LIMIT 5
RETURN social_factor, diseases_affected, total_impact;

// Expected:
// 1. Stress (46 diseases, 1628 links)
// 2. Full-time employment (45 diseases, 1613 links)
// 3. Part-time employment (41 diseases, 1463 links)
// 4. Social isolation (38 diseases, 1230 links)
// 5. Limited social contact (41 diseases, 1217 links)


// ============================================================================
// 7. STRONGEST SOCIAL → CLINICAL LINKS (Top 5)
// ============================================================================
MATCH (social:SocialDeterminant)-[r:RISK_FACTOR_FOR]->(chronic:ChronicDisease)
RETURN social.description as social_factor,
       chronic.description as chronic_disease,
       r.weight as patients_affected
ORDER BY r.weight DESC
LIMIT 5;

// Expected:
// 1. Stress → Prediabetes (197 patients)
// 2. Full-time Employment → Prediabetes (197 patients)
// 3. Stress → Anemia (182 patients)
// 4. Full-time Employment → Anemia (182 patients)
// 5. Part-time Employment → Prediabetes (174 patients)


// ============================================================================
// 8. METABOLIC SYNDROME CASCADE (Disease Progression)
// ============================================================================
MATCH path = (start:ChronicDisease)-[:CO_OCCURS_WITH*1..3]-(end:ChronicDisease)
WHERE start.description CONTAINS 'Prediabetes'
  AND end.description CONTAINS 'End-stage renal disease'
WITH path, length(path) as path_length
ORDER BY path_length
LIMIT 1
RETURN [n in nodes(path) | n.description] as progression_pathway,
       path_length;

// Shows typical progression:
// Prediabetes → Diabetes Type 2 → Kidney Disease → CKD Stages → ESRD


// ============================================================================
// 9. HIGH-RISK PATIENT PROFILE
// ============================================================================
MATCH (p:Patient)-[:HAS_CONDITION]->(c:ChronicDisease)
WITH p, count(DISTINCT c) as chronic_count
ORDER BY chronic_count DESC
LIMIT 1
MATCH (p)-[:HAS_CONDITION]->(disease:ChronicDisease)
RETURN p.patient_id as patient_id,
       chronic_count as total_chronic_diseases,
       collect(disease.description)[0..10] as top_10_conditions;

// Shows patient with maximum comorbidities (22 diseases)


// ============================================================================
// 10. QUICK NETWORK STATS
// ============================================================================
CALL apoc.meta.stats() 
YIELD nodeCount, relCount, labels, relTypesCount
RETURN nodeCount, relCount, labels, relTypesCount;

// Overview of entire database structure


// ============================================================================
// 11. PRESENTATION ONE-LINER QUERIES
// ============================================================================

// How many patients have diabetes AND heart disease?
MATCH (p:Patient)-[:HAS_CONDITION]->(d1:ChronicDisease),
      (p)-[:HAS_CONDITION]->(d2:ChronicDisease)
WHERE d1.description CONTAINS 'diabetes'
  AND d2.description CONTAINS 'heart disease'
RETURN count(DISTINCT p) as patients_with_both;

// What percentage of stressed patients develop hypertension?
MATCH (p:Patient)-[:HAS_CONDITION]->(stress:SocialDeterminant {description: 'Stress (finding)'})
WITH count(p) as stressed_patients
MATCH (p:Patient)-[:HAS_CONDITION]->(stress:SocialDeterminant {description: 'Stress (finding)'}),
      (p)-[:HAS_CONDITION]->(ht:ChronicDisease {description: 'Essential hypertension (disorder)'})
WITH stressed_patients, count(DISTINCT p) as stressed_with_ht
RETURN stressed_patients,
       stressed_with_ht,
       round(100.0 * stressed_with_ht / stressed_patients, 1) as percentage;

// How many communities have >10 diseases?
MATCH (c:ChronicDisease)
WITH c.communityId as community, count(c) as size
WHERE size > 10
RETURN count(community) as large_communities;

// Average age at diagnosis for diabetes?
MATCH (c:ChronicDisease)
WHERE c.description CONTAINS 'Diabetes mellitus type 2'
RETURN c.avgAgeAtDiagnosis as avg_age_years;

// Most connected disease (highest degree)?
MATCH (c:ChronicDisease)-[r:CO_OCCURS_WITH]-()
RETURN c.description as disease,
       count(r) as connections
ORDER BY connections DESC
LIMIT 1;


// ============================================================================
// END OF PRESENTATION STATS
// ============================================================================
