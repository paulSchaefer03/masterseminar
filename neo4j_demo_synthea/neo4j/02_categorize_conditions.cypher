// ============================================================================
// 02_categorize_conditions.cypher - Erkrankungs-Kategorisierung
// ============================================================================
// Zweck: Kategorisiert Synthea Conditions in sinnvolle Gruppen
// - ChronicDisease: Chronische Erkrankungen (Hauptziel für Komorbiditätsanalyse)
// - SocialDeterminant: Soziale Risikofaktoren
// - AcuteCondition: Akute/temporäre Erkrankungen
// - DentalCondition: Zahnerkrankungen
// - Administrative: Administrative Einträge (Rauschen)
//
// WICHTIG: Diese Kategorisierung ist KRITISCH für aussagekräftige Analysen!
// Ohne Kategorisierung sind 85% der "Conditions" irrelevantes Rauschen.
//
// Wird automatisch ausgeführt von: etl/setup_notebooks.py
// ============================================================================

// ----------------------------------------------------------------------------
// SCHRITT 1: Kategorisierung - CHRONISCHE ERKRANKUNGEN
// ----------------------------------------------------------------------------
// Zielgruppe: Erkrankungen für Komorbiditätsanalyse
// Kriterien: 
//   - Enthält "disorder", "disease", "syndrome" ODER
//   - Klinische Befunde: obesity, prediabetes, chronic pain, metabolic, etc.
//   - ABER NICHT: viral/akut/infektiös (temporär)
//   - ABER NICHT: dental/zahnbezogen (separate Kategorie)
//   - ABER NICHT: sozial/beruflich (separate Kategorie)

MATCH (c:Condition)
WHERE (c.description =~ '.*disorder.*' 
       OR c.description =~ '.*disease.*' 
       OR c.description =~ '.*syndrome.*'
       OR c.description =~ '.*(obesity|obese).*'
       OR c.description =~ '.*(prediabetes|pre-diabetes).*'
       OR c.description =~ '.*(chronic pain|chronic low back pain).*'
       OR c.description =~ '.*(body mass index 30|body mass index 40).*'
       OR c.description =~ '.*(metabolic|cardiovascular|coronary|ischemic).*')
  AND NOT c.description =~ '.*(viral|acute|infection|sinusitis|pharyngitis|bronchitis|cystitis|streptococcal).*'
  AND NOT c.description =~ '.*(dental|tooth|gingiv|caries).*'
  AND NOT c.description =~ '.*(social isolation|limited social contact|employment|violence|criminal|education|housing|transport|activity|labor force|received.*education).*'
  AND NOT c.description =~ '.*(medication review|normal pregnancy).*'
SET c:ChronicDisease
RETURN count(c) as chronic_disease_count;

// ----------------------------------------------------------------------------
// SCHRITT 2: Kategorisierung - SOZIALE DETERMINANTEN
// ----------------------------------------------------------------------------
// Zielgruppe: Soziale, ökonomische, umweltbedingte Risikofaktoren
// Kriterien: Employment, Education, Housing, Violence, Isolation, etc.
// ABER NICHT: Klinische Befunde (obesity, prediabetes bereits oben)

MATCH (c:Condition)
WHERE (c.description =~ '.*(social isolation|limited social contact|stress|violence|criminal|education|housing|transport).*'
   OR c.description =~ '.*(received.*education|labor force|unemployed|part-time|full-time|employment).*'
   OR c.description =~ '.*(victim|abuse|reports of violence).*'
   OR c.description =~ '.*(risk activity|unhealthy.*behavior|has a criminal).*'
   OR c.description =~ '.*(normal pregnancy).*')
  AND NOT c.description =~ '.*(obesity|obese|prediabetes|chronic pain|chronic low back|body mass index 30|body mass index 40).*'
  AND NOT c.description =~ '.*(disorder|disease|syndrome).*'
  AND NOT (c:ChronicDisease)
SET c:SocialDeterminant
RETURN count(c) as social_determinant_count;

// ----------------------------------------------------------------------------
// SCHRITT 3: Kategorisierung - AKUTE ERKRANKUNGEN
// ----------------------------------------------------------------------------

MATCH (c:Condition)
WHERE c.description =~ '.*(viral|acute|infection|sinusitis|pharyngitis|bronchitis|cystitis|streptococcal).*'
  AND NOT c.description =~ '.*(chronic).*'
SET c:AcuteCondition
RETURN count(c) as acute_condition_count;

// ----------------------------------------------------------------------------
// SCHRITT 4: Kategorisierung - ZAHNERKRANKUNGEN
// ----------------------------------------------------------------------------

MATCH (c:Condition)
WHERE c.description =~ '.*(dental|tooth|gingiv|caries).*'
SET c:DentalCondition
RETURN count(c) as dental_condition_count;

// ----------------------------------------------------------------------------
// SCHRITT 5: Kategorisierung - ADMINISTRATIVE EINTRÄGE
// ----------------------------------------------------------------------------

MATCH (c:Condition)
WHERE c.description =~ '.*(medication review|situation).*'
  AND NOT (c:ChronicDisease OR c:SocialDeterminant OR c:AcuteCondition OR c:DentalCondition)
SET c:Administrative
RETURN count(c) as administrative_count;

// ----------------------------------------------------------------------------
// SCHRITT 6: Validierung der Kategorisierung
// ----------------------------------------------------------------------------

// Überblick über Kategorisierung
MATCH (c:Condition)
RETURN 
  count(c) as total_conditions,
  sum(CASE WHEN c:ChronicDisease THEN 1 ELSE 0 END) as chronic,
  sum(CASE WHEN c:SocialDeterminant THEN 1 ELSE 0 END) as social,
  sum(CASE WHEN c:AcuteCondition THEN 1 ELSE 0 END) as acute,
  sum(CASE WHEN c:DentalCondition THEN 1 ELSE 0 END) as dental,
  sum(CASE WHEN c:Administrative THEN 1 ELSE 0 END) as administrative,
  sum(CASE WHEN NOT (c:ChronicDisease OR c:SocialDeterminant OR c:AcuteCondition OR c:DentalCondition OR c:Administrative) THEN 1 ELSE 0 END) as uncategorized;

// Patienten mit ≥2 chronischen Erkrankungen (Zielgruppe für Komorbiditätsanalyse)
MATCH (p:Patient)-[:HAS_CONDITION]->(c:ChronicDisease)
WITH p, count(DISTINCT c) as chronic_count
WHERE chronic_count >= 2
RETURN 
  count(p) as patients_with_comorbidities,
  round(avg(chronic_count), 1) as avg_chronic_diseases,
  max(chronic_count) as max_chronic_diseases;

// ----------------------------------------------------------------------------
// SCHRITT 7: Properties für GDS hinzufügen
// ----------------------------------------------------------------------------

// Prävalenz-Property (Anzahl Patienten mit dieser Erkrankung)
MATCH (c:Condition)
MATCH (c)<-[:HAS_CONDITION]-(p:Patient)
WITH c, count(DISTINCT p) as patient_count
SET c.prevalence = patient_count
RETURN labels(c) as category, count(c) as condition_count, round(avg(patient_count), 1) as avg_prevalence;

// Durchschnittliches Diagnosealter (für chronische Erkrankungen)
MATCH (p:Patient)-[r:HAS_CONDITION]->(c:ChronicDisease)
WHERE p.birthDate IS NOT NULL
WITH c, p, duration.between(date(p.birthDate), date(coalesce(r.onset, date()))).years as age_at_diagnosis
WITH c, avg(age_at_diagnosis) as avg_age
SET c.avgAgeAtDiagnosis = round(avg_age, 1)
RETURN count(c) as conditions_with_age_data;

// ============================================================================
// KATEGORISIERUNG ABGESCHLOSSEN
// ============================================================================
