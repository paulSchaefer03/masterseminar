// ============================================================================
// Social Network Demo - Setup Script
// Eine einfache Demonstration von Graphdatenbanken mit Neo4j
// ============================================================================

// --- RESET (nur für Demo!) ---
MATCH (n) DETACH DELETE n;

// --- 1. Personen laden ---
LOAD CSV WITH HEADERS FROM 'file:///social-graph/people.csv' AS row
WITH trim(row.name) AS name, 
     toInteger(row.age) AS age,
     trim(row.city) AS city
WHERE name <> ''
MERGE (p:Person {name: name})
SET p.age = age,
    p.city = city;

// --- 2. Firmen laden ---
LOAD CSV WITH HEADERS FROM 'file:///social-graph/companies.csv' AS row
WITH trim(row.name) AS name,
     trim(row.city) AS city,
     trim(row.industry) AS industry,
     toInteger(row.founded) AS founded
WHERE name <> ''
MERGE (c:Company {name: name})
SET c.city = city,
    c.industry = industry,
    c.founded = founded;

// --- 3. KNOWS Beziehungen laden ---
LOAD CSV WITH HEADERS FROM 'file:///social-graph/knows.csv' AS row
MATCH (a:Person {name: trim(row.src)}),
      (b:Person {name: trim(row.dst)})
MERGE (a)-[r:KNOWS]->(b)
SET r.since = toInteger(row.since);

// --- 4. WORKS_AT Beziehungen laden ---
LOAD CSV WITH HEADERS FROM 'file:///social-graph/works_at.csv' AS row
MATCH (p:Person {name: trim(row.person)}),
      (c:Company {name: trim(row.company)})
MERGE (p)-[w:WORKS_AT]->(c)
SET w.position = trim(row.position),
    w.since = toInteger(row.since),
    w.salary = toInteger(row.salary);

// --- 5. Constraints und Indizes ---
CREATE CONSTRAINT person_name IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS UNIQUE;
CREATE CONSTRAINT company_name IF NOT EXISTS FOR (c:Company) REQUIRE c.name IS UNIQUE;
CREATE INDEX person_city IF NOT EXISTS FOR (p:Person) ON (p.city);
CREATE INDEX company_city IF NOT EXISTS FOR (c:Company) ON (c.city);
CREATE INDEX company_industry IF NOT EXISTS FOR (c:Company) ON (c.industry);

// ============================================================================
// Validation Queries
// ============================================================================

// Wie viele Personen und Firmen haben wir?
MATCH (n:Person) RETURN count(n) AS total_persons;
MATCH (n:Company) RETURN count(n) AS total_companies;

// Wie viele Beziehungen?
MATCH (:Person)-[r:KNOWS]->(:Person) RETURN count(r) AS total_knows;
MATCH (:Person)-[r:WORKS_AT]->(:Company) RETURN count(r) AS total_works_at;

// Wer kennt wen?
MATCH (p:Person)-[r:KNOWS]->(f:Person) 
RETURN p.name AS person, 
       collect(f.name) AS friends,
       count(f) AS friend_count
ORDER BY friend_count DESC;

// Wer arbeitet wo?
MATCH (p:Person)-[w:WORKS_AT]->(c:Company)
RETURN p.name AS person, 
       c.name AS company,
       w.position AS position,
       w.since AS since
ORDER BY c.name, p.name;

// ============================================================================
// Hilfestellungen - Beispiel-Queries für Übungen
// Diese Queries zeigen die Syntax und sind NICHT Teil der Übungsaufgaben
// ============================================================================

// --- MATCH Pattern Beispiele ---

// Einfaches MATCH (ein Node-Type)
MATCH (p:Person)
RETURN p.name, p.age, p.city;

// MATCH mit Property-Filter im Pattern
MATCH (alice:Person {name: 'Alice'})
RETURN alice.name, alice.city;

// MATCH mit Relationship
MATCH (p:Person)-[:KNOWS]->(friend:Person)
RETURN p.name, friend.name;

// MATCH in beide Richtungen (ohne Pfeil)
MATCH (p:Person)-[:KNOWS]-(friend:Person)
RETURN p.name, friend.name;

// --- WHERE Clause Beispiele ---

// Einfacher Vergleich
MATCH (p:Person)
WHERE p.age > 30
RETURN p.name, p.age;

// String Vergleich
MATCH (p:Person)
WHERE p.city = 'Berlin'
RETURN p.name;

// Mehrere Bedingungen mit AND
MATCH (p:Person)
WHERE p.age > 25 AND p.city = 'Berlin'
RETURN p.name, p.age;

// Node-Vergleich (Selbstausschluss)
MATCH (p1:Person)-[:KNOWS]-(p2:Person)
WHERE p1 <> p2
RETURN p1.name, p2.name;

// --- Aggregations-Funktionen ---

// COUNT - Anzahl zählen
MATCH (p:Person)
RETURN count(p) AS anzahl_personen;

// AVG - Durchschnitt berechnen
MATCH (p:Person)-[w:WORKS_AT]->()
RETURN avg(w.salary) AS durchschnittsgehalt;

// Gruppierung mit WITH
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
WITH c, count(p) AS mitarbeiter
RETURN c.name, mitarbeiter;

// COLLECT - Werte sammeln
MATCH (p:Person)
RETURN p.city, collect(p.name) AS personen;

// --- Komplexe Patterns ---

// Zwei MATCH Statements kombinieren
MATCH (p1:Person)-[:KNOWS]-(p2:Person)
MATCH (p1)-[:WORKS_AT]->(c:Company)
RETURN p1.name, p2.name, c.name;

// Pattern mit gemeinsamer Node
MATCH (p1:Person)-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(p2:Person)
WHERE p1 <> p2
RETURN p1.name, p2.name, c.name;

// Variable Pfadlänge
MATCH (p:Person)-[:KNOWS*2]-(fof:Person)
RETURN p.name, fof.name;

// Kürzester Pfad
MATCH path = shortestPath(
  (p1:Person {name: 'Alice'})-[:KNOWS*]-(p2:Person {name: 'Henry'})
)
RETURN [n in nodes(path) | n.name] AS pfad;

// --- OPTIONAL MATCH ---

// Wenn Beziehung nicht existiert, trotzdem Node zurückgeben
MATCH (c:Company)
OPTIONAL MATCH (p:Person)-[:WORKS_AT]->(c)
RETURN c.name, count(p) AS mitarbeiter;

// --- Berechnungen ---

// Mathematische Operationen
MATCH (p:Person)-[w:WORKS_AT]->(c:Company)
WITH p, c, w, (2025 - w.since) AS jahre
RETURN p.name, jahre, w.salary / jahre AS gehalt_pro_jahr;

// --- Sortierung und Limitierung ---

// ORDER BY
MATCH (p:Person)
RETURN p.name, p.age
ORDER BY p.age DESC;

// LIMIT
MATCH (p:Person)
RETURN p.name
LIMIT 3;

// --- DISTINCT ---

// Duplikate vermeiden
MATCH (p:Person)-[:KNOWS*2]-(fof:Person)
RETURN DISTINCT fof.name;

// ============================================================================
// Visualisierung
// ============================================================================

// Gesamtes Netzwerk
MATCH path = (n)-[r]-(m)
RETURN path
LIMIT 50;

// Nur Personen und KNOWS-Beziehungen
MATCH path = (p:Person)-[:KNOWS]-(friend:Person)
RETURN path;

// Firmen mit Mitarbeitern
MATCH path = (c:Company)<-[:WORKS_AT]-(p:Person)
RETURN path;
