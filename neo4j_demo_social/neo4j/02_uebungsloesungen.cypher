// ============================================================================
// Social Network Demo - Übungslösungen
// Lösungen zu allen Aufgaben aus docs/UEBUNGSAUFGABEN.md
// ============================================================================

// ============================================================================
// Level 1: Basis-Queries (Einstieg)
// ============================================================================

// Aufgabe 1.1: Alle Personen anzeigen
MATCH (p:Person)
RETURN p.name, p.age, p.city;

// Aufgabe 1.2: Personen über 30
MATCH (p:Person)
WHERE p.age > 30
RETURN p.name, p.age
ORDER BY p.age DESC;

// Aufgabe 1.3: Alle Firmen in Berlin
MATCH (c:Company)
WHERE c.city = 'Berlin'
RETURN c.name, c.industry, c.founded;

// ============================================================================
// Level 2: Einfache Beziehungen
// ============================================================================

// Aufgabe 2.1: Freunde von Alice
MATCH (alice:Person {name: 'Alice'})-[:KNOWS]->(friend)
RETURN friend.name, friend.age, friend.city;

// Aufgabe 2.2: Wo arbeitet Bob?
MATCH (bob:Person {name: 'Bob'})-[w:WORKS_AT]->(c:Company)
RETURN c.name AS company, 
       c.industry, 
       w.position, 
       w.salary,
       w.since AS since_year;

// Aufgabe 2.3: Alle Mitarbeiter von TechCorp
MATCH (p:Person)-[w:WORKS_AT]->(c:Company {name: 'TechCorp'})
RETURN p.name, 
       w.position, 
       w.salary,
       w.since
ORDER BY w.salary DESC;

// ============================================================================
// Level 3: Aggregationen
// ============================================================================

// Aufgabe 3.1: Größte Firmen nach Mitarbeiterzahl
MATCH (c:Company)
OPTIONAL MATCH (p:Person)-[:WORKS_AT]->(c)
WITH c, count(p) AS employee_count
RETURN c.name AS company,
       c.city,
       c.industry,
       employee_count
ORDER BY employee_count DESC;

// Aufgabe 3.2: Durchschnittsgehalt pro Branche
MATCH (p:Person)-[w:WORKS_AT]->(c:Company)
RETURN c.industry AS industry,
       avg(w.salary) AS avg_salary,
       count(p) AS employees
ORDER BY avg_salary DESC;

// Aufgabe 3.3: Wer hat die meisten Freunde?
MATCH (p:Person)
OPTIONAL MATCH (p)-[:KNOWS]-(friend)
WITH p, count(DISTINCT friend) AS friend_count
RETURN p.name, 
       p.city, 
       friend_count
ORDER BY friend_count DESC;

// ============================================================================
// Level 4: Komplexe Patterns
// ============================================================================

// Aufgabe 4.1: Kollegen finden
MATCH (alice:Person {name: 'Alice'})-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(colleague:Person)
WHERE alice <> colleague
RETURN colleague.name AS colleague,
       colleague.age,
       c.name AS company;

// Aufgabe 4.2: Freunde, die auch Kollegen sind
MATCH (p1:Person)-[:KNOWS]-(p2:Person)
MATCH (p1)-[:WORKS_AT]->(c:Company)<-[:WORKS_AT]-(p2)
RETURN p1.name AS person1,
       p2.name AS person2,
       c.name AS company;

// Aufgabe 4.3: Freunde von Freunden (2. Grad)
MATCH (alice:Person {name: 'Alice'})-[:KNOWS*2]-(fof:Person)
WHERE fof <> alice
  AND NOT (alice)-[:KNOWS]-(fof)
RETURN DISTINCT fof.name AS friend_of_friend,
       fof.age,
       fof.city;

// Aufgabe 4.4: Kürzester Pfad zwischen zwei Personen
MATCH path = shortestPath(
  (alice:Person {name: 'Alice'})-[:KNOWS*]-(henry:Person {name: 'Henry'})
)
RETURN [n in nodes(path) | n.name] AS path,
       length(path) AS hops;

// ============================================================================
// Level 5: Business Intelligence
// ============================================================================

// Aufgabe 5.1: Netzwerk-Recruiting
MATCH path = (alice:Person {name: 'Alice'})-[:KNOWS*1..3]-(candidate:Person)
MATCH (candidate)-[w:WORKS_AT]->(c:Company)
WHERE w.position = 'Data Scientist'
  AND candidate <> alice
RETURN candidate.name AS candidate,
       c.name AS current_company,
       w.salary AS current_salary,
       length(path) AS connection_distance,
       [n in nodes(path) | n.name] AS path
ORDER BY connection_distance ASC;

// Aufgabe 5.2: Gehaltsgefälle analysieren
MATCH (p1:Person)-[w1:WORKS_AT]->(c:Company)<-[w2:WORKS_AT]-(p2:Person)
WHERE p1 <> p2 
  AND w1.salary > w2.salary + 10000
RETURN p1.name AS higher_paid,
       w1.salary AS salary1,
       p2.name AS lower_paid,
       w2.salary AS salary2,
       c.name AS company,
       (w1.salary - w2.salary) AS difference
ORDER BY difference DESC;

// Aufgabe 5.3: Expansion-Kandidaten
MATCH (p:Person)-[:WORKS_AT]->(tc:Company {name: 'TechCorp'})
WITH p.city AS employee_city, count(p) AS employee_count
WHERE employee_count >= 2
  AND NOT EXISTS {
    MATCH (c:Company {name: 'TechCorp'})
    WHERE c.city = employee_city
  }
RETURN employee_city AS potential_location,
       employee_count AS employees_living_there;

// Aufgabe 5.4: Karriere-Tracking
MATCH (p:Person)-[w:WORKS_AT]->(c:Company)
WITH p, c, w, (2025 - w.since) AS years_employed
WHERE years_employed >= 5
RETURN p.name AS person,
       c.name AS company,
       w.position,
       years_employed,
       w.salary,
       round(w.salary / years_employed) AS salary_per_year
ORDER BY salary_per_year DESC;

// Aufgabe 5.5: Network-Effect für Job-Wechsel
MATCH (alice:Person {name: 'Alice'})
MATCH (dm:Company {name: 'DataMinds'})
OPTIONAL MATCH (alice)-[:KNOWS]-(friend:Person)-[:WORKS_AT]->(dm)
WITH alice, dm, count(DISTINCT friend) AS friends_there
RETURN dm.name AS potential_company,
       dm.city AS company_location,
       alice.city AS current_location,
       dm.city = alice.city AS same_city,
       friends_there;

// ============================================================================
// Bonus-Aufgaben (Fortgeschritten)
// ============================================================================

// Bonus 1: Branchen-Netzwerk
MATCH (p1:Person)-[:WORKS_AT]->(c1:Company)
MATCH (p2:Person)-[:WORKS_AT]->(c2:Company)
MATCH (p1)-[:KNOWS]-(p2)
WHERE c1.industry < c2.industry  // Vermeidet Duplikate
WITH c1.industry AS industry1, 
     c2.industry AS industry2, 
     count(*) AS connections
RETURN industry1, industry2, connections
ORDER BY connections DESC;

// Bonus 2: Firmen-Altersverteilung
MATCH (p:Person)-[:WORKS_AT]->(c:Company)
RETURN c.name AS company,
       avg(p.age) AS avg_age,
       min(p.age) AS youngest,
       max(p.age) AS oldest,
       count(p) AS employees
ORDER BY avg_age DESC;

// Bonus 3: Startup vs. Established
MATCH (p:Person)-[w:WORKS_AT]->(c:Company)
WITH c,
     CASE 
       WHEN c.founded > 2015 THEN 'Startup'
       ELSE 'Established'
     END AS company_type,
     avg(w.salary) AS avg_salary,
     count(p) AS employees
RETURN company_type,
       count(c) AS company_count,
       avg(avg_salary) AS overall_avg_salary,
       avg(employees) AS avg_employees_per_company
ORDER BY company_type;
