import csv, os
from neo4j import GraphDatabase

URI  = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASS = os.getenv("NEO4J_PASS", "password")

PEOPLE = "/import/social-graph/people.csv"
KNOWS  = "/import/social-graph/knows.csv"
COMPANIES = "/import/social-graph/companies.csv"
WORKS_AT = "/import/social-graph/works_at.csv"

def run(tx, query, **params):
    tx.run(query, **params)

def main():
    driver = GraphDatabase.driver(URI, auth=(USER, PASS))
    with driver.session() as s:
        # Reset (Demo!)
        s.execute_write(run, "MATCH (n) DETACH DELETE n")

        # Nodes
        with open(PEOPLE, newline='', encoding='utf-8') as f:
            r = csv.DictReader(f)
            for row in r:
                name = (row.get("name") or "").strip()
                if not name: 
                    continue
                
                # Parse age and city
                age = None
                if row.get("age"):
                    try:
                        age = int(row["age"])
                    except ValueError:
                        pass
                
                city = (row.get("city") or "").strip() or None
                
                s.execute_write(
                    run,
                    """
                    MERGE (p:Person {name: $name})
                    SET p.age = $age, p.city = $city
                    """,
                    name=name, age=age, city=city
                )

        # Companies
        with open(COMPANIES, newline='', encoding='utf-8') as f:
            r = csv.DictReader(f)
            for row in r:
                name = (row.get("name") or "").strip()
                if not name:
                    continue
                
                city = (row.get("city") or "").strip() or None
                industry = (row.get("industry") or "").strip() or None
                founded = None
                if row.get("founded"):
                    try:
                        founded = int(row["founded"])
                    except ValueError:
                        pass
                
                s.execute_write(
                    run,
                    """
                    MERGE (c:Company {name: $name})
                    SET c.city = $city, c.industry = $industry, c.founded = $founded
                    """,
                    name=name, city=city, industry=industry, founded=founded
                )

        # KNOWS Relationships
        with open(KNOWS, newline='', encoding='utf-8') as f:
            r = csv.DictReader(f)
            for row in r:
                src = (row.get("src") or "").strip()
                dst = (row.get("dst") or "").strip()
                if not src or not dst:
                    continue
                # Parse since attribute
                since = None
                if row.get("since"):
                    try:
                        since = int(row["since"])
                    except ValueError:
                        pass
                
                s.execute_write(
                    run,
                    """
                    MATCH (a:Person {name: $src}), (b:Person {name: $dst})
                    MERGE (a)-[r:KNOWS]->(b)
                    SET r.since = $since
                    """,
                    src=src, dst=dst, since=since
                )

        # WORKS_AT Relationships
        with open(WORKS_AT, newline='', encoding='utf-8') as f:
            r = csv.DictReader(f)
            for row in r:
                person = (row.get("person") or "").strip()
                company = (row.get("company") or "").strip()
                if not person or not company:
                    continue
                
                position = (row.get("position") or "").strip() or None
                since = None
                salary = None
                
                if row.get("since"):
                    try:
                        since = int(row["since"])
                    except ValueError:
                        pass
                
                if row.get("salary"):
                    try:
                        salary = int(row["salary"])
                    except ValueError:
                        pass
                
                s.execute_write(
                    run,
                    """
                    MATCH (p:Person {name: $person}), (c:Company {name: $company})
                    MERGE (p)-[w:WORKS_AT]->(c)
                    SET w.position = $position, w.since = $since, w.salary = $salary
                    """,
                    person=person, company=company, position=position, since=since, salary=salary
                )

        # Quick sanity
        persons = s.run("MATCH (n:Person) RETURN count(n) AS c").single()["c"]
        companies = s.run("MATCH (n:Company) RETURN count(n) AS c").single()["c"]
        knows_rels = s.run("MATCH ()-[r:KNOWS]->() RETURN count(r) AS c").single()["c"]
        works_rels = s.run("MATCH ()-[r:WORKS_AT]->() RETURN count(r) AS c").single()["c"]
        print(f"Loaded persons={persons}, companies={companies}, knows={knows_rels}, works_at={works_rels}")

    driver.close()

if __name__ == "__main__":
    main()
