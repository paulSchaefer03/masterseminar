"""Organizations, Providers, Encounters, Conditions, Observations modules"""

from .base import Neo4jConnection, IMPORT_DIR, clean_dataframe
import pandas as pd

def load_organizations(connection=None):
    print("\n" + "=" * 60)
    print("Loading Organizations")
    print("=" * 60)
    
    close_connection = connection is None
    if close_connection:
        connection = Neo4jConnection()
    
    try:
        df = pd.read_csv(f"{IMPORT_DIR}/organizations.csv")
        df = clean_dataframe(df)
        print(f"Found {len(df)} organizations")
        
        total = 0
        with connection.driver.session() as session:
            for _, row in df.iterrows():
                try:
                    session.run("""
                        CREATE (o:Organization {
                            organization_id: $id, name: $name, address: $address,
                            city: $city, state: $state, zip: $zip,
                            lat: $lat, lon: $lon, phone: $phone,
                            revenue: $revenue, utilization: $utilization
                        })
                    """, id=row['Id'], name=row['NAME'], address=row.get('ADDRESS'),
                    city=row.get('CITY'), state=row.get('STATE'), zip=row.get('ZIP'),
                    lat=row.get('LAT'), lon=row.get('LON'), phone=row.get('PHONE'),
                    revenue=row.get('REVENUE'), utilization=row.get('UTILIZATION'))
                    total += 1
                except Exception as e:
                    print(f"Error: {e}")
        print(f"✓ Loaded {total} organizations")
        return total
    finally:
        if close_connection:
            connection.close()

def load_providers(connection=None):
    print("\n" + "=" * 60)
    print("Loading Providers")
    print("=" * 60)
    
    close_connection = connection is None
    if close_connection:
        connection = Neo4jConnection()
    
    try:
        df = pd.read_csv(f"{IMPORT_DIR}/providers.csv")
        df = clean_dataframe(df)
        print(f"Found {len(df)} providers")
        
        total = 0
        with connection.driver.session() as session:
            for _, row in df.iterrows():
                try:
                    session.run("""
                        CREATE (prov:Provider {
                            provider_id: $id, name: $name, gender: $gender,
                            specialty: $specialty, address: $address,
                            city: $city, state: $state, zip: $zip,
                            lat: $lat, lon: $lon, utilization: $utilization
                        })
                    """, id=row['Id'], name=row['NAME'], gender=row.get('GENDER'),
                    specialty=row.get('SPECIALITY'), address=row.get('ADDRESS'),
                    city=row.get('CITY'), state=row.get('STATE'), zip=row.get('ZIP'),
                    lat=row.get('LAT'), lon=row.get('LON'), utilization=row.get('ENCOUNTERS'))
                    
                    # Link to organization
                    session.run("""
                        MATCH (prov:Provider {provider_id: $prov_id})
                        MATCH (org:Organization {organization_id: $org_id})
                        MERGE (prov)-[:WORKS_AT]->(org)
                    """, prov_id=row['Id'], org_id=row['ORGANIZATION'])
                    total += 1
                except Exception as e:
                    print(f"Error: {e}")
        print(f"✓ Loaded {total} providers")
        return total
    finally:
        if close_connection:
            connection.close()

def load_encounters(connection=None):
    print("\n" + "=" * 60)
    print("Loading Encounters")
    print("=" * 60)
