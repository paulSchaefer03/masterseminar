"""
Patient data loader
Loads patient demographics from Synthea patients.csv
"""

from .base import Neo4jConnection, IMPORT_DIR, clean_dataframe
import pandas as pd

def load_patients(connection=None):
    """Load patient demographic data"""
    print("\n" + "=" * 60)
    print("Loading Patients")
    print("=" * 60)
    
    close_connection = False
    if connection is None:
        connection = Neo4jConnection()
        close_connection = True
    
    try:
        # Read data
        df = pd.read_csv(f"{IMPORT_DIR}/patients.csv")
        df = clean_dataframe(df)
        print(f"Found {len(df)} patients in CSV")
        
        # Load patients
        total = 0
        errors = 0
        
        with connection.driver.session() as session:
            for _, row in df.iterrows():
                try:
                    session.run("""
                        CREATE (p:Patient {
                            patient_id: $id,
                            birthDate: date($birthDate),
                            deathDate: CASE WHEN $deathDate IS NOT NULL THEN date($deathDate) ELSE null END,
                            ssn: $ssn,
                            drivers: $drivers,
                            passport: $passport,
                            prefix: $prefix,
                            given: $first,
                            family: $last,
                            suffix: $suffix,
                            maiden: $maiden,
                            maritalStatus: $marital,
                            race: $race,
                            ethnicity: $ethnicity,
                            gender: $gender,
                            birthPlace: $birthPlace,
                            address: $address,
                            city: $city,
                            state: $state,
                            county: $county,
                            fips: $fips,
                            zip: $zip,
                            lat: $lat,
                            lon: $lon,
                            income: $income,
                            healthcareExpenses: $expenses,
                            healthcareCoverage: $coverage,
                            qaly: $qaly,
                            daly: $daly
                        })
                    """, 
                    id=row['Id'], 
                    birthDate=row['BIRTHDATE'], 
                    deathDate=row.get('DEATHDATE'),
                    ssn=row.get('SSN'),
                    drivers=row.get('DRIVERS'),
                    passport=row.get('PASSPORT'),
                    prefix=row.get('PREFIX'),
                    first=row.get('FIRST'), 
                    last=row.get('LAST'),
                    suffix=row.get('SUFFIX'),
                    maiden=row.get('MAIDEN'),
                    marital=row.get('MARITAL'),
                    race=row.get('RACE'), 
                    ethnicity=row.get('ETHNICITY'),
                    gender=row.get('GENDER'),
                    birthPlace=row.get('BIRTHPLACE'),
                    address=row.get('ADDRESS'),
                    city=row.get('CITY'),
                    state=row.get('STATE'),
                    county=row.get('COUNTY'),
                    fips=row.get('FIPS'),
                    zip=row.get('ZIP'),
                    lat=row.get('LAT'),
                    lon=row.get('LON'),
                    income=row.get('INCOME'),
                    expenses=row.get('HEALTHCARE_EXPENSES'),
                    coverage=row.get('HEALTHCARE_COVERAGE'),
                    qaly=row.get('QALY'),
                    daly=row.get('DALY')
                    )
                    total += 1
                except Exception as e:
                    errors += 1
                    if errors <= 5:  # Only print first 5 errors
                        print(f"Error loading patient: {e}")
        
        print(f"✓ Loaded {total} patients ({errors} errors)")
        return total
        
    finally:
        if close_connection:
            connection.close()

if __name__ == "__main__":
    # Can be run standalone
    load_patients()
