#!/usr/bin/env python3
"""
ETL Pipeline Runner - Plattformunabhängig
==========================================

Orchestriert den vollständigen ETL + Notebook Setup Prozess:
1. Installiert Python-Abhängigkeiten
2. Führt Synthea ETL aus (load_synthea_complete.py)
3. Führt Notebook Setup aus (setup_notebooks.py - Cypher Scripts)

Läuft auf Windows, Mac und Linux.
"""

import subprocess
import sys
import os
from pathlib import Path


def print_header(text: str, char: str = "="):
    """Druckt formatierte Überschrift"""
    line = char * 60
    print(f"\n{line}")
    print(f"🚀 {text}")
    print(f"{line}\n")


def print_step(step_num: int, text: str):
    """Druckt Schritt-Header"""
    print(f"\n{'=' * 60}")
    print(f"📦 Step {step_num}: {text}")
    print(f"{'=' * 60}\n")


def run_command(description: str, args: list, check: bool = True) -> bool:
    """
    Führt ein Kommando aus und gibt Status zurück.
    
    Args:
        description: Beschreibung des Kommandos
        args: Liste der Kommando-Argumente
        check: Ob bei Fehler abgebrochen werden soll
    
    Returns:
        True wenn erfolgreich, False bei Fehler
    """
    print(f"▶️  {description}")
    print(f"   Command: {' '.join(args)}")
    
    try:
        result = subprocess.run(
            args,
            check=check,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
        
        # Output anzeigen
        if result.stdout:
            for line in result.stdout.strip().split('\n'):
                print(f"   {line}")
        
        print(f"✅ {description} - SUCCESS\n")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {description} - FAILED")
        print(f"   Exit code: {e.returncode}")
        if e.stdout:
            print(f"   Output: {e.stdout}")
        
        if check:
            print(f"\n{'=' * 60}")
            print(f"❌ PIPELINE FAILED at step: {description}")
            print(f"{'=' * 60}\n")
            sys.exit(e.returncode)
        
        return False
    
    except Exception as e:
        print(f"\n❌ {description} - ERROR: {e}")
        if check:
            sys.exit(1)
        return False


def install_dependencies():
    """Installiert Python-Abhängigkeiten"""
    print_step(1, "Installing Python Dependencies")
    
    requirements_file = Path("/etl/requirements.txt")
    
    if not requirements_file.exists():
        print(f"⚠️  WARNING: {requirements_file} not found")
        print("   Skipping dependency installation")
        return True
    
    return run_command(
        "Install Python packages",
        [sys.executable, "-m", "pip", "install", "--no-cache-dir", "-r", str(requirements_file)],
        check=True
    )


def run_synthea_etl():
    """Führt Synthea ETL aus"""
    print_step(2, "Running Synthea ETL (Load Data)")
    
    etl_script = Path("/etl/load_synthea_complete.py")
    
    if not etl_script.exists():
        print(f"❌ ERROR: {etl_script} not found")
        sys.exit(1)
    
    return run_command(
        "Execute Synthea ETL",
        [sys.executable, str(etl_script)],
        check=True
    )


def run_notebook_setup():
    """Führt Notebook Setup (Cypher Scripts) aus"""
    print_step(3, "Running Notebook Setup (Cypher Scripts)")
    
    setup_script = Path("/etl/setup_notebooks.py")
    
    if not setup_script.exists():
        print(f"⚠️  WARNING: {setup_script} not found")
        print("   Skipping notebook setup")
        print("   ⚠️  Notebooks may not work without manual Cypher execution!")
        return False
    
    # Nicht mit check=True, damit Pipeline auch ohne Setup weiterläuft
    success = run_command(
        "Execute Notebook Setup",
        [sys.executable, str(setup_script)],
        check=False
    )
    
    if not success:
        print("\n⚠️  WARNING: Notebook setup had issues")
        print("   You may need to run Cypher scripts manually:")
        print("   1. Open Neo4j Browser: http://localhost:7475")
        print("   2. Execute neo4j/02_categorize_conditions.cypher")
        print("   3. Execute neo4j/03_comorbidity_analysis.cypher")
    
    return success


def print_summary(etl_success: bool, setup_success: bool):
    """Druckt Zusammenfassung"""
    print_header("Pipeline Summary", "=")
    
    print("Status:")
    print(f"  {'✅' if etl_success else '❌'} ETL (Synthea Data Import)")
    print(f"  {'✅' if setup_success else '⚠️ '} Notebook Setup (Cypher Scripts)")
    
    print("\nNext Steps:")
    
    if etl_success and setup_success:
        print("  ✅ All setup complete!")
        print("  🎉 Open Jupyter Notebooks: http://localhost:8889")
        print("  🎉 Open Neo4j Browser: http://localhost:7475")
        print()
    
    elif etl_success and not setup_success:
        print("  ⚠️  ETL completed, but notebook setup had issues")
        print("  📝 You may need to run Cypher scripts manually")
        print("  🔗 See docs/CYPHER_FILES_REVIEW.md for details")
        print()
    
    else:
        print("  ❌ Pipeline failed - check logs above")
        print()


def main():
    """Hauptfunktion"""
    print_header("Synthea ETL + Notebook Setup Pipeline")
    
    print("Platform Information:")
    print(f"  Python: {sys.version}")
    print(f"  OS: {sys.platform}")
    print(f"  Working Directory: {os.getcwd()}")
    print()
    
    # Step 1: Dependencies
    install_dependencies()
    
    # Step 2: ETL
    etl_success = run_synthea_etl()
    
    # Step 3: Notebook Setup (nur wenn ETL erfolgreich)
    setup_success = False
    if etl_success:
        setup_success = run_notebook_setup()
    
    # Summary
    print_summary(etl_success, setup_success)
    
    # Exit Code
    if etl_success:
        sys.exit(0)  # ETL ist wichtiger
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
