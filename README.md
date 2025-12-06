# Masterseminar – Neo4j Demos

Dieses Repository (`masterseminar`) enthält zwei voneinander unabhängige Neo4j-Demos:

* `neo4j_demo_sozial` – einfache Social-Network-Demo zum Einstieg in Neo4j & Cypher
* `neo4j_demo_synthea` – erweiterte Demo mit synthetischen Gesundheitsdaten (Synthea) und Graph Data Science

Die Demos dienen ausschließlich Lehr- und Demonstrationszwecken.

---

## Getestete Umgebungen

Getestet unter:

* Windows 10 / Windows 11
* Ubuntu Linux

Unter macOS wurde nicht praktisch getestet. Da nur Docker, Python und Java verwendet werden, ist ein Betrieb grundsätzlich möglich, aber **ohne Gewähr**.

---

## Voraussetzungen (Kurzfassung)

Allgemein:

* Git oder ZIP-Download dieses Repositories
* Aktuelle Version von Docker / Docker Desktop (inkl. `docker compose`)

Zusätzlich für `neo4j_demo_synthea`:

* Python 3 (z. B. 3.10 oder 3.11)
* Java JDK 11, 17 oder 21 (Versionen > 21 sind für Synthea nicht geeignet)
* Ausreichend **freier** RAM (Demo ist speicherintensiv, v. a. bei vielen Patienten)

---

## Quickstart: Social-Demo (`neo4j_demo_sozial`)

Einstieg in Neo4j mit einem kleinen Social-Network-Datensatz und Übungsaufgaben.

```bash
cd neo4j_demo_sozial
docker compose up -d
```

Neo4j Browser:

* URL: [http://localhost:7474](http://localhost:7474)
* User: `neo4j`
* Passwort (Demo): `password`

Stoppen:

```bash
docker compose down
```

---

## Quickstart: Synthea-Demo (`neo4j_demo_synthea`)

Showcase mit synthetischen Patientendaten (Synthea), ETL-Pipeline, Neo4j GDS und Jupyter-Notebooks.

### 1. Setup-Skript ausführen

> Unter Windows meist `python`, unter Linux/macOS meist `python3`.

```bash
cd neo4j_demo_synthea

# Beispiel: kleine Testkonfiguration
python3 setup.py --patients 25 --ram 4 --clean-start
```

Das Skript:

* prüft Docker / Java / Python,
* generiert optional Synthea-Daten,
* startet Neo4j und führt den ETL-Import aus,
* startet optional Jupyter-Notebooks.

### 2. Zugriff auf Neo4j und Jupyter

Neo4j:

* URL: [http://localhost:7475](http://localhost:7475)
* Bolt-Port: `7688`
* User: `neo4j`
* Passwort (Demo): `synthea123`

Jupyter-Notebooks:

* URL: [http://localhost:8889](http://localhost:8889)
* Token: `synthea`

Notebooks nach Möglichkeit immer komplett per „Restart Kernel and Run All“ ausführen (oben werden Bibliotheken geladen und die Neo4j-Verbindung aufgebaut).

### 3. Start/Stop

Stoppen:

```bash
docker compose down
```

Erneutes Starten **ohne** Neu-Setup (nach erfolgreichem Setup):

```bash
docker compose up -d
```

Für geänderte Konfiguration (z. B. andere Patientenzahl) `setup.py` erneut mit `--clean-start` ausführen.

---

## Hinweise

* Die Synthea-Demo benötigt deutlich mehr Ressourcen (RAM, Laufzeit) als die Social-Demo.
* Typische Probleme bei zu wenig Speicher:

  * Docker Exit Code `137`
  * Neo4j `MemoryPoolOutOfMemoryError`
    → Patientenzahl reduzieren, andere Anwendungen schließen, ggf. Docker-Memory-Limit erhöhen.
* Weitere Details (Architektur, Parameterliste, bekannte Probleme) sind in der schriftlichen Ausarbeitung zum Masterseminar dokumentiert.
