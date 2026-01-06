# ELT proces – Austrálske nemocnice (Australia Healthcare Locations)

Tento repozitár predstavuje ukážkovú implementáciu ELT procesu v Snowflake a návrh dátového skladu so schémou hviezdy (Star Schema). Projekt pracuje s verejne dostupným datasetom o nemocniciach v Austrálii, ktorý obsahuje informácie o lokalite, type nemocnice, sektore, sieti, stave otvorenia a ďalších atribútoch.

Projekt je navrhnutý ako záverečný/študijný projekt, ktorého cieľom je demonštrovať:

* správny návrh dátovej architektúry,
* implementáciu ELT procesu v Snowflake,
* transformáciu dát z normalizovaného modelu do dimenzionálneho modelu,
* prípravu dát pre analytické dotazy a dashboardy.

---

## 1. Úvod a popis zdrojových dát

V tomto projekte analyzujeme dáta o nemocniciach v Austrálii. Hlavným cieľom analýzy je porozumieť:

* geografickému rozloženiu nemocníc,
* rozdielom medzi verejným a súkromným sektorom,
* hustote nemocníc v jednotlivých štátoch,
* štruktúre nemocničných sietí,
* dostupnosti zdravotnej starostlivosti podľa regiónov.

Zdrojové dáta pochádzajú z verejného datasetu AIHW – Australian Institute of Health and Welfare, ktorý je dostupný v Snowflake ako free dataset.

Dataset obsahuje najmä tieto typy údajov:

* identifikátory a názvy nemocníc,
* geografické údaje (štát, súradnice),
* typ a sektor nemocnice,
* príslušnosť k lokálnym a primárnym zdravotným sieťam,
* stav nemocnice (otvorená/zatvorená),
* metadáta o zdroji a aktualizácii dát.

Účelom ELT procesu bolo tieto dáta extrahovať, vyčistiť, normalizovať a následne transformovať do dimenzionálneho modelu vhodného na analytické spracovanie.

---

## 1.1 Dátová architektúra

### ERD diagram

Surové dáta boli najskôr spracované do normalizovaného relačného modelu, ktorý je znázornený na entitno-relačnom diagrame (ERD).

Miesto pre obrázok:

> Obrázok 1: Entitno-relačný diagram (ERD) – Normalizovaný model

Normalizovaný model obsahuje samostatné tabuľky pre:

* nemocnice,
* lokality,
* typy nemocníc,
* siete nemocníc,
* stav nemocníc.

Tento krok zabezpečuje konzistentnosť dát a eliminuje redundanciu.

---

## 2. Dimenzionálny model

Na analytické účely bol navrhnutý dimenzionálny model typu Star Schema podľa Kimballovej metodológie.

Model obsahuje jednu faktovú tabuľku fact_hospitals, ktorá je prepojená s nasledujúcimi dimenziami:

* dim_hospital – základné informácie o nemocniciach (kód, názov, zdroj dát),
* dim_location – geografické údaje (štát, súradnice),
* dim_type – typ a sektor nemocnice (verejná/súkromná),
* dim_network – informácie o zdravotných sieťach,
* dim_status – stav nemocnice (otvorená/zatvorená).

Faktová tabuľka obsahuje metriky a odvodené ukazovatele, napríklad:

* počet nemocníc,
* poradie nemocnice v rámci štátu,
* celkový počet nemocníc v danom štáte.

Miesto pre obrázok:

> Obrázok 2: Schéma hviezdy (Star Schema) – Dimenzionálny model

---

## 3. ELT proces v Snowflake

ELT proces pozostáva z troch hlavných fáz:

1. Extract (Extrahovanie)
2. Load (Načítanie)
3. Transform (Transformácia)

Celý proces bol implementovaný priamo v Snowflake pomocou SQL skriptov.

---

## 3.1 Extract (Extrahovanie dát)

Dáta boli extrahované z verejného Snowflake datasetu HEALTHCARE__LOCATIONS__STATISTICS__AUSTRALIA__FREE. Tento dataset poskytuje aktuálne a štruktúrované informácie o nemocniciach v Austrálii.

Zdrojové dáta boli skopírované do staging schémy, ktorá slúži ako dočasná vrstva na ďalšie spracovanie.

```sql
CREATE DATABASE IF NOT EXISTS PIGEON_DOLPHIN_PROJECT_DB;
CREATE SCHEMA IF NOT EXISTS PIGEON_DOLPHIN_PROJECT_DB.STAGING;

CREATE OR REPLACE TABLE PIGEON_DOLPHIN_PROJECT_DB.STAGING.hospitals_staging AS
SELECT *
FROM HEALTHCARE__LOCATIONS__STATISTICS__AUSTRALIA__FREE.HEALTHCARE_AUS_FREE.AIHW_HOSPITAL_MAPPING;

```

---

## 3.2 Load (Načítanie dát)

V tejto fáze boli dáta načítané do staging tabuliek v databáze projektu. Staging vrstva obsahuje surové dáta bez zásadných transformácií.

Táto vrstva slúži ako:

* ochrana pôvodných dát,
* miesto na kontrolu kvality dát,
* východisko pre ďalšie transformácie.


---

## 3.3 Transform (Transformácia dát)

Transformačná fáza pozostáva z dvoch hlavných krokov:

### 3.3.1 Normalizácia dát

Zo staging tabuliek bol vytvorený normalizovaný model, ktorý rozdeľuje dáta do logických entít (location, type, network, status).

Príklad kódu:
```sql
select * from location;
CREATE OR REPLACE TABLE network AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY LOCAL_HOSPITAL_NETWORK) AS network_id,
    LOCAL_HOSPITAL_NETWORK,
    PRIMARY_HEALTH_NETWORK_AREA
FROM (SELECT DISTINCT LOCAL_HOSPITAL_NETWORK, PRIMARY_HEALTH_NETWORK_AREA FROM STAGING.hospitals_staging);
```

Cieľom bolo:

* odstránenie redundancie,
* zabezpečenie referenčnej integrity,
* príprava dát na dimenzionálne modelovanie.

### 3.3.2 Tvorba dimenzií a faktovej tabuľky

Z normalizovaného modelu bol následne vytvorený dimenzionálny model:

* dimenzie sú typu SCD Type 0, keďže atribúty sú považované za nemenné,
Príklad kódu:
```sql
CREATE TABLE dim_hospital AS 
SELECT hospital_ID AS dim_hospital_id, code AS hospital_code, name AS hospital_name, data_source, data_supplier FROM NORMALIZED.hospital;

CREATE OR REPLACE TABLE dim_location AS 
SELECT location_id AS dim_location_id, state, longitude, latitude FROM NORMALIZED.location;

CREATE OR REPLACE TABLE dim_type AS 
SELECT type_id AS dim_type_id, type, sector FROM NORMALIZED.hospital_type;

CREATE OR REPLACE TABLE dim_network AS 
SELECT network_id AS dim_networ_id, local_hospital_network, primary_health_network_area FROM NORMALIZED.network;

CREATE OR REPLACE TABLE dim_status AS 
SELECT status_id AS dim_status_id, open_closed, open_closed_bool FROM NORMALIZED.status;

```

* faktová tabuľka obsahuje odvodené analytické metriky.
Príklad kódu:
```sql
CREATE OR REPLACE TABLE fact_hospitals AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY h.hospital_ID) AS fact_id, 
    h.hospital_ID AS dim_hospital_id,
    h.hospital_type_type_id AS dim_type_id,
    h.location_location_id AS dim_location_id,
    h.network_network_id AS dim_network_id,
    h.status_status_id AS dim_status_id,
    1 AS hospital_count, 
    RANK() OVER (PARTITION BY l.state ORDER BY h.name) AS rank_in_state,
    COUNT(*) OVER (PARTITION BY l.state) AS total_hospitals_in_state
FROM NORMALIZED.hospital h
LEFT JOIN NORMALIZED.location l ON h.location_location_id = l.location_id;
```

Faktová tabuľka fact_hospitals prepája všetky dimenzie a umožňuje jednoduchú analytickú agregáciu.

Po úspešnom vytvorení finálneho modelu boli staging tabuľky odstránené z dôvodu optimalizácie úložiska.
Príklad kódu:
```sql
DROP TABLE IF EXISTS hospital; 
DROP TABLE IF EXISTS hospital_type;
DROP TABLE IF EXISTS network; 
DROP TABLE IF EXISTS location; 
DROP TABLE IF EXISTS status;
```
---

## 4. Vizualizácia dát

Dashboard obsahuje viacero vizualizácií, ktoré poskytujú prehľad o rozložení a štruktúre nemocníc v Austrálii.

![](https://github.com/1Parad0x/database_project-dsrg/blob/main/img/hospitals_dashboard1.png)
![](https://github.com/1Parad0x/database_project-dsrg/blob/main/img/hospitals_dashboard2.png)
![](https://github.com/1Parad0x/database_project-dsrg/blob/main/img/hospitals_dashboard3.png)

### Graf 1: Rozdelenie nemocníc podľa sektora

Graf ukazuje pomer medzi verejným a súkromným zdravotníckym sektorom.
```sql
SELECT 
    t.sector,
    SUM(f.hospital_count) as count
FROM fact_hospitals f
JOIN dim_type t ON f.dim_type_id = t.dim_type_id
WHERE t.sector IS NOT NULL
GROUP BY t.sector;
```
### Graf 2: Počet nemocníc podľa štátov

Vizualizácia zobrazuje celkový počet nemocníc v jednotlivých austrálskych štátoch. Umožňuje identifikovať regióny s najväčšou hustotou zdravotníckych zariadení, pričom najviac nemocníc sa pravdepodobne nachádza v ľudnatejších štátoch
```sql
SELECT 
    l.state,
    SUM(f.hospital_count) as total_hospitals
FROM fact_hospitals f
JOIN dim_location l ON f.dim_location_id = l.dim_location_id
GROUP BY l.state
ORDER BY total_hospitals DESC;
```
### Graf 3: Stav nemocníc Otvorené vs Zatvorené

Graf zobrazuje podiel aktívnych (Open) a zrušených (Closed) nemocníc.
```sql
SELECT 
    s.open_closed,
    SUM(f.hospital_count) as count
FROM fact_hospitals f
JOIN dim_status s ON f.dim_status_id = s.dim_status_id
GROUP BY s.open_closed;
```
### Graf 4: Najväčšie siete nemocníc

Vizualizácia predstavuje rebríček najväčších sietí lokálnych nemocníc.
```sql
SELECT 
    n.local_hospital_network,
    SUM(f.hospital_count) as facilities_count
FROM fact_hospitals f
JOIN dim_network n ON f.dim_network_id = n.dim_networ_id
WHERE n.local_hospital_network IS NOT NULL
GROUP BY n.local_hospital_network
ORDER BY facilities_count DESC
LIMIT 10;
```
### Graf 5: Distribúcia nemocníc podľa PHN

Vizualizácia ukazuje rozloženie nemocníc podľa oblastí "Primary Health Network". Tieto oblasti sú kľúčové pre koordináciu zdravotnej starostlivosti a graf ukazuje, ktoré z nich sú najviac vybavené lôžkovými zariadeniami.

```sql
SELECT 
    n.primary_health_network_area,
    SUM(f.hospital_count) as count
FROM fact_hospitals f
JOIN dim_network n ON f.dim_network_id = n.dim_networ_id
WHERE n.primary_health_network_area IS NOT NULL
GROUP BY n.primary_health_network_area
ORDER BY count DESC;
```
### Graf 6: Prvé 3 nemocnice v každom štáte

 Tabuľka využíva predpočítanú window funkciu  na zobrazenie prvých troch nemocníc v každom štáte. Demonštruje schopnosť dátového modelu efektívne filtrovať a radiť dáta v rámci partícií.

```sql
SELECT 
    l.state,
    h.hospital_name,
    f.rank_in_state
FROM fact_hospitals f
JOIN dim_location l ON f.dim_location_id = l.dim_location_id
JOIN dim_hospital h ON f.dim_hospital_id = h.dim_hospital_id
WHERE f.rank_in_state <= 3
ORDER BY l.state, f.rank_in_state;
```
### Graf 7: SQL "Priemerná hustota siete"

Graf ukazuje priemerný počet nemocníc pripadajúcich na jednu nemocničnú sieť v danom štáte.
```sql
SELECT 
    l.state,
    COUNT(DISTINCT f.dim_hospital_id) / COUNT(DISTINCT f.dim_network_id) as avg_hospitals_per_network
FROM fact_hospitals f
JOIN dim_location l ON f.dim_location_id = l.dim_location_id
GROUP BY l.state
ORDER BY avg_hospitals_per_network DESC;
```

### Graf 8: Analýza trhovej koncentrácie nemocničných sietí

Analýza koncentrácie trhu. Graf rozdeľuje trh na "Top 10 Dominantných Sietí" a "Ostatné". Ukazuje, či je austrálske zdravotníctvo ovládané niekoľkými gigantmi, alebo či je trh fragmentovaný medzi stovky malých lokálnych sietí.

```sql
WITH NetworkCounts AS (
    SELECT 
        n.local_hospital_network,
        COUNT(*) as hospitals_count
    FROM fact_hospitals f
    JOIN dim_network n ON f.dim_network_id = n.dim_networ_id
    WHERE n.local_hospital_network IS NOT NULL
    GROUP BY n.local_hospital_network
)
SELECT 
    CASE 
        WHEN rank_val <= 10 THEN local_hospital_network 
        ELSE 'Other Small Networks' 
    END AS network_category,
    SUM(hospitals_count) AS total_facilities,
    ROUND(RATIO_TO_REPORT(SUM(hospitals_count)) OVER () * 100, 2) AS market_share_percent
FROM (
    SELECT 
        local_hospital_network, 
        hospitals_count,
        RANK() OVER (ORDER BY hospitals_count DESC) as rank_val
    FROM NetworkCounts
)
GROUP BY network_category
ORDER BY total_facilities DESC;
```

## Autori: Oleksandr Nevtrynis a Vladyslav Chornoivan
