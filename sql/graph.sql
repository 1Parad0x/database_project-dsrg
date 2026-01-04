-- GRAPH 1 SQL "Rozdelenie nemocníc podľa sektora"
SELECT 
    t.sector,
    SUM(f.hospital_count) as count
FROM fact_hospitals f
JOIN dim_type t ON f.dim_type_id = t.dim_type_id
WHERE t.sector IS NOT NULL
GROUP BY t.sector;
-- GRAPH 2 SQL "Počet nemocníc podľa štátov"
SELECT 
    l.state,
    SUM(f.hospital_count) as total_hospitals
FROM fact_hospitals f
JOIN dim_location l ON f.dim_location_id = l.dim_location_id
GROUP BY l.state
ORDER BY total_hospitals DESC;
-- GRAPH 3 SQL "Stav nemocníc Otvorené vs Zatvorené"
SELECT 
    s.open_closed,
    SUM(f.hospital_count) as count
FROM fact_hospitals f
JOIN dim_status s ON f.dim_status_id = s.dim_status_id
GROUP BY s.open_closed;
-- GRAPH 4 SQL "Najväčšie siete nemocníc"
SELECT 
    n.local_hospital_network,
    SUM(f.hospital_count) as facilities_count
FROM fact_hospitals f
JOIN dim_network n ON f.dim_network_id = n.dim_networ_id
WHERE n.local_hospital_network IS NOT NULL
GROUP BY n.local_hospital_network
ORDER BY facilities_count DESC
LIMIT 10;
-- GRAPH 5 SQL "Distribúcia nemocníc podľa PHN"
SELECT 
    n.primary_health_network_area,
    SUM(f.hospital_count) as count
FROM fact_hospitals f
JOIN dim_network n ON f.dim_network_id = n.dim_networ_id
WHERE n.primary_health_network_area IS NOT NULL
GROUP BY n.primary_health_network_area
ORDER BY count DESC;
-- GRAPH 6 SQL "Prvé 3 nemocnice v každom štáte"
SELECT 
    l.state,
    h.hospital_name,
    f.rank_in_state
FROM fact_hospitals f
JOIN dim_location l ON f.dim_location_id = l.dim_location_id
JOIN dim_hospital h ON f.dim_hospital_id = h.dim_hospital_id
WHERE f.rank_in_state <= 3
ORDER BY l.state, f.rank_in_state;
-- GRAPH 7 SQL "Priemerná hustota siete"
SELECT 
    l.state,
    COUNT(DISTINCT f.dim_hospital_id) / COUNT(DISTINCT f.dim_network_id) as avg_hospitals_per_network
FROM fact_hospitals f
JOIN dim_location l ON f.dim_location_id = l.dim_location_id
GROUP BY l.state
ORDER BY avg_hospitals_per_network DESC;
-- GRAPH 8 SQL "Analýza trhovej koncentrácie nemocničných sietí"
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