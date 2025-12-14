

------------------------Création Shéma pour les transformation---------------------------------
----------------------------------------------------------------------------------------------
CREATE SCHEMA analytics_elhouzi_amazouz;


-------------------------------Raffinage de la table bike_rentals ------------------------------
------------------------------------------------------------------------------------------------
CREATE TABLE analytics_elhouzi_amazouz.silver_bike_rentals AS
WITH cleaned_data AS (
    SELECT
        CAST(rental_id AS BIGINT) as rental_id,
        CAST(bike_id AS INTEGER) as bike_id,
        CAST(user_id AS INTEGER) as user_id,

        --1. STANDARDISATION DES STATIONS sous forme STA_xxxx
        'STA_' || REGEXP_REPLACE(start_station_id, '[^0-9]', '', 'g') AS start_station_id,
        'STA_' || REGEXP_REPLACE(end_station_id, '[^0-9]', '', 'g') AS end_station_id,

        -- 2. NETTOYAGE start_t (Retirer 'TBD')
        CASE
            WHEN start_t IS NULL OR start_t IN ('[null]', '', 'TBD', 'null') THEN NULL
            WHEN start_t LIKE '%/%' THEN TO_TIMESTAMP(start_t, 'DD/MM/YYYY HH24:MI:SS')
            ELSE TO_TIMESTAMP(start_t, 'YYYY-MM-DD HH24:MI:SS')
        END AS start_ts,

        -- 3. NETTOYAGE end_t (Retirer 'TBD')
        CASE
            WHEN end_t IS NULL OR end_t IN ('[null]', '', 'TBD', 'null') THEN NULL
            WHEN end_t LIKE '%/%' THEN TO_TIMESTAMP(end_t, 'DD/MM/YYYY HH24:MI:SS')
            ELSE TO_TIMESTAMP(end_t, 'YYYY-MM-DD HH24:MI:SS')
        END AS end_ts

    FROM raw.bike_rentals
)
SELECT
    rental_id,
    bike_id,
    user_id,
    start_station_id,
    end_station_id,
    start_ts,
    end_ts,
    -- 4. CALCUL DE LA DURÉE
    EXTRACT(EPOCH FROM (end_ts - start_ts)) / 60 AS duration_minutes
FROM cleaned_data
WHERE
    start_ts IS NOT NULL 
    AND end_ts IS NOT NULL
	-- filtre durée minimale 2 minutes
    AND (EXTRACT(EPOCH FROM (end_ts - start_ts)) / 60) >= 2;

--- Affichage les premier 100 ligne de la table Transformer Silver_bike_rentals
SELECT * FROM analytics_elhouzi_amazouz.silver_bike_rentals
ORDER BY rental_id ASC 
LIMIT 100;
-----------------------------------------------------------------------------------------------------



----------------------------------Raffinage de la table bike_rentals---------------------------------
-----------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS analytics_elhouzi_amazouz.silver_user_accounts;

CREATE TABLE analytics_elhouzi_amazouz.silver_user_accounts AS

WITH cleaned_users AS (
    SELECT
        CAST(user_id AS INTEGER) as user_id,
        
        -- 1. CORRECTION EMAIL (# -> @)
        CASE 
            WHEN email LIKE '%#%' THEN REPLACE(email, '#', '@')
            ELSE email 
        END as email_clean,

        -- 2. RECUPERATION nom et prenom via email (Si manquants)
        CASE 
            WHEN first_name IS NULL OR first_name IN ('[null]', '', 'null') THEN 
                INITCAP(SPLIT_PART(SPLIT_PART(REPLACE(email, '#', '@'), '@', 1), '.', 1))
            ELSE INITCAP(first_name)
        END as first_name,

        CASE 
            WHEN last_name IS NULL OR last_name IN ('[null]', '', 'null') THEN 
                INITCAP(SPLIT_PART(SPLIT_PART(REPLACE(email, '#', '@'), '@', 1), '.', 2))
            ELSE INITCAP(last_name)
        END as last_name,

        -- 3. TELEPHONE (Chiffres uniquement)
        CASE 
            WHEN phone_number IS NULL OR phone_number IN ('[null]', '', 'null') THEN NULL
            ELSE REGEXP_REPLACE(phone_number, '[^0-9]', '', 'g')
        END as phone_raw,

        -- 4. DATES DE NAISSANCE
        CASE 
            WHEN birthdate IS NULL OR birthdate IN ('[null]', '', 'null') THEN NULL
            WHEN birthdate ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(birthdate, 'MM-DD-YYYY')
            WHEN birthdate LIKE '%/%' THEN TO_DATE(birthdate, 'DD/MM/YYYY')
            ELSE TO_DATE(birthdate, 'YYYY-MM-DD')
        END as birth_date_clean,

        -- 5. DATE INSCRIPTION
        CASE 
            WHEN registration_date IS NULL OR registration_date IN ('[null]', '', 'null') THEN NULL
            WHEN registration_date ~ '[A-Za-z]{3}' THEN TO_DATE(registration_date, 'DD Mon YYYY')
            WHEN registration_date LIKE '%/%/%' AND LENGTH(registration_date) < 10 THEN TO_DATE(registration_date, 'MM/DD/YY')
            ELSE TO_DATE(registration_date, 'YYYY-MM-DD')
        END as registration_date_clean,

        -- 6. ABONNEMENT
        CASE 
            WHEN subscription_id IN ('[null]', '', 'null') THEN NULL
            ELSE LOWER(TRIM(subscription_id))
        END as subscription_id

    FROM raw.user_accounts
)
SELECT
    user_id,
    first_name,
    last_name,
    email_clean as email,
    
    -- FORMATAGE FORMA TELEPHONE
    CASE 
        WHEN LENGTH(phone_raw) = 9 THEN '0' || phone_raw
        WHEN LENGTH(phone_raw) = 11 AND phone_raw LIKE '33%' THEN '0' || SUBSTRING(phone_raw, 3)
        WHEN LENGTH(phone_raw) = 11 AND phone_raw LIKE '1%' THEN '0' || SUBSTRING(phone_raw, 2)
        WHEN phone_raw LIKE '0%' THEN phone_raw
        ELSE phone_raw 
    END as phone,

    birth_date_clean as birth_date,
    registration_date_clean as registration_date,
    subscription_id,
    DATE_PART('year', AGE(CURRENT_DATE, birth_date_clean)) as age
FROM cleaned_users
WHERE 
    birth_date_clean IS NOT NULL 
    AND registration_date_clean IS NOT NULL
    AND subscription_id IS NOT NULL;
-------------------------------------------------------------------------------------------------------------------------


------------------------------------------Raffinage de la table subscriptions--------------------------------------------
-------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS analytics_elhouzi_amazouz.silver_subscriptions;

CREATE TABLE analytics_elhouzi_amazouz.silver_subscriptions AS
WITH cleaned_subs AS (
    SELECT
        LOWER(TRIM(subscription_id)) as subscription_id,

        -- Au lieu de NULL, on met un texte par défaut pour sub_007
        CASE 
            WHEN sub_name IS NULL OR sub_name IN ('[null]', '', 'null') THEN 'Non défini'
            ELSE sub_name 
        END as sub_name,

        -- Nettoyage Prix
        CASE 
            WHEN price IS NULL OR CAST(price AS TEXT) IN ('[null]', '') THEN NULL
            ELSE REGEXP_REPLACE(CAST(price AS TEXT), '[^0-9.]', '', 'g')
        END as price_raw,

        -- Standardisation Devise
        CASE 
            WHEN currency IN ('€', 'Euro', 'euro') THEN 'EUR'
            WHEN currency IS NULL OR currency IN ('[null]', '') THEN 'EUR'
            ELSE currency
        END as currency,

        -- Dates
        CASE 
            WHEN CAST(start_date AS TEXT) IN ('[null]', '', 'null') OR start_date IS NULL THEN NULL
            ELSE CAST(start_date AS DATE)
        END as start_date,
        
        CASE 
            WHEN CAST(end_date AS TEXT) IN ('[null]', '', 'null') OR end_date IS NULL THEN NULL
            ELSE CAST(end_date AS DATE)
        END as end_date,

        country_scope

    FROM raw.subscriptions
)
SELECT
    subscription_id,
    sub_name,
    CAST(price_raw AS DECIMAL(10, 2)) as price,
    currency,
    country_scope,
    start_date,
    end_date,
    -- Détermination du statut actif/inactif
    CASE 
        WHEN end_date IS NULL OR end_date > CURRENT_DATE THEN true 
        ELSE false 
    END as is_active

FROM cleaned_subs
WHERE 
    price_raw IS NOT NULL; 
-----------------------------------------------------------------------------------------------------------------------





----------------------------------------------Raffinage de la table bikes----------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS analytics_elhouzi_amazouz.silver_bikes;

CREATE TABLE analytics_elhouzi_amazouz.silver_bikes AS
WITH cleaned_bikes AS (
    SELECT
        CAST(bike_id AS INTEGER) as bike_id,
        TRIM(model_name) as model_name,
        
        -- CORRECTION TYPE (Basé sur le nom du modèle)
        CASE 
            WHEN model_name LIKE 'E-%' THEN 'electrique'
            WHEN model_name LIKE 'CityBike%' THEN 'mecanique'
            WHEN LOWER(bike_type) IN ('electric', 'electrique') THEN 'electrique'
            WHEN LOWER(bike_type) IN ('mecanique', 'mécanique') THEN 'mecanique'
            ELSE 'mecanique'
        END as bike_type,

        -- CORRECTION DATE 
        CASE 
            
            WHEN CAST(commissioning_date AS DATE) = '1970-01-01' THEN NULL
            WHEN commissioning_date IS NULL THEN NULL
            ELSE CAST(commissioning_date AS DATE)
        END as commissioning_date,

        LOWER(TRIM(status)) as status

    FROM raw.bikes
)
SELECT
    bike_id,
    model_name,
    bike_type,
    commissioning_date,
    status,
    EXTRACT(YEAR FROM commissioning_date) as year_service
FROM cleaned_bikes;
------------------------------------------------------------------------------------------------------------------------



------------------------------------------------Raffinage de la table cities--------------------------------------------
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS analytics_elhouzi_amazouz.silver-cities;

CREATE TABLE analytics_elhouzi_amazouz.silver_cities AS
SELECT
    CAST(city_id AS INTEGER) as city_id,
    UPPER(TRIM(city_name)) as city_name,
    TRIM(region) as region,
    UPPER(TRIM(country)) as country

FROM raw.cities;
------------------------------------------------------------------------------------------------------------------------



------------------------------------------------Raffinage de la tablebike_stations--------------------------------------
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS analytics_elhouzi_amazouz.silver_bike_stations;

CREATE TABLE analytics_elhouzi_amazouz.silver_bike_stations AS
SELECT
    'STA_' || REGEXP_REPLACE(station_id, '[^0-9]', '', 'g') AS station_id,
    CAST(city_id AS INTEGER) as city_id,
    INITCAP(TRIM(station_name)) AS station_name,
    UPPER(TRIM(city_name)) AS city,
    
    CASE 
        WHEN capacity IS NULL OR CAST(capacity AS TEXT) IN ('[null]', '', 'null') THEN 0 
        ELSE CAST(capacity AS INTEGER) 
    END as capacity,
    
    CASE
        WHEN latitude IS NULL OR CAST(latitude AS TEXT) = '' THEN NULL
        WHEN CAST(latitude AS TEXT) ~ '[a-zA-Z]' THEN NULL
        ELSE CAST(REPLACE(TRIM(CAST(latitude AS TEXT)), ',', '.') AS DECIMAL(10, 6))
    END as latitude,
    
    CASE
        WHEN longitude IS NULL OR CAST(longitude AS TEXT) = '' THEN NULL
        WHEN CAST(longitude AS TEXT) ~ '[a-zA-Z]' THEN NULL
        ELSE CAST(REPLACE(TRIM(CAST(longitude AS TEXT)), ',', '.') AS DECIMAL(10, 6))
    END as longitude

FROM raw.bike_stations
WHERE 
    station_name IS NOT NULL 
    AND station_name NOT IN ('[null]', '', 'null')
    AND CAST(city_id AS INTEGER) <> 99;
-------------------------------------------------------------------------------------------------------------------------


----------------------------------------------------Couche GOLD----------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS analytics_elhouzi_amazouz.gold_daily_activity;
CREATE TABLE analytics_elhouzi_amazouz.gold_daily_activity AS
SELECT
    r.start_ts::DATE AS rental_date,
    s.city_name AS city,
    st.station_name AS start_station,
	st.latitude,
    st.longitude,
    b.bike_type AS bike_type,
    sub.sub_name AS sub_name,
    
    COUNT(r.rental_id) AS total_rentals,
    ROUND(AVG(r.duration_minutes), 2) AS average_duration_minutes,
    COUNT(DISTINCT r.user_id) AS unique_users

FROM analytics_elhouzi_amazouz.silver_bike_rentals r

JOIN analytics_elhouzi_amazouz.silver_user_accounts u
    ON r.user_id = u.user_id

JOIN analytics_elhouzi_amazouz.silver_bikes b
    ON r.bike_id = b.bike_id

JOIN analytics_elhouzi_amazouz.silver_bike_stations st
    ON r.start_station_id = st.station_id

JOIN analytics_elhouzi_amazouz.silver_cities s
    ON st.city_id = s.city_id

JOIN analytics_elhouzi_amazouz.silver_subscriptions sub
    ON u.subscription_id = sub.subscription_id

GROUP BY rental_date, city_name, start_station,st.latitude,st.longitude, bike_type, sub_name
ORDER BY rental_date, city_name, start_station;
-------------------------------------------------------------------------------------------------------------------------



-----------------------------------------------------GESTION DES RÔLES ET DES DROITS-------------------------------------
-------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------
--Création des rôles--
CREATE ROLE marketing_user LOGIN PASSWORD 'Epsi##25';
CREATE ROLE manager_lyon LOGIN PASSWORD 'Epsi##25';


--Suppression des droits sur le schéma raw pour marketing--
REVOKE ALL ON SCHEMA raw FROM marketing_user;
REVOKE ALL ON ALL TABLES IN SCHEMA raw FROM marketing_user;



--Suppression des droits sur le schéma raw pour marketing--
GRANT USAGE ON SCHEMA analytics_elhouzi_amazouz TO marketing_user;
GRANT SELECT ON  analytics_elhouzi_amazouz.gold_daily_activity TO marketing_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA analytics_elhouzi_amazouz
GRANT SELECT ON TABLES TO marketing_user;


---Test du rôle marketing_user--
SET ROLE marketing_user;
SELECT * FROM raw.user_accounts;

SET ROLE marketing_user;
SELECT * FROM analytics_elhouzi_amazouz.gold_daily_activity;

RESET ROLE;

-- Droits pour manager Lyon
GRANT USAGE ON SCHEMA analytics_elhouzi_amazouz TO manager_lyon;
GRANT SELECT ON  analytics_elhouzi_amazouz.gold_daily_activity TO manager_lyon;




--Activation de la Row Level Security--
ALTER TABLE  analytics_elhouzi_amazouz.gold_daily_activity ENABLE ROW LEVEL SECURITY;


DROP POLICY lyon_only_policy ON analytics_elhouzi_amazouz.gold_daily_activity

--Politique : le manager Lyon ne voit que la ville de LYON---
CREATE POLICY lyon_only_policy
ON analytics_elhouzi_amazouz.gold_daily_activity
FOR SELECT
TO manager_lyon
USING (city = 'LYON')

--Test du rôle manager_lyon--
SET ROLE manager_lyon;
SELECT * FROM analytics_elhouzi_amazouz.gold_daily_activity;

