# Script anonymisé pour extraction des utilisateurs Opt-in
# Technologies : PySpark + SQL + stockage ADLS

from datetime import date
from pyspark.sql.functions import col

# Date du jour
today_date = date.today().strftime("%Y%m%d")

# Chargement des données opt-in et opt-out
optin_df = spark.sql("""
WITH Base_Optin AS (
    SELECT *
    FROM read_files('abfss://lake@companylake.dfs.core.windows.net/FR/Marketing/Email_Optin',
    format => 'csv',
    header => true,
    mode => 'PERMISSIVE')
),
Base_Optout AS (
    SELECT email_optout
    FROM read_files('abfss://lake@companylake.dfs.core.windows.net/FR/Marketing/Email_Optout/optout_2024.csv',
    format => 'csv',
    header => true,
    sep =>';',
    mode => 'PERMISSIVE')
)
SELECT *
FROM Base_Optin
WHERE TRIM(UPPER(email)) NOT IN (
    SELECT DISTINCT TRIM(UPPER(email_optout)) FROM Base_Optout
)
""")

# Écriture du fichier filtré vers ADLS
optin_df.coalesce(1).write.mode("overwrite").csv(
    "abfss://lake@companylake.dfs.core.windows.net/FR/Marketing/Email_Optin_Filtered",
    header=True, encoding='UTF-8')

# Renommer automatiquement le fichier CSV

def get_latest_csv(directory):
    files = dbutils.fs.ls(directory)
    for f in files:
        if "csv" in f.name:
            return directory + f.name

old_name = get_latest_csv("abfss://lake@companylake.dfs.core.windows.net/FR/Marketing/Email_Optin_Filtered/")
new_name = f"abfss://lake@companylake.dfs.core.windows.net/FR/Marketing/Email_Optin_Filtered/Base_Optin_{today_date}.csv"
dbutils.fs.mv(old_name, new_name)

# Exemple de segmentation utilisateur Web (client/prospect)
df_user_analysis = spark.sql("""
WITH DateReference AS (
    SELECT CAST(add_months(now(), -36) AS DATE) AS date_ref
),
PickupPoints AS (
    SELECT DISTINCT country || '-' || code AS location_code, type AS pickup_type
    FROM prod.dimension.pickup_locations
    WHERE type IN ('PUDO', 'LOCKER')
)
SELECT 
    u.id,
    UPPER(NULLIF(u.title, '')) AS civility,
    u.last_name,
    u.first_name,
    LOWER(u.email) AS email,
    u.phone,
    u.language,
    CASE WHEN COUNT(DISTINCT p.id) > 0 THEN 'Client' ELSE 'Prospect' END AS user_type,
    a.postal_code,
    a.country_code,
    CASE WHEN u.is_inactive = TRUE THEN FALSE ELSE TRUE END AS is_active,
    CASE WHEN u.business_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_business_account,
    u.favorite_pickup_code,
    fr.pickup_type AS favorite_pickup_type,
    MAX(CASE WHEN cp.pickup_type = 'LOCKER' THEN TRUE ELSE FALSE END) AS has_dropped_in_locker,
    MAX(CASE WHEN cp.pickup_type = 'PUDO' THEN TRUE ELSE FALSE END) AS has_dropped_in_pudo,
    MAX(CASE WHEN dp.pickup_type = 'LOCKER' THEN TRUE ELSE FALSE END) AS has_sent_to_locker,
    MAX(CASE WHEN dp.pickup_type = 'PUDO' THEN TRUE ELSE FALSE END) AS has_sent_to_pudo,
    MAX(CAST(p.creation_date AS DATE)) AS last_parcel_date,
    CASE 
        WHEN DATEDIFF(MONTH, MAX(CAST(p.creation_date AS DATE)), NOW()) < 3 THEN 'less than 3 months'
        WHEN DATEDIFF(MONTH, MAX(CAST(p.creation_date AS DATE)), NOW()) BETWEEN 3 AND 5 THEN '3-6 months'
        WHEN DATEDIFF(MONTH, MAX(CAST(p.creation_date AS DATE)), NOW()) >= 6 THEN '6+ months'
        ELSE 'Prospect'
    END AS usage_recency_segment,
    CASE WHEN DATEDIFF(MONTH, MAX(CAST(p.creation_date AS DATE)), NOW()) <= 12 AND u.business_id IS NULL THEN TRUE ELSE FALSE END AS active_optin_12mo
FROM prod.web.users u
LEFT JOIN prod.parcels.events p ON TRIM(UPPER(u.email)) = TRIM(UPPER(p.recipient_email))
LEFT JOIN prod.web.addresses a ON u.billing_address_id = a.id
LEFT JOIN PickupPoints fr ON fr.location_code = u.favorite_pickup_code
LEFT JOIN PickupPoints cp ON cp.location_code = p.pickup_point_code
LEFT JOIN PickupPoints dp ON dp.location_code = p.delivery_point_code
WHERE u.is_email_optin = TRUE 
  AND (a.country_code = 'FR' OR (UPPER(u.language) LIKE '%FR%' AND (a.country_code IS NULL OR a.country_code = 'FR')))
  AND (
    (p.creation_date >= NOW() - INTERVAL 36 MONTH AND p.status = 'Shipped') OR
    (p.event_type = 'Delivered' AND p.event_date >= NOW() - INTERVAL 36 MONTH)
  )
GROUP BY ALL
""")

# Export du résultat
output_path = "abfss://lake@companylake.dfs.core.windows.net/FR/Marketing/User_Segmentation/"
df_user_analysis.coalesce(1).write.mode("overwrite").csv(output_path, header=True, encoding='UTF-8')
