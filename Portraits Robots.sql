--portrait robot
    --  Les noms de tables, colonnes et valeurs ont été modifiés à des fins d'anonymisation.
    --  Cette requête est inspirée d'un cas réel dans le secteur logistique.

--Répartition des livraisons par département 

WITH recipient_city_counts AS (
  SELECT
    LEFT(recipient_postal_code, 2) AS department,
    recipient_country AS country,
    COUNT(*) AS city_count
  FROM
    analytics.parcel_shipping_snapshot
  WHERE
    recipient_country = 'FR'
    AND recipient_postal_code NOT LIKE '@%'
    AND event_type = 'ParcelDelivered'
  GROUP BY
    LEFT(recipient_postal_code, 2), recipient_country
)
SELECT
  department,
  country,
  ROUND((SUM(city_count) * 100.0 / SUM(SUM(city_count)) OVER()), 2) AS percentage
FROM
  recipient_city_counts
GROUP BY
  department, country
ORDER BY
  percentage DESC;

--Répartition des envois par département d’origine

WITH sender_city_counts AS (
  SELECT
    LEFT(sender_postal_code, 2) AS department,
    sender_country AS country,
    COUNT(sender_city) AS city_count
  FROM
    analytics.parcel_shipping_snapshot
  WHERE
    sender_postal_code NOT LIKE '@%'
    AND event_type = 'ParcelDelivered'
  GROUP BY
    LEFT(sender_postal_code, 2), sender_country
)
SELECT
  department,
  country,
  ROUND((SUM(city_count) * 100.0 / SUM(SUM(city_count)) OVER()), 2) AS percentage
FROM
  sender_city_counts
GROUP BY
  department, country
ORDER BY
  percentage DESC;

--Répartition des modes de livraison utilisés : PUDO vs APM

WITH unique_shipping AS (
  SELECT
    shipping_id,
    MIN(delivery_mode) AS dominant_mode
  FROM
    analytics.parcel_shipping_snapshot
  WHERE
    event_type = 'ParcelDelivered'
    AND delivery_mode IN ('PUDO', 'APM')
    AND first_event_date >= NOW() - INTERVAL 2 YEAR
  GROUP BY
    shipping_id
),
total_shipping AS (
  SELECT COUNT(*) AS total_count FROM unique_shipping
),
shipping_by_mode AS (
  SELECT
    dominant_mode,
    COUNT(*) AS count_by_mode
  FROM
    unique_shipping
  GROUP BY
    dominant_mode
)
SELECT
  dominant_mode,
  ROUND((count_by_mode * 100.0 / total_count), 2) AS percentage
FROM
  shipping_by_mode, total_shipping
ORDER BY
  percentage DESC;

--Clients utilisant un ou plusieurs types de livraison

WITH unique_shipping AS (
  SELECT
    shipping_id,
    ARRAY_AGG(DISTINCT delivery_mode) AS modes_used
  FROM
    analytics.parcel_shipping_snapshot
  WHERE
    event_type = 'ParcelDelivered'
    AND delivery_mode IN ('PUDO', 'APM')
    AND first_event_date >= NOW() - INTERVAL 2 YEAR
  GROUP BY
    shipping_id
),
total_shipping AS (
  SELECT COUNT(*) AS total_count FROM unique_shipping
),
shipping_mixed_usage AS (
  SELECT
    CASE 
      WHEN SIZE(modes_used) > 1 THEN 'Mixed'
      WHEN ARRAY_CONTAINS(modes_used, 'PUDO') THEN 'PUDO'
      WHEN ARRAY_CONTAINS(modes_used, 'APM') THEN 'APM'
      ELSE 'Other'
    END AS mode_usage,
    COUNT(*) AS count_by_mode
  FROM unique_shipping
  GROUP BY mode_usage
)
SELECT
  mode_usage,
  ROUND((count_by_mode * 100.0 / total_count), 2) AS percentage
FROM
  shipping_mixed_usage, total_shipping
ORDER BY
  percentage DESC;

-- Fréquence d’achat moyenne par client sur les 12 derniers mois

SELECT
  COUNT(DISTINCT shipping_id) AS total_orders,
  COUNT(DISTINCT recipient_email) AS unique_customers,
  ROUND(
    COUNT(DISTINCT shipping_id) / NULLIF(COUNT(DISTINCT recipient_email), 0), 
    1
  ) AS average_order_frequency
FROM
  analytics.parcel_shipping_snapshot
WHERE
  event_type = 'ParcelDelivered'
  AND event_country = 'FR'
  AND first_event_date >= NOW() - INTERVAL 1 YEAR;




