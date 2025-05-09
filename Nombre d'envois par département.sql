--Nombre d'envois par département dans une région (ex : région IDF)

SELECT 
    DATE_TRUNC('month', event_date) AS month,
    LEFT(delivery_location_code, 2) AS department,
    COUNT(shipping_id) AS shipping_count
FROM 
    analytics.fact_parcel_shipping
WHERE 
    event_date BETWEEN '2023-01-01' AND '2025-04-30'
    AND country_code = 'FR'
    AND (
        delivery_location_code LIKE '75%' OR
        delivery_location_code LIKE '77%' OR
        delivery_location_code LIKE '78%' OR
        delivery_location_code LIKE '91%' OR
        delivery_location_code LIKE '92%' OR
        delivery_location_code LIKE '93%' OR
        delivery_location_code LIKE '94%' OR
        delivery_location_code LIKE '95%'
    )
GROUP BY 
    month, department
ORDER BY 
    month, department;

    -- Remarques :
--Les noms de tables, colonnes et valeurs ont été modifiés à des fins d'anonymisation.
--Cette requête est inspirée d'un cas réel dans le secteur logistique.
