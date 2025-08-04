from pyspark.sql.functions import *

spark.sql("""
WITH exclusion_clients AS (
    SELECT    
        origine_client
    FROM
        donnees.facturation_colis 
    WHERE 
        LEFT(numero_colis, 2) IN ('X1', 'X2') 
        AND code_client IN ('CANAL_WEB', 'PARTENAIRE_Y') 
        AND date_facturation BETWEEN '2023-11-01' AND '2024-10-31'
    GROUP BY
        origine_client
    HAVING COUNT(DISTINCT numero_colis) > 19
)

SELECT  
    fct.origine_client, 
    fct.code_client, 
    fct.entite_commerciale, 
    fct.pays_facturation, 
    fct.pays_collecte, 
    fct.pays_livraison, 
    fct.type_injection, 
    fct.mode_livraison, 
    fct.service, 
    fct.groupe_service,
    colis.mode_paiement,
    colis.tranche_poids,
    WEEKOFYEAR(fct.date_facturation) AS semaine,
    MONTH(fct.date_facturation) AS mois,
    YEAR(fct.date_facturation) AS annee,
    fct.date_facturation,
    SUM(CASE 
        WHEN fct.montant_ht > 0 THEN 1 
        WHEN fct.montant_ht < 0 THEN -1  
        ELSE 0 
    END) AS nb_colis, 
    SUM(fct.montant_ht) AS revenus_ht
FROM 
    donnees.facturation_colis fct
JOIN (
    SELECT 
        numero_colis, 
        date_facturation, 
        MAX(mode_paiement) AS mode_paiement, 
        MAX(tranche_poids) AS tranche_poids 
    FROM 
        donnees.details_colis_web 
    WHERE 
        date_facturation BETWEEN '2024-01-01' AND '2024-12-31' 
    GROUP BY 
        numero_colis, 
        date_facturation
) colis 
ON colis.numero_colis = fct.numero_colis 
AND colis.date_facturation = fct.date_facturation 
LEFT OUTER JOIN exclusion_clients excl
ON fct.origine_client = excl.origine_client
WHERE 
    LEFT(fct.numero_colis, 2) IN ('X1', 'X2') 
    AND fct.code_client IN ('CANAL_WEB', 'PARTENAIRE_Y') 
    AND fct.date_facturation BETWEEN '2024-01-01' AND '2024-12-31'
    AND excl.origine_client IS NULL
GROUP BY ALL
""").coalesce(1).write.mode("overwrite").parquet(
    "abfss://chemin_stockage/Segmentation_client/C2U_WEB_parquet/C2U.parquet"
)
