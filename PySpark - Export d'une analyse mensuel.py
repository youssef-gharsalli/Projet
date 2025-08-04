# PySpark - Export d'une analyse mensuelle en Parquet
from pyspark.sql.functions import *

spark.sql("""
    SELECT 
        YEAR(e.date_creation) AS annee, 
        MONTH(e.date_creation) AS mois, 
        p.email_expediteur, 
        CASE 
            WHEN COUNT(p.numero_colis) = 1 THEN '1 étiquette'
            WHEN COUNT(p.numero_colis) = 2 THEN '2 étiquettes'
            WHEN COUNT(p.numero_colis) = 3 THEN '3 étiquettes'
            WHEN COUNT(p.numero_colis) BETWEEN 4 AND 8 THEN '4 à 8 étiquettes'
            WHEN COUNT(p.numero_colis) > 8 THEN '9+ étiquettes'
        END AS segment_etiquettes
    FROM donnees.colis p
    JOIN donnees.utilisateur u
        ON p.email_expediteur = u.email
    JOIN donnees.volumes_facturation f
        ON p.numero_colis = f.numero_colis
    LEFT JOIN donnees.expeditions e
        ON u.id_utilisateur = e.id_utilisateur
    WHERE 
        e.date_creation >= '2023-01-01'
        AND p.marque IN ('X1', 'X2')
        AND u.est_pro = FALSE
        AND f.code_client IN ('CANAL_WEB', 'PARTENAIRE_Y')
    GROUP BY 
        p.email_expediteur, 
        YEAR(e.date_creation), 
        MONTH(e.date_creation)
    HAVING COUNT(p.numero_colis) >= 1
""").coalesce(1).write.mode("overwrite").parquet(
    "abfss://chemin_stockage/Segmentation_client/volume_etiquettes"
)
