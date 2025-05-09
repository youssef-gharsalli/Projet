-- BDD segmentation client :
create or replace temporary table datalake_client as 
WITH 
DPC_JSON AS (
    SELECT 
        JSON:"codeMarque":: STRING AS marquetech,
        JSON:"numeroExpedition":: STRING AS numeroExpedition,
        JSON:"idExpedition":: STRING AS idfast,
        JSON:"codeEnseigne":: STRING AS codeEnseigne,
        concat(to_char(JSON:"codeMarque":: STRING), To_char(JSON:"codeEnseigne":: STRING)) AS clientoriginetech,
        JSON:"expediteur":"adresse":"codePostal":: STRING AS CODE_POSTAL_EXPEDITEUR,
        JSON:"expediteur":"adresse":"pays":: STRING AS PAYS_EXPEDITEUR,
        JSON:"expediteur":"email"::STRING as EMAIL_EXPEDITEUR,
        REPLACE(REPLACE(TRIM(JSON:"expediteur":"telephone1"::VARCHAR(100)),' ',''),'.','') AS expediteur_telephone1,
        REPLACE(REPLACE(TRIM(JSON:"expediteur":"telephone2"::VARCHAR(100)),' ',''),'.','') AS expediteur_telephone2,
        COALESCE(expediteur_telephone1,expediteur_telephone2) expediteur_telephone,
        JSON:"destinataire":"adresse":"codePostal":: STRING AS CODE_POSTAL_DESTINATAIRE,
        UPPER(TRIM(JSON:"destinataire":"email":: STRING)) AS EMAIL_DESTINATAIRE,
        REPLACE(REPLACE(TRIM(JSON:"destinataire":"telephone1"::VARCHAR(100)),' ',''),'.','') AS destinataire_telephone1,
        REPLACE(REPLACE(TRIM(JSON:"destinataire":"telephone2"::VARCHAR(100)),' ',''),'.','') AS destinataire_telephone2,
        COALESCE(destinataire_telephone1,destinataire_telephone2) destinataire_telephone,
        JSON:"livraison":"mode":: STRING AS MODELIVRAISONCODE,
        JSON:"livraison":"pays":: STRING AS PAYS_LIVRAISON,
        TO_CHAR(JSON:"dateAnnonce"::TIMESTAMP_NTZ(9),'YYYYMMDD') AS date_dpc_fk,
        JSON:"dateAnnonce"::TIMESTAMP_NTZ(9) AS dateAnnonce,
        IFF(JSON:"lirem"::string is null,'N/A',JSON:"lirem"::string) as Point_Remise, 
        ROW_NUMBER() OVER (PARTITION BY JSON:"idExpedition" ORDER BY JSON:"dateAnnonce" DESC) AS row_dpc_json
    FROM DWMR_Datalake_Prod.PUBLIC.DPC
    WHERE IS_NULL_VALUE(JSON:"codeMarque")=FALSE
    AND IS_NULL_VALUE(JSON:"numeroExpedition")=FALSE
    AND IS_NULL_VALUE (JSON : "idExpedition")=FALSE
    AND date(JSON:"dateAnnonce"::timestamp) >DATEADD(YEAR, -4, '2024-10-31') 
    and date(JSON:"dateAnnonce"::timestamp)<='2024-10-31'
)
SELECT 
    marquetech,
    numeroExpedition,
    idfast,
    codeEnseigne,
    clientoriginetech,
    CODE_POSTAL_EXPEDITEUR,
    PAYS_EXPEDITEUR,
    EMAIL_EXPEDITEUR,
    expediteur_telephone1,
    expediteur_telephone2,
    CASE WHEN 
        expediteur_telephone IS NOT NULL
        AND (length(expediteur_telephone1)>4 OR length(expediteur_telephone2)>4)
        AND (TRY_CAST(RIGHT(expediteur_telephone1,length(expediteur_telephone1)-4) AS INT)+ TRY_CAST(RIGHT(expediteur_telephone2,length(expediteur_telephone2)-4) AS INT) !=0
        OR (expediteur_telephone1 IS NULL AND expediteur_telephone2 IS NOT NULL) OR (expediteur_telephone1 IS NOT NULL AND expediteur_telephone2 IS NULL))
    THEN
        expediteur_telephone
    ELSE NULL 
    END AS expediteur_telephone,
    CODE_POSTAL_DESTINATAIRE,
    EMAIL_DESTINATAIRE,
    destinataire_telephone1,
    destinataire_telephone2,
    CASE WHEN 
        destinataire_telephone IS NOT NULL
        AND (length(destinataire_telephone1)>4 OR length(destinataire_telephone2)>4)
        AND (TRY_CAST(RIGHT(destinataire_telephone1,length(destinataire_telephone1)-4) AS INT)+ TRY_CAST(RIGHT(destinataire_telephone2,length(destinataire_telephone2)-4) AS INT) !=0
        OR (destinataire_telephone1 IS NULL AND destinataire_telephone2 IS NOT NULL) OR (destinataire_telephone1 IS NOT NULL AND destinataire_telephone2 IS NULL))
    THEN
        destinataire_telephone
    ELSE NULL 
    END AS destinataire_telephone,
    MODELIVRAISONCODE,
    PAYS_LIVRAISON,
    date_dpc_fk,
    dateAnnonce,
    Point_Remise
FROM DPC_JSON
WHERE row_dpc_json = 1;

-- Requête pour récupérer les événements de PEC
create or replace temporary table client as 
SELECT * 
FROM datalake_client d
INNER JOIN (
    SELECT 
        JSON:"expedition":"idExpedition":: STRING AS ID_EXPEDITION,
        MIN(JSON:"date"::timestamp) AS DATE_PREMIER_PEC
    FROM   DWMR_Datalake_Prod.PUBLIC.EVENEMENT_EXPEDITION
    WHERE  JSON:"code":: STRING = 'PEC' 
    GROUP BY 1
    HAVING MIN(JSON:"date"::timestamp)>DATEADD(YEAR, -4, '2024-10-31') 
    and MIN(JSON:"date"::timestamp)<='2024-10-31'
) PEC1
ON PEC1.ID_EXPEDITION=D.idfast;

-- Base de données email anonymisée pour analyser l'activité des clients
create or replace temporary table Base_client_email as 
SELECT 
    dr.EMAIL,
    client as client,
    EXPEDITEUR,
    DESTINATAIRE,
    PREMIERE_ACTIVITE,
    RETOUR,
    IFF(COMPTE_CREE_SUR_SITE is null, FALSE, COMPTE_CREE_SUR_SITE) AS COMPTE_CREE_SUR_SITE,
    IFF(ESTABONNEOPTINMR is null, FALSE, ESTABONNEOPTINMR) AS ESTABONNEOPTINMR,
    year(PREMIERE_ACTIVITE) = year(dateadd('year', -1, '2024-10-31')) AS activ_An_1,
    PREMIERE_ACTIVITE > dateadd('month', -12, '2024-10-31') AS activ_12,
    (PREMIERE_ACTIVITE > dateadd('month', -24, '2024-10-31') AND PREMIERE_ACTIVITE <= dateadd('month', -12, '2024-10-31')) AS activ_24,
    (PREMIERE_ACTIVITE > dateadd('month', -36, '2024-10-31') AND PREMIERE_ACTIVITE <= dateadd('month', -24, '2024-10-31')) AS activ_36
FROM Date_recrute dr
LEFT JOIN (
    SELECT  
        DISTINCT
        IFF(c.origine = 'WEB', true, false) AS compte_cree_sur_site,
        UPPER(TRIM(EMAIL)) AS EMAIL,
        ESTABONNEOPTINMR
    FROM DWMR_PROD.PUBLIC.dimclientorigine c
    WHERE c.membreactif
    AND DATEPREMIERCOLIS <= '2024-10-31'
) i
ON i.EMAIL=DR.EMAIL
WHERE PREMIERE_ACTIVITE <= '2024-10-31';
