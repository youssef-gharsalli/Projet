WITH unique_colis AS (
  -- Attribue à chaque colis un seul type dominant (REL ou APM)
  SELECT
    numero_colis,
    MIN(type_point_retrait) AS type_dominant
  FROM
    schema_colis.faits_colis
  WHERE
    evenement = 'ColisLivre'
    AND type_point_retrait IN ('REL', 'APM')
    AND date_premier_evenement >= CURRENT_DATE - INTERVAL '2 year'
  GROUP BY
    numero_colis
),
total_colis AS (
  SELECT COUNT(*) AS total
  FROM unique_colis
),
repartition_par_type AS (
  SELECT
    type_dominant AS type_point_retrait,
    COUNT(*) AS nb_colis
  FROM
    unique_colis
  GROUP BY
    type_dominant
)
-- Calcule les pourcentages par type
SELECT
  r.type_point_retrait,
  ROUND((r.nb_colis * 100.0 / t.total), 2) AS pourcentage
FROM
  repartition_par_type r
  JOIN total_colis t ON 1=1
ORDER BY
  pourcentage DESC;
