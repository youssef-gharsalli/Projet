--Nettoyage de la base e-mail opt-in : suppression des opt-out

WITH Base_Optin AS (
  SELECT *
  FROM read_files(
    'storage://your-container/clean_data/email_optin_base/',
    format => 'csv',
    header => true,
    mode => 'PERMISSIVE'
  )
),
Base_Optout AS (
  SELECT email AS email_optout
  FROM read_files(
    'storage://your-container/clean_data/email_optout_list/optout_2024.csv',
    format => 'csv',
    sep => ';',
    header => true,
    mode => 'PERMISSIVE'
  )
)
-- On conserve uniquement les e-mails valides, non opt-out
SELECT *
FROM Base_Optin
WHERE TRIM(UPPER(email)) NOT IN (
  SELECT DISTINCT TRIM(UPPER(email_optout))
  FROM Base_Optout
);

/* Nettoyage de base d’emails – Opt-in vs Opt-out

Ce script permet de filtrer une base d’e-mails marketing (`opt-in`) en excluant les contacts ayant explicitement refusé la communication (`opt-out`).  
Les fichiers sont lus depuis un data lake (format CSV), puis comparés de façon sécurisée (case insensitive, sans doublons).

### Objectif :
Préparer une base e-mail propre pour des campagnes de communication respectueuses du RGPD.

### Format :
- Opt-in : `email`
- Opt-out : `email_optout`
- Format fichier : CSV, avec ou sans séparateur personnalisé

