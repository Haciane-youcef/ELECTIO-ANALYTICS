-- ============================================================
-- ETL 01 — RAW → BRONZE
-- Moteur : Dremio + Iceberg (via Nessie)
-- Périmètre : Départements 92, 93, 94
-- ============================================================
-- TRANSFORMATIONS APPLIQUÉES :
--   - Suppression des espaces (TRIM)
--   - Remplacement virgule décimale → point (REPLACE)
--   - Gestion des valeurs 'NA', '', 'S' (secret INSEE) → NULL
--   - CAST vers types métier (DOUBLE, INTEGER)
--   - Filtre géographique 92/93/94
--   - Calcul de ratios (participation, candidats)
--   - Pivot indicateurs criminalité
--   - COALESCE NULL → 0 pour les taux de criminalité
--   - Patch manuel commune 93059 absente
-- ============================================================


-- ============================================================
-- Macro de nettoyage réutilisable (documentation)
-- ============================================================
-- Pour un champ DOUBLE venant de CSV :
--   CAST(REPLACE(NULLIF(NULLIF(TRIM(col),''),'S'),',','.') AS DOUBLE)
--
-- Pour un champ INTEGER venant de CSV :
--   CAST(CAST(NULLIF(NULLIF(TRIM(col),''),'S') AS DOUBLE) AS INTEGER)
--
-- Explication :
--   TRIM         → supprime les espaces avant/après
--   NULLIF(.,'') → '' devient NULL
--   NULLIF(.,'S')→ 'S' (secret statistique INSEE) devient NULL
--   REPLACE(',','.')→ virgule décimale française → point
--   CAST AS DOUBLE  → conversion numérique (NULL propagé si échec)
--   CAST AS INTEGER → double cast pour éviter les erreurs de troncation
-- ============================================================


-- ------------------------------------------------------------
-- B1. BRONZE.vie_associative_commune
--     Source : 3 CSV RNA par département (92, 93, 94)
--     Clé    : adrs_codeinsee (code commune INSEE)
--     Règles : position = 'A' (association active)
--              filtre 92/93/94 sur les 2 premiers chars
-- ------------------------------------------------------------

CREATE TABLE Bronze.vie_associative_commune AS
SELECT
    adrs_codeinsee                             AS code_commune,
    COUNT(*)                                   AS nb_associations,
    CURRENT_TIMESTAMP                          AS date_chargement,
    'rna_waldec_20260306_dpt_92+93+94'         AS source_fichier
FROM (
    SELECT adrs_codeinsee, "position"
    FROM   data."iceberg-datalake"."vie associative"."rna_waldec_20260306_dpt_92.csv"
    UNION ALL
    SELECT adrs_codeinsee, "position"
    FROM   data."iceberg-datalake"."vie associative"."rna_waldec_20260306_dpt_93.csv"
    UNION ALL
    SELECT adrs_codeinsee, "position"
    FROM   data."iceberg-datalake"."vie associative"."rna_waldec_20260306_dpt_94.csv"
) t
WHERE SUBSTR(adrs_codeinsee, 1, 2) IN ('92','93','94')
  AND "position" = 'A'
GROUP BY adrs_codeinsee;


-- ------------------------------------------------------------
-- B2. BRONZE.criminalite_commune  (brut — NULLs conservés)
--     Source : CSV data.gouv sécurité 2025, filtre annee=2022
--     Pivot  : 4 indicateurs → colonnes
--     Note   : taux_pour_mille = 'NA' ou '' → NULL conservé ici
--              (la table _clean remplacera par 0)
-- ------------------------------------------------------------

CREATE TABLE Bronze.criminalite_commune AS
SELECT
    CODGEO_2025                                                                AS code_commune,

    AVG(CASE
        WHEN indicateur = 'Cambriolages de logement'
        THEN CAST(REPLACE(NULLIF(NULLIF(TRIM(taux_pour_mille),''),'NA'),',','.') AS DOUBLE)
    END)                                                                       AS taux_cambriolage,

    AVG(CASE
        WHEN indicateur = 'Violences physiques intrafamiliales'
        THEN CAST(REPLACE(NULLIF(NULLIF(TRIM(taux_pour_mille),''),'NA'),',','.') AS DOUBLE)
    END)                                                                       AS taux_violence_famille,

    AVG(CASE
        WHEN indicateur = 'Violences sexuelles'
        THEN CAST(REPLACE(NULLIF(NULLIF(TRIM(taux_pour_mille),''),'NA'),',','.') AS DOUBLE)
    END)                                                                       AS taux_violence_sexuelle,

    AVG(CASE
        WHEN indicateur = 'Vols avec armes'
        THEN CAST(REPLACE(NULLIF(NULLIF(TRIM(taux_pour_mille),''),'NA'),',','.') AS DOUBLE)
    END)                                                                       AS taux_vols_armes,

    CURRENT_TIMESTAMP                                                          AS date_chargement,
    'donnee-data.gouv-2025-geographie2025-produit-le2026-02-03.csv'           AS source_fichier

FROM data."iceberg-datalake"."sécurité"."donnee-data.gouv-2025-geographie2025-produit-le2026-02-03.csv"
WHERE annee = 2022
  AND SUBSTR(CODGEO_2025, 1, 2) IN ('92','93','94')
GROUP BY CODGEO_2025;

-- Patch : commune 93059 absente du fichier source
INSERT INTO Bronze.criminalite_commune
    (code_commune, taux_cambriolage, taux_violence_famille,
     taux_violence_sexuelle, taux_vols_armes, date_chargement, source_fichier)
VALUES
    ('93059', NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP, 'patch_manuel');


-- ------------------------------------------------------------
-- B3. BRONZE.criminalite_commune_clean  (NULLs → 0)
--     Transformation : COALESCE sur la table brute Bronze
--     Justification  : un taux NULL signifie absence de données
--                      publiées (secret ou commune non couverte),
--                      on utilise 0 comme valeur neutre pour
--                      les agrégats Gold.
-- ------------------------------------------------------------

CREATE TABLE Bronze.criminalite_commune_clean AS
SELECT
    code_commune,
    COALESCE(taux_cambriolage,       0) AS taux_cambriolage,
    COALESCE(taux_violence_famille,  0) AS taux_violence_famille,
    COALESCE(taux_violence_sexuelle, 0) AS taux_violence_sexuelle,
    COALESCE(taux_vols_armes,        0) AS taux_vols_armes,
    date_chargement,
    source_fichier
FROM Bronze.criminalite_commune;


-- ------------------------------------------------------------
-- B4. BRONZE.socio_demo_communes
--     Source : dossier_complet.csv (INSEE RP + Filosofi)
--     Règles :
--       - TRIM + NULLIF '' + NULLIF 'S' (secret INSEE) → NULL
--       - Virgule → point avant CAST DOUBLE
--       - Double CAST (→ DOUBLE → INTEGER) pour les comptages
--     Colonnes : revenus, pauvreté, démographie, chômage,
--                activité, logement
-- ------------------------------------------------------------

CREATE TABLE Bronze.socio_demo_communes AS
SELECT
    CODGEO AS code_commune,

    -- ===== REVENUS & PAUVRETÉ =====
    CAST(REPLACE(NULLIF(NULLIF(TRIM(MED21),       ''), 'S'), ',', '.') AS DOUBLE) AS revenu_median_menages_2021,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(TP6021),      ''), 'S'), ',', '.') AS DOUBLE) AS taux_pauvrete_total_2021,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(TP60AGE121),  ''), 'S'), ',', '.') AS DOUBLE) AS taux_pauvrete_0_14_ans,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(TP60AGE221),  ''), 'S'), ',', '.') AS DOUBLE) AS taux_pauvrete_15_29_ans,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(TP60AGE321),  ''), 'S'), ',', '.') AS DOUBLE) AS taux_pauvrete_30_44_ans,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(TP60AGE421),  ''), 'S'), ',', '.') AS DOUBLE) AS taux_pauvrete_45_59_ans,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(TP60AGE521),  ''), 'S'), ',', '.') AS DOUBLE) AS taux_pauvrete_60_74_ans,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(TP60AGE621),  ''), 'S'), ',', '.') AS DOUBLE) AS taux_pauvrete_75_ans_plus,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(D121),        ''), 'S'), ',', '.') AS DOUBLE) AS revenu_decile_1_menages,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(D921),        ''), 'S'), ',', '.') AS DOUBLE) AS revenu_decile_9_menages,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(PACT21),      ''), 'S'), ',', '.') AS DOUBLE) AS part_revenus_activite,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(PBEN21),      ''), 'S'), ',', '.') AS DOUBLE) AS part_revenus_patrimoine,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(PPAT21),      ''), 'S'), ',', '.') AS DOUBLE) AS part_prestations_familiales,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(PPSOC21),     ''), 'S'), ',', '.') AS DOUBLE) AS part_prestations_sociales,
    CAST(REPLACE(NULLIF(NULLIF(TRIM(PPLOGT21),    ''), 'S'), ',', '.') AS DOUBLE) AS part_aides_logement,

    -- ===== DÉMOGRAPHIE =====
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_POP),     ''), 'S') AS DOUBLE) AS INTEGER) AS population_totale_2022,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_POP0014), ''), 'S') AS DOUBLE) AS INTEGER) AS population_0_14_ans,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_POP1529), ''), 'S') AS DOUBLE) AS INTEGER) AS population_15_29_ans,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_POP3044), ''), 'S') AS DOUBLE) AS INTEGER) AS population_30_44_ans,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_POP4559), ''), 'S') AS DOUBLE) AS INTEGER) AS population_45_59_ans,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_POP6074), ''), 'S') AS DOUBLE) AS INTEGER) AS population_60_74_ans,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_POP7589), ''), 'S') AS DOUBLE) AS INTEGER) AS population_75_89_ans,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_POP90P),  ''), 'S') AS DOUBLE) AS INTEGER) AS population_90_ans_plus,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_POPH),    ''), 'S') AS DOUBLE) AS INTEGER) AS population_hommes,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_POPF),    ''), 'S') AS DOUBLE) AS INTEGER) AS population_femmes,

    -- ===== CHÔMAGE =====
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_CHOM1524), ''), 'S') AS DOUBLE) AS INTEGER) AS chomeurs_15_24_ans,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_CHOM2554), ''), 'S') AS DOUBLE) AS INTEGER) AS chomeurs_25_54_ans,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_CHOM5564), ''), 'S') AS DOUBLE) AS INTEGER) AS chomeurs_55_64_ans,

    -- ===== ACTIFS & EMPLOI =====
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_ACT15P),    ''), 'S') AS DOUBLE) AS INTEGER) AS actifs_15_ans_plus,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_ACTOCC15P), ''), 'S') AS DOUBLE) AS INTEGER) AS actifs_occupes_15_ans_plus,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_SAL15P),    ''), 'S') AS DOUBLE) AS INTEGER) AS nombre_salaries,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_NSAL15P),   ''), 'S') AS DOUBLE) AS INTEGER) AS nombre_non_salaries,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_EMPLT_SAL), ''), 'S') AS DOUBLE) AS INTEGER) AS emplois_salaries,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_EMPLT_NSAL),''), 'S') AS DOUBLE) AS INTEGER) AS emplois_non_salaries,

    -- ===== ACTIVITÉ ÉCONOMIQUE =====
    CAST(CAST(NULLIF(NULLIF(TRIM(ENCTOT24), ''), 'S') AS DOUBLE) AS INTEGER) AS nombre_creations_entreprises_2024,
    CAST(CAST(NULLIF(NULLIF(TRIM(ETNTOT23), ''), 'S') AS DOUBLE) AS INTEGER) AS nombre_total_etablissements_2023,
    CAST(CAST(NULLIF(NULLIF(TRIM(ETPTOT24), ''), 'S') AS DOUBLE) AS INTEGER) AS effectif_total_salarie_2024,

    -- ===== LOGEMENT =====
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_LOG),       ''), 'S') AS DOUBLE) AS INTEGER) AS nombre_total_logements,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_LOGVAC),    ''), 'S') AS DOUBLE) AS INTEGER) AS nombre_logements_vacants,
    CAST(CAST(NULLIF(NULLIF(TRIM(P22_RP_LOCHLMV),''), 'S') AS DOUBLE) AS INTEGER) AS residences_principales_hlm,

    CURRENT_TIMESTAMP  AS date_chargement,
    'dossier_complet.csv' AS source_fichier

FROM data."iceberg-datalake".base."dossier_complet.csv"
WHERE SUBSTR(CODGEO, 1, 2) IN ('92','93','94')
ORDER BY CODGEO;


-- ------------------------------------------------------------
-- B5. BRONZE.elections_communes
--     Source : presidentielle-2022-general-results.csv
--     Règles : SUM agrégé par commune (1 ligne / commune)
--              NULLIF 'NA' → NULL avant CAST
--              Calcul de 6 ratios de participation
-- ------------------------------------------------------------

CREATE TABLE Bronze.elections_communes AS
SELECT
    code_departement,
    libelle_departement,
    code_commune,
    libelle_commune,

    SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(inscrits),    ''), 'NA'), ',', '.') AS DOUBLE)) AS total_inscrits,
    SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(abstentions), ''), 'NA'), ',', '.') AS DOUBLE)) AS total_abstentions,
    SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(votants),     ''), 'NA'), ',', '.') AS DOUBLE)) AS total_votants,
    SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(blancs),      ''), 'NA'), ',', '.') AS DOUBLE)) AS total_blancs,
    SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(nuls),        ''), 'NA'), ',', '.') AS DOUBLE)) AS total_nuls,
    SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(exprimes),    ''), 'NA'), ',', '.') AS DOUBLE)) AS total_exprimes,

    -- Ratio abstentions / inscrits (%)
    CASE
        WHEN SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(inscrits),''),'NA'),',','.') AS DOUBLE)) = 0 THEN 0
        ELSE SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(abstentions),''),'NA'),',','.') AS DOUBLE)) * 100.0
             / SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(inscrits),''),'NA'),',','.') AS DOUBLE))
    END AS ratio_abstentions_inscrits,

    -- Ratio votants / inscrits (taux de participation %)
    CASE
        WHEN SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(inscrits),''),'NA'),',','.') AS DOUBLE)) = 0 THEN 0
        ELSE SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(votants),''),'NA'),',','.') AS DOUBLE)) * 100.0
             / SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(inscrits),''),'NA'),',','.') AS DOUBLE))
    END AS ratio_votants_inscrits,

    -- Ratio blancs / votants (%)
    CASE
        WHEN SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(votants),''),'NA'),',','.') AS DOUBLE)) = 0 THEN 0
        ELSE SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(blancs),''),'NA'),',','.') AS DOUBLE)) * 100.0
             / SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(votants),''),'NA'),',','.') AS DOUBLE))
    END AS ratio_blancs_votants,

    -- Ratio nuls / votants (%)
    CASE
        WHEN SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(votants),''),'NA'),',','.') AS DOUBLE)) = 0 THEN 0
        ELSE SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(nuls),''),'NA'),',','.') AS DOUBLE)) * 100.0
             / SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(votants),''),'NA'),',','.') AS DOUBLE))
    END AS ratio_nuls_votants,

    -- Ratio exprimés / inscrits (%)
    CASE
        WHEN SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(inscrits),''),'NA'),',','.') AS DOUBLE)) = 0 THEN 0
        ELSE SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(exprimes),''),'NA'),',','.') AS DOUBLE)) * 100.0
             / SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(inscrits),''),'NA'),',','.') AS DOUBLE))
    END AS ratio_exprimes_inscrits,

    -- Ratio exprimés / votants (%)
    CASE
        WHEN SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(votants),''),'NA'),',','.') AS DOUBLE)) = 0 THEN 0
        ELSE SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(exprimes),''),'NA'),',','.') AS DOUBLE)) * 100.0
             / SUM(CAST(REPLACE(NULLIF(NULLIF(TRIM(votants),''),'NA'),',','.') AS DOUBLE))
    END AS ratio_exprimes_votants,

    CURRENT_TIMESTAMP AS date_chargement,
    'presidentielle-2022-general-results.csv' AS source_fichier

FROM data."iceberg-datalake".presidentielle."general-results.csv"
WHERE SUBSTR(code_departement, 1, 2) IN ('92','93','94')
GROUP BY code_departement, libelle_departement, code_commune, libelle_commune;


-- ------------------------------------------------------------
-- B6. BRONZE.presidentielle_2022_communes_candidats_pct
--     Source : presidentielle-2022-communes-t1.csv
--     Règles : 1 ligne = 1 candidat × 1 commune
--              voix CAST INTEGER
--              pourcentage = voix / SUM(voix) par commune × 100
-- ------------------------------------------------------------

CREATE TABLE Bronze.presidentielle_2022_communes_candidats_pct AS
SELECT
    code_departement,
    libelle_departement,
    code_commune,
    libelle_commune,
    TRIM(prenom)                                                  AS prenom,
    TRIM(nom)                                                     AS nom,
    CAST(CAST(NULLIF(NULLIF(TRIM(voix),''),'NA') AS DOUBLE) AS INTEGER) AS voix,

    CASE
        WHEN SUM(CAST(CAST(NULLIF(NULLIF(TRIM(voix),''),'NA') AS DOUBLE) AS INTEGER))
             OVER (PARTITION BY code_commune) = 0 THEN 0
        ELSE CAST(CAST(NULLIF(NULLIF(TRIM(voix),''),'NA') AS DOUBLE) AS INTEGER) * 100.0
             / SUM(CAST(CAST(NULLIF(NULLIF(TRIM(voix),''),'NA') AS DOUBLE) AS INTEGER))
               OVER (PARTITION BY code_commune)
    END                                                           AS pourcentage_voix,

    CURRENT_TIMESTAMP AS date_chargement,
    'presidentielle-2022-communes-t1.csv' AS source_fichier

FROM data."iceberg-datalake".presidentielle."presidentielle-2022-communes-t1.csv"
WHERE code_departement IN ('92','93','94')
ORDER BY code_departement, code_commune, voix DESC;