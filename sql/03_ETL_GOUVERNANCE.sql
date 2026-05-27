-- ============================================================
-- ETL 03 — GOUVERNANCE
-- Tables : Gouvernance.META_CHARGEMENT
--          Gouvernance.DATA_QUALITY_LOG
-- Moteur : Dremio + Iceberg
-- ============================================================
-- ORDRE D'EXÉCUTION DANS CE FICHIER :
--   1. Création des 2 tables de gouvernance
--   2. INSERT META_CHARGEMENT ligne IN_PROGRESS (avant ETL)
--   3. Contrôles qualité Bronze (QC1 → QC9)
--   4. Contrôles qualité Datalake.Gold   (QC10 → QC18)
--   5. INSERT META_CHARGEMENT ligne SUCCESS (fin ETL)
-- ============================================================
-- RÈGLES DE STATUT DATA_QUALITY_LOG :
--   PASS  → 0 erreur
--   WARN  → nb_erreurs > 0 mais sous le seuil_fail
--   FAIL  → nb_erreurs dépasse le seuil_fail
-- ============================================================


-- ============================================================
-- 1. CRÉATION DES TABLES DE GOUVERNANCE
-- ============================================================

-- ------------------------------------------------------------
-- Gouvernance.META_CHARGEMENT
--   1 ligne = 1 exécution d'ETL (début + fin)
--   Dremio/Iceberg ne supportant pas UPDATE natif,
--   on insère 2 lignes : IN_PROGRESS puis SUCCESS/FAILED
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Gouvernance.META_CHARGEMENT (
    id_chargement      INT,
    nom_fichier        VARCHAR,
    source_url         VARCHAR,
    date_chargement    TIMESTAMP,
    statut             VARCHAR,         -- IN_PROGRESS | SUCCESS | FAILED
    nb_lignes_source   INT,
    nb_lignes_chargees INT,
    nb_lignes_rejetees INT,
    erreur_message     VARCHAR,
    etl_version        VARCHAR,
    operateur          VARCHAR
);

-- ------------------------------------------------------------
-- Gouvernance.DATA_QUALITY_LOG
--   1 ligne = 1 contrôle qualité appliqué sur 1 colonne
--   fk_chargement référence META_CHARGEMENT.id_chargement
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Gouvernance.DATA_QUALITY_LOG (
    id_check           INT,
    fk_chargement      INT,
    table_name         VARCHAR,
    colonne            VARCHAR,
    regle_qualite      VARCHAR,         -- NOT_NULL | RANGE | COHERENCE | UNIQUE | REFERENTIAL
    nb_enregistrements INT,
    nb_erreurs         INT,
    taux_conformite    DOUBLE,          -- [0..100]
    statut             VARCHAR,         -- PASS | WARN | FAIL
    seuil_warn         DOUBLE,          -- % en dessous duquel → WARN
    seuil_fail         DOUBLE,          -- % en dessous duquel → FAIL
    date_check         TIMESTAMP,
    detail_message     VARCHAR
);


-- ============================================================
-- 2. META_CHARGEMENT — DÉBUT D'ETL  (IN_PROGRESS)
--    À insérer AVANT de lancer les ETL 01 et 02
-- ============================================================

INSERT INTO Gouvernance.META_CHARGEMENT VALUES (
    1,
    'ETL_RAW_TO_BRONZE',
    'internal://data.iceberg-datalake.*',
    CURRENT_TIMESTAMP,
    'IN_PROGRESS',
    NULL, NULL, NULL, NULL,
    'v1.0',
    'ETL01_DREMIO'
);

INSERT INTO Gouvernance.META_CHARGEMENT VALUES (
    2,
    'ETL_BRONZE_TO_Datalake.Gold',
    'internal://Bronze.*',
    CURRENT_TIMESTAMP,
    'IN_PROGRESS',
    NULL, NULL, NULL, NULL,
    'v1.0',
    'ETL02_DREMIO'
);


-- ============================================================
-- 3. CONTRÔLES QUALITÉ — BRONZE
--    À exécuter APRÈS le chargement complet de la zone Bronze
-- ============================================================


-- ------------------------------------------------------------
-- QC1 : NOT NULL — code_commune dans criminalite_commune_clean
-- ------------------------------------------------------------
INSERT INTO Gouvernance.DATA_QUALITY_LOG
SELECT
    1                                                                     AS id_check,
    1                                                                     AS fk_chargement,
    'Bronze.criminalite_commune_clean'                                    AS table_name,
    'code_commune'                                                        AS colonne,
    'NOT_NULL'                                                            AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE WHEN code_commune IS NULL THEN 1 ELSE 0 END)                 AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE WHEN code_commune IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE WHEN code_commune IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        WHEN SUM(CASE WHEN code_commune IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*) < 0.05 THEN 'WARN'
        ELSE 'FAIL'
    END                                                                   AS statut,
    95.0                                                                  AS seuil_warn,
    90.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Vérification NOT NULL sur code_commune'                              AS detail_message
FROM Bronze.criminalite_commune_clean;


-- ------------------------------------------------------------
-- QC2 : RANGE — taux_cambriolage dans [0, 100]
-- ------------------------------------------------------------
INSERT INTO Gouvernance.DATA_QUALITY_LOG
SELECT
    2                                                                     AS id_check,
    1                                                                     AS fk_chargement,
    'Bronze.criminalite_commune_clean'                                    AS table_name,
    'taux_cambriolage'                                                    AS colonne,
    'RANGE[0,100]'                                                        AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE WHEN taux_cambriolage < 0 OR taux_cambriolage > 100 THEN 1 ELSE 0 END) AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE WHEN taux_cambriolage < 0 OR taux_cambriolage > 100 THEN 1 ELSE 0 END)
               * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE WHEN taux_cambriolage < 0 OR taux_cambriolage > 100 THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        WHEN SUM(CASE WHEN taux_cambriolage < 0 OR taux_cambriolage > 100 THEN 1 ELSE 0 END)
             * 1.0 / COUNT(*) < 0.02 THEN 'WARN'
        ELSE 'FAIL'
    END                                                                   AS statut,
    98.0                                                                  AS seuil_warn,
    95.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Taux cambriolage hors intervalle [0,100]'                            AS detail_message
FROM Bronze.criminalite_commune_clean;


-- ------------------------------------------------------------
-- QC3 : RANGE — tous les taux de criminalité >= 0
--        (taux_violence_famille, sexuelle, vols_armes)
-- ------------------------------------------------------------
INSERT INTO Gouvernance.DATA_QUALITY_LOG
SELECT
    3                                                                     AS id_check,
    1                                                                     AS fk_chargement,
    'Bronze.criminalite_commune_clean'                                    AS table_name,
    'taux_violence_famille | taux_violence_sexuelle | taux_vols_armes'    AS colonne,
    'RANGE[>=0]'                                                          AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE
        WHEN taux_violence_famille  < 0
          OR taux_violence_sexuelle < 0
          OR taux_vols_armes        < 0
        THEN 1 ELSE 0
    END)                                                                  AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE
             WHEN taux_violence_famille  < 0
               OR taux_violence_sexuelle < 0
               OR taux_vols_armes        < 0
             THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE
            WHEN taux_violence_famille  < 0
              OR taux_violence_sexuelle < 0
              OR taux_vols_armes        < 0
            THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                                                   AS statut,
    99.0                                                                  AS seuil_warn,
    95.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Taux de criminalité négatifs détectés'                               AS detail_message
FROM Bronze.criminalite_commune_clean;


-- ------------------------------------------------------------
-- QC4 : NOT NULL — code_commune dans socio_demo_communes
-- ------------------------------------------------------------
INSERT INTO Gouvernance.DATA_QUALITY_LOG
SELECT
    4                                                                     AS id_check,
    1                                                                     AS fk_chargement,
    'Bronze.socio_demo_communes'                                          AS table_name,
    'code_commune'                                                        AS colonne,
    'NOT_NULL'                                                            AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE WHEN code_commune IS NULL THEN 1 ELSE 0 END)                 AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE WHEN code_commune IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE WHEN code_commune IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                                                   AS statut,
    99.0                                                                  AS seuil_warn,
    95.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Vérification NOT NULL code_commune socio_demo'                       AS detail_message
FROM Bronze.socio_demo_communes;


-- ------------------------------------------------------------
-- QC5 : RANGE — population_totale_2022 > 0
-- ------------------------------------------------------------
INSERT INTO Gouvernance.DATA_QUALITY_LOG
SELECT
    5                                                                     AS id_check,
    1                                                                     AS fk_chargement,
    'Bronze.socio_demo_communes'                                          AS table_name,
    'population_totale_2022'                                              AS colonne,
    'RANGE[>0]'                                                           AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE WHEN population_totale_2022 IS NULL OR population_totale_2022 <= 0 THEN 1 ELSE 0 END) AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE WHEN population_totale_2022 IS NULL OR population_totale_2022 <= 0 THEN 1 ELSE 0 END)
               * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE WHEN population_totale_2022 IS NULL OR population_totale_2022 <= 0 THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        WHEN SUM(CASE WHEN population_totale_2022 IS NULL OR population_totale_2022 <= 0 THEN 1 ELSE 0 END)
             * 1.0 / COUNT(*) < 0.02 THEN 'WARN'
        ELSE 'FAIL'
    END                                                                   AS statut,
    98.0                                                                  AS seuil_warn,
    95.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Population totale nulle ou négative'                                 AS detail_message
FROM Bronze.socio_demo_communes;


-- ------------------------------------------------------------
-- QC6 : COHÉRENCE — population hommes + femmes = population totale
--        (tolérance ±1 pour arrondis INSEE)
-- ------------------------------------------------------------
INSERT INTO Gouvernance.DATA_QUALITY_LOG
SELECT
    6                                                                     AS id_check,
    1                                                                     AS fk_chargement,
    'Bronze.socio_demo_communes'                                          AS table_name,
    'population_hommes + population_femmes vs population_totale_2022'     AS colonne,
    'COHERENCE[H+F=TOT±1]'                                               AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE
        WHEN ABS((COALESCE(population_hommes, 0) + COALESCE(population_femmes, 0))
                  - COALESCE(population_totale_2022, 0)) > 1
        THEN 1 ELSE 0
    END)                                                                  AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE
             WHEN ABS((COALESCE(population_hommes, 0) + COALESCE(population_femmes, 0))
                       - COALESCE(population_totale_2022, 0)) > 1
             THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE
            WHEN ABS((COALESCE(population_hommes, 0) + COALESCE(population_femmes, 0))
                      - COALESCE(population_totale_2022, 0)) > 1
            THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        WHEN SUM(CASE
            WHEN ABS((COALESCE(population_hommes, 0) + COALESCE(population_femmes, 0))
                      - COALESCE(population_totale_2022, 0)) > 1
            THEN 1 ELSE 0 END) * 1.0 / COUNT(*) < 0.05 THEN 'WARN'
        ELSE 'FAIL'
    END                                                                   AS statut,
    95.0                                                                  AS seuil_warn,
    90.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Somme H+F doit être égale à population totale (±1)'                  AS detail_message
FROM Bronze.socio_demo_communes;


-- ------------------------------------------------------------
-- QC7 : NOT NULL — code_commune dans elections_communes
-- ------------------------------------------------------------
INSERT INTO Gouvernance.DATA_QUALITY_LOG
SELECT
    7                                                                     AS id_check,
    1                                                                     AS fk_chargement,
    'Bronze.elections_communes'                                           AS table_name,
    'code_commune'                                                        AS colonne,
    'NOT_NULL'                                                            AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE WHEN code_commune IS NULL THEN 1 ELSE 0 END)                 AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE WHEN code_commune IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE WHEN code_commune IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                                                   AS statut,
    99.0                                                                  AS seuil_warn,
    95.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Vérification NOT NULL code_commune elections_communes'               AS detail_message
FROM Bronze.elections_communes;


-- ------------------------------------------------------------
-- QC8 : RANGE — total_inscrits > 0
-- ------------------------------------------------------------
INSERT INTO Gouvernance.DATA_QUALITY_LOG
SELECT
    8                                                                     AS id_check,
    1                                                                     AS fk_chargement,
    'Bronze.elections_communes'                                           AS table_name,
    'total_inscrits'                                                      AS colonne,
    'RANGE[>0]'                                                           AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE WHEN total_inscrits IS NULL OR total_inscrits <= 0 THEN 1 ELSE 0 END) AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE WHEN total_inscrits IS NULL OR total_inscrits <= 0 THEN 1 ELSE 0 END)
               * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE WHEN total_inscrits IS NULL OR total_inscrits <= 0 THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        WHEN SUM(CASE WHEN total_inscrits IS NULL OR total_inscrits <= 0 THEN 1 ELSE 0 END)
             * 1.0 / COUNT(*) < 0.02 THEN 'WARN'
        ELSE 'FAIL'
    END                                                                   AS statut,
    98.0                                                                  AS seuil_warn,
    95.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Communes sans inscrits détectées'                                    AS detail_message
FROM Bronze.elections_communes;


-- ------------------------------------------------------------
-- QC9 : COHÉRENCE — total_votants <= total_inscrits
-- ------------------------------------------------------------
INSERT INTO Gouvernance.DATA_QUALITY_LOG
SELECT
    9                                                                     AS id_check,
    1                                                                     AS fk_chargement,
    'Bronze.elections_communes'                                           AS table_name,
    'total_votants <= total_inscrits'                                     AS colonne,
    'COHERENCE[votants<=inscrits]'                                        AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE WHEN total_votants > total_inscrits THEN 1 ELSE 0 END)       AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE WHEN total_votants > total_inscrits THEN 1 ELSE 0 END)
               * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE WHEN total_votants > total_inscrits THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                                                   AS statut,
    99.0                                                                  AS seuil_warn,
    95.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Votants supérieurs aux inscrits — incohérence source'                AS detail_message
FROM Bronze.elections_communes;


-- ============================================================
-- 4. CONTRÔLES QUALITÉ — Datalake.Gold
--    À exécuter APRÈS le chargement complet de la zone Datalake.Gold
-- ============================================================


-- ------------------------------------------------------------
-- QC10 : NOT NULL — nb_voix dans FACT_RESULTATS_ELECTORAUX
-- ------------------------------------------------------------
INSERT INTO Datalake.Gouvernance.DATA_QUALITY_LOG
SELECT
    10                                                                    AS id_check,
    2                                                                     AS fk_chargement,
    'Datalake.Gold.FACT_RESULTATS_ELECTORAUX'                                      AS table_name,
    'nb_voix'                                                             AS colonne,
    'NOT_NULL'                                                            AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE WHEN nb_voix IS NULL THEN 1 ELSE 0 END)                      AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE WHEN nb_voix IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE WHEN nb_voix IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'WARN'
    END                                                                   AS statut,
    99.0                                                                  AS seuil_warn,
    95.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Vérification NOT NULL nb_voix dans FACT_RESULTATS'                   AS detail_message
FROM Datalake.Gold.FACT_RESULTATS_ELECTORAUX;


-- ------------------------------------------------------------
-- QC11 : RANGE — ratio_voix_exprimes dans [0, 1]
-- ------------------------------------------------------------
INSERT INTO Datalake.Gouvernance.DATA_QUALITY_LOG
SELECT
    11                                                                    AS id_check,
    2                                                                     AS fk_chargement,
    'Datalake.Gold.FACT_RESULTATS_ELECTORAUX'                                      AS table_name,
    'ratio_voix_exprimes'                                                 AS colonne,
    'RANGE[0,1]'                                                          AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE WHEN ratio_voix_exprimes < 0 OR ratio_voix_exprimes > 1 THEN 1 ELSE 0 END) AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE WHEN ratio_voix_exprimes < 0 OR ratio_voix_exprimes > 1 THEN 1 ELSE 0 END)
               * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE WHEN ratio_voix_exprimes < 0 OR ratio_voix_exprimes > 1 THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                                                   AS statut,
    99.0                                                                  AS seuil_warn,
    95.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Ratio voix exprimées hors [0,1]'                                     AS detail_message
FROM Datalake.Gold.FACT_RESULTATS_ELECTORAUX;


-- ------------------------------------------------------------
-- QC12 : RÉFÉRENTIEL — fk_candidat dans DIM_CANDIDAT
--         Détecte les candidats non mappés (fk_candidat = 0)
-- ------------------------------------------------------------
INSERT INTO Datalake.Gouvernance.DATA_QUALITY_LOG
SELECT
    12                                                                    AS id_check,
    2                                                                     AS fk_chargement,
    'Datalake.Gold.FACT_RESULTATS_ELECTORAUX'                                      AS table_name,
    'fk_candidat'                                                         AS colonne,
    'REFERENTIAL[DIM_CANDIDAT]'                                           AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE WHEN fk_candidat = 0 THEN 1 ELSE 0 END)                      AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE WHEN fk_candidat = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE WHEN fk_candidat = 0 THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        WHEN SUM(CASE WHEN fk_candidat = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) < 0.05 THEN 'WARN'
        ELSE 'FAIL'
    END                                                                   AS statut,
    95.0                                                                  AS seuil_warn,
    90.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Lignes avec fk_candidat=0 : candidat non résolu dans DIM_CANDIDAT'   AS detail_message
FROM Datalake.Gold.FACT_RESULTATS_ELECTORAUX;


-- ------------------------------------------------------------
-- QC13 : COHÉRENCE — somme taux_participation + taux_abstention ≈ 1
--         (tolérance ±0.01)
-- ------------------------------------------------------------
INSERT INTO Datalake.Gouvernance.DATA_QUALITY_LOG
SELECT
    13                                                                    AS id_check,
    2                                                                     AS fk_chargement,
    'Datalake.Gold.FACT_PARTICIPATION_ELECTORALE'                                  AS table_name,
    'taux_participation + taux_abstention'                                AS colonne,
    'COHERENCE[SUM≈1±0.01]'                                              AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE
        WHEN ABS((taux_participation + taux_abstention) - 1.0) > 0.01
        THEN 1 ELSE 0
    END)                                                                  AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE
             WHEN ABS((taux_participation + taux_abstention) - 1.0) > 0.01
             THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE
            WHEN ABS((taux_participation + taux_abstention) - 1.0) > 0.01
            THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'WARN'
    END                                                                   AS statut,
    98.0                                                                  AS seuil_warn,
    90.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Participation + abstention devrait être ≈ 1.0 (±0.01)'              AS detail_message
FROM Datalake.Gold.FACT_PARTICIPATION_ELECTORALE;


-- ------------------------------------------------------------
-- QC14 : RANGE — taux_participation dans [0, 1]
-- ------------------------------------------------------------
INSERT INTO Datalake.Gouvernance.DATA_QUALITY_LOG
SELECT
    14                                                                    AS id_check,
    2                                                                     AS fk_chargement,
    'Datalake.Gold.FACT_PARTICIPATION_ELECTORALE'                                  AS table_name,
    'taux_participation'                                                  AS colonne,
    'RANGE[0,1]'                                                          AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE WHEN taux_participation < 0 OR taux_participation > 1 THEN 1 ELSE 0 END) AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE WHEN taux_participation < 0 OR taux_participation > 1 THEN 1 ELSE 0 END)
               * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE WHEN taux_participation < 0 OR taux_participation > 1 THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                                                   AS statut,
    99.0                                                                  AS seuil_warn,
    95.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Taux participation hors [0,1] — division /100 manquante ?'           AS detail_message
FROM Datalake.Gold.FACT_PARTICIPATION_ELECTORALE;


-- ------------------------------------------------------------
-- QC15 : NOT NULL — fk_commune dans toutes les tables de faits
--         (vérification sur FACT_REVENU comme représentative)
-- ------------------------------------------------------------
INSERT INTO Datalake.Gouvernance.DATA_QUALITY_LOG
SELECT
    15                                                                    AS id_check,
    2                                                                     AS fk_chargement,
    'Datalake.Gold.FACT_REVENU'                                                    AS table_name,
    'fk_commune'                                                          AS colonne,
    'NOT_NULL'                                                            AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE WHEN fk_commune IS NULL THEN 1 ELSE 0 END)                   AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE WHEN fk_commune IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE WHEN fk_commune IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                                                   AS statut,
    99.0                                                                  AS seuil_warn,
    95.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Vérification NOT NULL fk_commune dans FACT_REVENU'                   AS detail_message
FROM Datalake.Gold.FACT_REVENU;


-- ------------------------------------------------------------
-- QC16 : RANGE — revenu_median_menages > 0 (quand non NULL)
-- ------------------------------------------------------------
INSERT INTO Datalake.Gouvernance.DATA_QUALITY_LOG
SELECT
    16                                                                    AS id_check,
    2                                                                     AS fk_chargement,
    'Datalake.Gold.FACT_REVENU'                                                    AS table_name,
    'revenu_median_menages'                                               AS colonne,
    'RANGE[>0 si non NULL]'                                               AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE WHEN revenu_median_menages IS NOT NULL
              AND revenu_median_menages <= 0 THEN 1 ELSE 0 END)           AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE WHEN revenu_median_menages IS NOT NULL
                               AND revenu_median_menages <= 0 THEN 1 ELSE 0 END)
               * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE WHEN revenu_median_menages IS NOT NULL
                       AND revenu_median_menages <= 0 THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'WARN'
    END                                                                   AS statut,
    98.0                                                                  AS seuil_warn,
    95.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Revenu médian nul ou négatif (hors secrets INSEE)'                   AS detail_message
FROM Datalake.Gold.FACT_REVENU;


-- ------------------------------------------------------------
-- QC17 : COHÉRENCE — DIM_COMMUNE : unicité code_commune is_current
--         Chaque code_commune ne doit avoir qu'une version active
-- ------------------------------------------------------------
INSERT INTO Datalake.Gouvernance.DATA_QUALITY_LOG
SELECT
    17                                                                    AS id_check,
    2                                                                     AS fk_chargement,
    'Datalake.Gold.DIM_COMMUNE'                                                    AS table_name,
    'code_commune WHERE is_current=TRUE'                                  AS colonne,
    'UNIQUE[code_commune, is_current]'                                    AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    COUNT(*) - COUNT(DISTINCT code_commune)                               AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - (COUNT(*) - COUNT(DISTINCT code_commune)) * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN COUNT(*) - COUNT(DISTINCT code_commune) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                                                   AS statut,
    99.0                                                                  AS seuil_warn,
    99.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Doublons SCD2 : plusieurs lignes is_current=TRUE pour un même code'  AS detail_message
FROM Datalake.Gold.DIM_COMMUNE
WHERE is_current = TRUE;


-- ------------------------------------------------------------
-- QC18 : RANGE — taux_chomage dans [0, 100] (quand non NULL)
-- ------------------------------------------------------------
INSERT INTO Datalake.Gouvernance.DATA_QUALITY_LOG
SELECT
    18                                                                    AS id_check,
    2                                                                     AS fk_chargement,
    'Datalake.Gold.FACT_ECONOMIE_LOCALE'                                           AS table_name,
    'taux_chomage'                                                        AS colonne,
    'RANGE[0,100 si non NULL]'                                            AS regle_qualite,
    COUNT(*)                                                              AS nb_enregistrements,
    SUM(CASE
        WHEN taux_chomage IS NOT NULL
         AND (taux_chomage < 0 OR taux_chomage > 100)
        THEN 1 ELSE 0
    END)                                                                  AS nb_erreurs,
    CASE WHEN COUNT(*) = 0 THEN 100.0
         ELSE (1.0 - SUM(CASE
             WHEN taux_chomage IS NOT NULL
              AND (taux_chomage < 0 OR taux_chomage > 100)
             THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
    END                                                                   AS taux_conformite,
    CASE
        WHEN SUM(CASE
            WHEN taux_chomage IS NOT NULL
             AND (taux_chomage < 0 OR taux_chomage > 100)
            THEN 1 ELSE 0 END) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END                                                                   AS statut,
    99.0                                                                  AS seuil_warn,
    95.0                                                                  AS seuil_fail,
    CURRENT_TIMESTAMP                                                     AS date_check,
    'Taux chômage hors [0,100] — erreur de calcul actifs ?'               AS detail_message
FROM Datalake.Gold.FACT_ECONOMIE_LOCALE;


-- ============================================================
-- 5. META_CHARGEMENT — FIN D'ETL  (SUCCESS)
--    À insérer APRÈS exécution complète des ETL 01 et 02
--    et APRÈS tous les QC ci-dessus.
--    Dremio Iceberg : pas d'UPDATE natif → 2ème ligne avec SUCCESS
-- ============================================================

INSERT INTO Datalake.Gouvernance.META_CHARGEMENT VALUES (
    3,
    'ETL_RAW_TO_BRONZE_COMPLETED',
    'internal://Bronze.*',
    CURRENT_TIMESTAMP,
    'SUCCESS',
    NULL,
    NULL,
    0,
    NULL,
    'v1.0',
    'ETL01_DREMIO'
);

INSERT INTO Datalake.Gouvernance.META_CHARGEMENT VALUES (
    4,
    'ETL_BRONZE_TO_Datalake.Gold_COMPLETED',
    'internal://Datalake.Gold.*',
    CURRENT_TIMESTAMP,
    'SUCCESS',
    NULL,
    NULL,
    0,
    NULL,
    'v1.0',
    'ETL02_DREMIO'
);


-- ============================================================
-- REQUÊTES DE CONTRÔLE — à exécuter après l'ETL 03
-- ============================================================

-- Vue synthétique des résultats qualité
-- SELECT
--     table_name,
--     colonne,
--     regle_qualite,
--     nb_enregistrements,
--     nb_erreurs,
--     ROUND(taux_conformite, 2) AS taux_conformite_pct,
--     statut,
--     date_check
-- FROM Datalake.Gouvernance.DATA_QUALITY_LOG
-- ORDER BY statut DESC, taux_conformite ASC;

-- Nombre de FAIL actifs
-- SELECT COUNT(*) AS nb_fail
-- FROM Datalake.Gouvernance.DATA_QUALITY_LOG
-- WHERE statut = 'FAIL';

-- Historique des chargements
-- SELECT id_chargement, nom_fichier, statut, date_chargement, operateur
-- FROM Datalake.Gouvernance.META_CHARGEMENT
-- ORDER BY date_chargement DESC;
