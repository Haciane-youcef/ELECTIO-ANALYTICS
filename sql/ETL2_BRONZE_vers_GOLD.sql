-- ============================================================
-- ETL 02 — BRONZE → GOLD  (modélisation dimensionnelle DWH)
-- Moteur : Dremio + Iceberg
-- ============================================================
-- TRANSFORMATIONS APPLIQUÉES :
--   - Modélisation en étoile (dimensions + faits)
--   - SCD2 sur DIM_COMMUNE et DIM_CANDIDAT (date_debut/fin + is_current)
--   - Clés de substitution (id_commune, id_candidat…) via ROW_NUMBER
--   - Jointures Bronze → Gold avec filtre is_current = TRUE
--   - Calcul taux_chomage en Gold (actifs > 0 guard)
--   - Ratios participation divisés par 100 (Bronze = %, Gold = [0..1])
--   - Enrichissement département / région calculé depuis code_commune
--   - FACT_EDUCATION initialisée vide (source non encore disponible)
-- ============================================================


-- ============================================================
-- DIMENSIONS
-- ============================================================


-- ------------------------------------------------------------
-- G1. GOLD.DIM_TEMPS
--     Peuplée manuellement pour les années référencées dans
--     les tables de faits (2021–2024)
-- ------------------------------------------------------------

CREATE TABLE Gold.DIM_TEMPS (
    id_temps         INT,
    date_reference   DATE,
    annee            INT,
    mois             INT,
    trimestre        INT,
    semestre         INT,
    annee_electorale BOOLEAN
);

INSERT INTO Gold.DIM_TEMPS VALUES
    (1, DATE '2022-01-01', 2022, 1,  1, 1, TRUE),
    (2, DATE '2022-04-10', 2022, 4,  2, 1, TRUE),   -- date présidentielle T1
    (3, DATE '2022-06-30', 2022, 6,  2, 1, TRUE),
    (4, DATE '2022-12-31', 2022, 12, 4, 2, TRUE),
    (5, DATE '2021-12-31', 2021, 12, 4, 2, FALSE),  -- référence Filosofi 2021
    (6, DATE '2023-12-31', 2023, 12, 4, 2, FALSE),  -- référence établissements 2023
    (7, DATE '2024-12-31', 2024, 12, 4, 2, FALSE);  -- référence créations 2024


-- ------------------------------------------------------------
-- G2. GOLD.DIM_COMMUNE  (SCD2 — snapshot initial)
--     Source     : Bronze.socio_demo_communes (liste exhaustive)
--     Enrichi    : libelle_commune depuis Bronze.elections_communes
--     Clé subst. : id_commune via ROW_NUMBER()
--     SCD2       : date_debut_validite = NOW, date_fin_validite = NULL
--                  is_current = TRUE pour toutes les lignes initiales
-- ------------------------------------------------------------

CREATE TABLE Gold.DIM_COMMUNE AS
SELECT
    ROW_NUMBER() OVER (ORDER BY s.code_commune)  AS id_commune,
    s.code_commune,
    COALESCE(e.libelle_commune, s.code_commune)  AS nom_commune,
    NULL                                          AS code_postal,

    SUBSTR(s.code_commune, 1, 2)                 AS code_departement,
    CASE SUBSTR(s.code_commune, 1, 2)
        WHEN '92' THEN 'Hauts-de-Seine'
        WHEN '93' THEN 'Seine-Saint-Denis'
        WHEN '94' THEN 'Val-de-Marne'
        ELSE 'Inconnu'
    END                                           AS nom_departement,
    '11'                                          AS code_region,
    'Île-de-France'                               AS nom_region,

    NULL                                          AS superficie_km2,
    'urbain'                                      AS zone_urbaine,
    NULL                                          AS code_epci,

    CURRENT_TIMESTAMP                             AS date_debut_validite,
    NULL                                          AS date_fin_validite,
    TRUE                                          AS is_current

FROM Bronze.socio_demo_communes s
LEFT JOIN (
    SELECT DISTINCT code_commune, libelle_commune
    FROM   Bronze.elections_communes
) e ON s.code_commune = e.code_commune;


-- ------------------------------------------------------------
-- G3. GOLD.DIM_PARTI_POLITIQUE
--     Référentiel des 12 candidats présidentielle 2022 + NA
-- ------------------------------------------------------------

CREATE TABLE Gold.DIM_PARTI_POLITIQUE (
    id_parti            INT,
    nom_parti           VARCHAR,
    sigle_parti         VARCHAR,
    famille_politique   VARCHAR,
    couleur_hex         VARCHAR,
    date_debut_validite TIMESTAMP,
    date_fin_validite   TIMESTAMP,
    is_current          BOOLEAN
);

INSERT INTO Gold.DIM_PARTI_POLITIQUE VALUES
    (1,  'La République En Marche',       'LREM', 'Centre',         '#FF8C00', CURRENT_TIMESTAMP, NULL, TRUE),
    (2,  'Rassemblement National',         'RN',   'Droite radicale','#0D1E8C', CURRENT_TIMESTAMP, NULL, TRUE),
    (3,  'La France Insoumise',            'LFI',  'Gauche radicale','#CC2443', CURRENT_TIMESTAMP, NULL, TRUE),
    (4,  'Les Républicains',              'LR',   'Droite',         '#0066CC', CURRENT_TIMESTAMP, NULL, TRUE),
    (5,  'Reconquête',                    'REC',  'Droite radicale','#00BFFF', CURRENT_TIMESTAMP, NULL, TRUE),
    (6,  'Parti Socialiste',              'PS',   'Gauche',         '#FF69B4', CURRENT_TIMESTAMP, NULL, TRUE),
    (7,  'Europe Écologie Les Verts',     'EELV', 'Gauche écolo',   '#27AE60', CURRENT_TIMESTAMP, NULL, TRUE),
    (8,  'Parti Communiste Français',     'PCF',  'Gauche radicale','#CC0000', CURRENT_TIMESTAMP, NULL, TRUE),
    (9,  'Debout la France',             'DLF',  'Droite souv.',   '#8B4513', CURRENT_TIMESTAMP, NULL, TRUE),
    (10, 'Union Populaire Républicaine',  'UPR',  'Droite souv.',   '#483D8B', CURRENT_TIMESTAMP, NULL, TRUE),
    (11, 'Lutte Ouvrière',               'LO',   'Extrême gauche', '#FF4500', CURRENT_TIMESTAMP, NULL, TRUE),
    (12, 'Nouveau Parti Anticapitaliste', 'NPA',  'Extrême gauche', '#8B0000', CURRENT_TIMESTAMP, NULL, TRUE),
    (99, 'Non applicable',               'NA',   'NA',             '#CCCCCC', CURRENT_TIMESTAMP, NULL, TRUE);


-- ------------------------------------------------------------
-- G4. GOLD.DIM_CANDIDAT  (SCD2 — snapshot initial)
--     Correspondance prenom+nom → parti
--     fk_parti référence Gold.DIM_PARTI_POLITIQUE.id_parti
-- ------------------------------------------------------------

CREATE TABLE Gold.DIM_CANDIDAT (
    id_candidat         INT,
    prenom              VARCHAR,
    nom                 VARCHAR,
    fk_parti            INT,
    date_debut_validite TIMESTAMP,
    date_fin_validite   TIMESTAMP,
    is_current          BOOLEAN
);

INSERT INTO Gold.DIM_CANDIDAT VALUES
    (1,  'Emmanuel', 'MACRON',          1,  CURRENT_TIMESTAMP, NULL, TRUE),
    (2,  'Marine',   'LE PEN',          2,  CURRENT_TIMESTAMP, NULL, TRUE),
    (3,  'Jean-Luc', 'MÉLENCHON',       3,  CURRENT_TIMESTAMP, NULL, TRUE),
    (4,  'Éric',     'ZEMMOUR',         5,  CURRENT_TIMESTAMP, NULL, TRUE),
    (5,  'Valérie',  'PÉCRESSE',        4,  CURRENT_TIMESTAMP, NULL, TRUE),
    (6,  'Yannick',  'JADOT',           7,  CURRENT_TIMESTAMP, NULL, TRUE),
    (7,  'Jean',     'LASSALLE',        9,  CURRENT_TIMESTAMP, NULL, TRUE),
    (8,  'Fabien',   'ROUSSEL',         8,  CURRENT_TIMESTAMP, NULL, TRUE),
    (9,  'Anne',     'HIDALGO',         6,  CURRENT_TIMESTAMP, NULL, TRUE),
    (10, 'Nicolas',  'DUPONT-AIGNAN',   9,  CURRENT_TIMESTAMP, NULL, TRUE),
    (11, 'Philippe', 'POUTOU',          12, CURRENT_TIMESTAMP, NULL, TRUE),
    (12, 'Nathalie', 'ARTHAUD',         11, CURRENT_TIMESTAMP, NULL, TRUE),
    (0,  'NON',      'APPLICABLE',      99, CURRENT_TIMESTAMP, NULL, TRUE);


-- ------------------------------------------------------------
-- G5. GOLD.DIM_ELECTION
--     1 ligne = présidentielle 2022 T1
--     fk_temps = 2 → DATE '2022-04-10'
-- ------------------------------------------------------------

CREATE TABLE Gold.DIM_ELECTION (
    id_election       INT,
    fk_temps          INT,
    type_election     VARCHAR,
    tour              INT,
    zone_geographique VARCHAR,
    annee_election    INT
);

INSERT INTO Gold.DIM_ELECTION VALUES
    (1, 2, 'présidentielle', 1, 'nationale', 2022);


-- ------------------------------------------------------------
-- G6. GOLD.DIM_TYPE_RESULTAT_VOTE
-- ------------------------------------------------------------

CREATE TABLE Gold.DIM_TYPE_RESULTAT_VOTE (
    id_type_resultat INT,
    code             VARCHAR,
    libelle          VARCHAR
);

INSERT INTO Gold.DIM_TYPE_RESULTAT_VOTE VALUES
    (1, 'CANDIDAT', 'Vote pour un candidat'),
    (2, 'BLANC',    'Vote blanc'),
    (3, 'NUL',      'Vote nul');


-- ------------------------------------------------------------
-- G7. GOLD.DIM_TYPE_PARTICIPATION
-- ------------------------------------------------------------

CREATE TABLE Gold.DIM_TYPE_PARTICIPATION (
    id_type_participation INT,
    code                  VARCHAR,
    libelle               VARCHAR
);

INSERT INTO Gold.DIM_TYPE_PARTICIPATION VALUES
    (1, 'VOTANT',     'Électeur ayant voté'),
    (2, 'ABSTENTION', 'Électeur abstentionniste');


-- ============================================================
-- TABLES DE FAITS
-- ============================================================


-- ------------------------------------------------------------
-- F1. GOLD.FACT_RESULTATS_ELECTORAUX
--     Granularité : 1 ligne = 1 candidat × 1 commune
--     Source      : Bronze.presidentielle_2022_communes_candidats_pct
--     Jointures   : DIM_COMMUNE (code_commune, is_current)
--                   DIM_CANDIDAT (UPPER TRIM nom, is_current)
--     Mesures     : nb_voix, ratio_voix_exprimes [0..1], rang_commune
-- ------------------------------------------------------------

CREATE TABLE Gold.FACT_RESULTATS_ELECTORAUX AS
SELECT
    ROW_NUMBER() OVER (ORDER BY b.code_commune, dc.id_candidat)  AS id_fait,

    c.id_commune                                                   AS fk_commune,
    1                                                              AS fk_election,
    COALESCE(dc.id_candidat, 0)                                    AS fk_candidat,
    1                                                              AS fk_type_resultat,

    CAST(b.voix AS INTEGER)                                        AS nb_voix,
    CAST(b.pourcentage_voix / 100.0 AS DOUBLE)                     AS ratio_voix_exprimes,

    NULL                                                           AS delta_voix_vs_precedent,
    NULL                                                           AS delta_ratio_vs_precedent,

    RANK() OVER (PARTITION BY b.code_commune ORDER BY b.voix DESC) AS rang_commune,

    1              AS fk_chargement,
    b.source_fichier,
    CURRENT_TIMESTAMP AS date_chargement

FROM Bronze.presidentielle_2022_communes_candidats_pct b

JOIN Gold.DIM_COMMUNE c
    ON b.code_commune = c.code_commune
    AND c.is_current  = TRUE

LEFT JOIN Gold.DIM_CANDIDAT dc
    ON UPPER(TRIM(b.nom)) = UPPER(TRIM(dc.nom))
    AND dc.is_current = TRUE;


-- ------------------------------------------------------------
-- F2. GOLD.FACT_PARTICIPATION_ELECTORALE
--     Granularité : 1 ligne = 1 commune
--     Source      : Bronze.elections_communes
--     Mesures     : comptages bruts + taux [0..1]
--     Note        : Bronze stocke les ratios en %, Gold divise /100
-- ------------------------------------------------------------

CREATE TABLE Gold.FACT_PARTICIPATION_ELECTORALE AS
SELECT
    ROW_NUMBER() OVER (ORDER BY b.code_commune) AS id_fait,

    c.id_commune                                AS fk_commune,
    1                                           AS fk_election,
    1                                           AS fk_type_participation,

    CAST(b.total_inscrits    AS INTEGER)        AS total_inscrits,
    CAST(b.total_votants     AS INTEGER)        AS total_votants,
    CAST(b.total_exprimes    AS INTEGER)        AS total_exprimes,
    CAST(b.total_abstentions AS INTEGER)        AS nb_abstentions,
    CAST(b.total_blancs      AS INTEGER)        AS nb_votes_blancs,
    CAST(b.total_nuls        AS INTEGER)        AS nb_votes_nuls,

    CAST(b.ratio_votants_inscrits     / 100.0 AS DOUBLE) AS taux_participation,
    CAST(b.ratio_abstentions_inscrits / 100.0 AS DOUBLE) AS taux_abstention,

    NULL AS delta_participation_vs_precedent,
    NULL AS delta_abstention_vs_precedent,

    1              AS fk_chargement,
    b.source_fichier,
    CURRENT_TIMESTAMP AS date_chargement

FROM Bronze.elections_communes b
JOIN Gold.DIM_COMMUNE c
    ON b.code_commune = c.code_commune
    AND c.is_current  = TRUE;


-- ------------------------------------------------------------
-- F3. GOLD.FACT_POPULATION
--     Granularité : 1 ligne = 1 commune
--     Source      : Bronze.socio_demo_communes
--     fk_temps = 5 → référence RP 2022 (année collecte 2021–2022)
-- ------------------------------------------------------------

CREATE TABLE Gold.FACT_POPULATION AS
SELECT
    ROW_NUMBER() OVER (ORDER BY s.code_commune) AS id_fait,

    c.id_commune AS fk_commune,
    5            AS fk_temps,

    s.population_totale_2022 AS population_totale,
    s.population_0_14_ans,
    s.population_15_29_ans,
    s.population_30_44_ans,
    s.population_45_59_ans,
    s.population_60_74_ans,
    s.population_75_89_ans   AS population_75_plus,
    s.population_hommes,
    s.population_femmes,

    NULL AS densite_population,   -- superficie_km2 non disponible dans RAW
    NULL AS age_median,
    NULL AS part_immigres,
    NULL AS part_etrangers,

    1              AS fk_chargement,
    s.source_fichier,
    CURRENT_TIMESTAMP AS date_chargement

FROM Bronze.socio_demo_communes s
JOIN Gold.DIM_COMMUNE c
    ON s.code_commune = c.code_commune
    AND c.is_current  = TRUE;


-- ------------------------------------------------------------
-- F4. GOLD.FACT_REVENU
--     Granularité : 1 ligne = 1 commune
--     Source      : Bronze.socio_demo_communes
--     fk_temps = 5 → Filosofi 2021
--     Note       : valeurs NULL = secret INSEE ('S') → conservées NULL
-- ------------------------------------------------------------

CREATE TABLE Gold.FACT_REVENU AS
SELECT
    ROW_NUMBER() OVER (ORDER BY s.code_commune) AS id_fait,

    c.id_commune AS fk_commune,
    5            AS fk_temps,

    CAST(s.revenu_median_menages_2021 AS INTEGER) AS revenu_median_menages,
    CAST(s.revenu_decile_1_menages    AS INTEGER) AS revenu_decile_1,
    CAST(s.revenu_decile_9_menages    AS INTEGER) AS revenu_decile_9,

    s.taux_pauvrete_total_2021  AS taux_pauvrete_total,
    s.taux_pauvrete_0_14_ans,
    s.taux_pauvrete_15_29_ans,
    s.taux_pauvrete_30_44_ans,
    s.taux_pauvrete_45_59_ans,
    s.taux_pauvrete_60_74_ans,
    s.taux_pauvrete_75_ans_plus AS taux_pauvrete_75_plus,

    s.part_revenus_activite,
    s.part_revenus_patrimoine,
    s.part_prestations_familiales,
    s.part_prestations_sociales,
    s.part_aides_logement,

    -- Comptage des colonnes revenues NULL à cause du secret INSEE
    (CASE WHEN s.revenu_median_menages_2021 IS NULL THEN 1 ELSE 0 END
   + CASE WHEN s.taux_pauvrete_total_2021   IS NULL THEN 1 ELSE 0 END
   + CASE WHEN s.revenu_decile_1_menages    IS NULL THEN 1 ELSE 0 END
   + CASE WHEN s.revenu_decile_9_menages    IS NULL THEN 1 ELSE 0 END)  AS nb_valeurs_secretes,
    FALSE AS is_imputed,
    NULL  AS imputation_methode,

    1              AS fk_chargement,
    s.source_fichier,
    CURRENT_TIMESTAMP AS date_chargement

FROM Bronze.socio_demo_communes s
JOIN Gold.DIM_COMMUNE c
    ON s.code_commune = c.code_commune
    AND c.is_current  = TRUE;


-- ------------------------------------------------------------
-- F5. GOLD.FACT_ECONOMIE_LOCALE
--     Granularité : 1 ligne = 1 commune
--     Source      : Bronze.socio_demo_communes + vie_associative_commune
--     fk_temps = 7 → 2024 (dernière donnée éco disponible)
--     Note       : taux_chomage calculé en Gold (guard actifs > 0)
-- ------------------------------------------------------------

CREATE TABLE Gold.FACT_ECONOMIE_LOCALE AS
SELECT
    ROW_NUMBER() OVER (ORDER BY s.code_commune) AS id_fait,

    c.id_commune AS fk_commune,
    7            AS fk_temps,

    -- Taux chômage calculé (guard : actifs > 0)
    CASE
        WHEN s.actifs_15_ans_plus > 0
        THEN CAST(
            COALESCE(s.chomeurs_15_24_ans, 0)
          + COALESCE(s.chomeurs_25_54_ans, 0)
          + COALESCE(s.chomeurs_55_64_ans, 0) AS DOUBLE)
            * 100.0 / s.actifs_15_ans_plus
        ELSE NULL
    END AS taux_chomage,

    s.actifs_15_ans_plus                                                        AS actifs_total,
    s.actifs_occupes_15_ans_plus                                                AS actifs_occupes_total,
    COALESCE(s.chomeurs_15_24_ans, 0)
  + COALESCE(s.chomeurs_25_54_ans, 0)
  + COALESCE(s.chomeurs_55_64_ans, 0)                                           AS chomeurs_total,

    s.nombre_salaries,
    s.nombre_non_salaries,
    s.nombre_total_etablissements_2023  AS nb_etablissements,
    s.nombre_creations_entreprises_2024 AS creations_entreprises,
    s.effectif_total_salarie_2024       AS effectif_total_salarie,

    COALESCE(v.nb_associations, 0)      AS nb_associations,

    1              AS fk_chargement,
    s.source_fichier,
    CURRENT_TIMESTAMP AS date_chargement

FROM Bronze.socio_demo_communes s
JOIN Gold.DIM_COMMUNE c
    ON s.code_commune = c.code_commune
    AND c.is_current  = TRUE
LEFT JOIN Bronze.vie_associative_commune v
    ON s.code_commune = v.code_commune;


-- ------------------------------------------------------------
-- F6. GOLD.FACT_SECURITE
--     Granularité : 1 ligne = 1 commune
--     Source      : Bronze.criminalite_commune_clean (NULLs → 0)
--     fk_temps = 1 → 2022
-- ------------------------------------------------------------

CREATE TABLE Gold.FACT_SECURITE AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cr.code_commune) AS id_fait,

    c.id_commune AS fk_commune,
    1            AS fk_temps,

    cr.taux_cambriolage,
    cr.taux_violence_famille,
    cr.taux_violence_sexuelle,
    cr.taux_vols_armes,

    1              AS fk_chargement,
    cr.source_fichier,
    CURRENT_TIMESTAMP AS date_chargement

FROM Bronze.criminalite_commune_clean cr
JOIN Gold.DIM_COMMUNE c
    ON cr.code_commune = c.code_commune
    AND c.is_current   = TRUE;


-- ------------------------------------------------------------
-- F7. GOLD.FACT_LOGEMENT
--     Granularité : 1 ligne = 1 commune
--     Source      : Bronze.socio_demo_communes
--     fk_temps = 1 → 2022
-- ------------------------------------------------------------

CREATE TABLE Gold.FACT_LOGEMENT AS
SELECT
    ROW_NUMBER() OVER (ORDER BY s.code_commune) AS id_fait,

    c.id_commune AS fk_commune,
    1            AS fk_temps,

    s.nombre_total_logements   AS logements_total,
    s.nombre_logements_vacants AS logements_vacants,
    s.residences_principales_hlm AS logements_hlm,

    1              AS fk_chargement,
    s.source_fichier,
    CURRENT_TIMESTAMP AS date_chargement

FROM Bronze.socio_demo_communes s
JOIN Gold.DIM_COMMUNE c
    ON s.code_commune = c.code_commune
    AND c.is_current  = TRUE;


-- ------------------------------------------------------------
-- F8. GOLD.FACT_EDUCATION
--     ⚠ Source non encore disponible dans RAW → table vide
--       À alimenter via ETL dédié quand la source
--       INSEE RP diplômes sera chargée dans RAW.
-- ------------------------------------------------------------

CREATE TABLE Gold.FACT_EDUCATION (
    id_fait                   INT,
    fk_commune                INT,
    fk_temps                  INT,
    taux_diplomes_bac         DOUBLE,
    taux_diplomes_superieur   DOUBLE,
    taux_sans_diplome         DOUBLE,
    taux_scolarisation_3_17   DOUBLE,
    nb_eleves_primaire        INT,
    nb_eleves_secondaire      INT,
    nb_etudiants              INT,
    fk_chargement             INT,
    source_fichier            VARCHAR,
    date_chargement           TIMESTAMP
);
-- ⚠ Table intentionnellement vide — à peupler via ETL dédié