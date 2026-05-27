import streamlit as st
import requests
import pandas as pd



# CONFIGURATION PAGE


st.set_page_config(
    page_title="Electio-Analytics",
    layout="wide"
)


# TITRE APPLICATION


st.title("Prédiction électorale - Electio-Analytics")

st.write(
    "Démo MSPR : prédiction du gagnant électoral pour une commune."
)



# URL API FASTAPI


API_URL = "http://127.0.0.1:8000/predict_winner"



# MENU LATÉRAL


st.sidebar.header("Données de la commune")


# CHAMPS UTILISATEUR


commune = st.sidebar.text_input(
    "Commune",
    "Vitry-sur-Seine"
)

population_totale_2022 = st.sidebar.number_input(
    "Population totale 2022",
    value=95282.0
)

population_15_29_ans = st.sidebar.number_input(
    "Population 15-29 ans",
    value=20176.10
)

population_45_59_ans = st.sidebar.number_input(
    "Population 45-59 ans",
    value=18214.75
)

population_60_74_ans = st.sidebar.number_input(
    "Population 60-74 ans",
    value=12513.07
)

revenu_median_menages_2021 = st.sidebar.number_input(
    "Revenu médian ménages 2021",
    value=19490.0
)

nombre_total_etablissements_2023 = st.sidebar.number_input(
    "Nombre total établissements 2023",
    value=6449.0
)

entreprises_par_habitant = st.sidebar.number_input(
    "Entreprises par habitant",
    value=0.0677
)

taux_chomage_global = st.sidebar.number_input(
    "Taux chômage global",
    value=0.1428
)

taux_pauvrete_total_2021 = st.sidebar.number_input(
    "Taux pauvreté total 2021",
    value=26.0
)

taux_hlm = st.sidebar.number_input(
    "Taux HLM",
    value=0.3370
)

associations_par_habitant = st.sidebar.number_input(
    "Associations par habitant",
    value=0.0184
)

participation_reelle = st.sidebar.number_input(
    "Participation réelle",
    value=0.6911
)

taux_logements_vacants = st.sidebar.number_input(
    "Taux logements vacants",
    value=0.0498
)

taux_cambriolage = st.sidebar.number_input(
    "Taux cambriolage",
    value=11.3244
)


# DONNÉES ENVOYÉES À L'API


data = {

    "commune": commune,

    "population_totale_2022": population_totale_2022,
    "population_15_29_ans": population_15_29_ans,
    "population_45_59_ans": population_45_59_ans,
    "population_60_74_ans": population_60_74_ans,

    "revenu_median_menages_2021":
        revenu_median_menages_2021,

    "nombre_total_etablissements_2023":
        nombre_total_etablissements_2023,

    "entreprises_par_habitant":
        entreprises_par_habitant,

    "taux_chomage_global":
        taux_chomage_global,

    "taux_pauvrete_total_2021":
        taux_pauvrete_total_2021,

    "taux_hlm":
        taux_hlm,

    "associations_par_habitant":
        associations_par_habitant,

    "participation_reelle":
        participation_reelle,

    "taux_logements_vacants":
        taux_logements_vacants,

    "taux_cambriolage":
        taux_cambriolage
}



# BOUTON PRÉDICTION


if st.button("Prédire le gagnant"):

    # Appel API
    response = requests.post(API_URL, json=data)

    # Vérification réponse
    if response.status_code == 200:

        result = response.json()

        st.success("Prédiction réalisée avec succès")

        # AFFICHAGE GAGNANT
      

        st.subheader("Résultat final")

        st.metric(
            label="Gagnant prédit",
            value=f'{result["gagnant_predit"]} - {result["famille_politique_gagnant"]}'
        )

        st.metric(
            label="Voix estimées",
            value=f'{result["voix_estimees"]} voix'
        )

        # TABLEAU COMPLET
    

        st.subheader("Classement complet des candidats")

        df = pd.DataFrame(result["classement_complet"])

        st.dataframe(df)

    else:

        st.error("Erreur lors de l'appel API")

        st.write(response.text)