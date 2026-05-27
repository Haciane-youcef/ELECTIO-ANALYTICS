from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import joblib


# INITIALISATION DE L'API

app = FastAPI(
    title="Prédiction électorale",
    description="MSPR Electio-Analytics pour la prédiction électorale",
    version="1.0"
)


# CHARGEMENT DU MODÈLE ML

model = joblib.load("best_model.pkl")
model_columns = joblib.load("model_columns.pkl")


# LISTE DES CANDIDATS

CANDIDATS = [
    "ARTHAUD Nathalie",
    "MÉLENCHON Jean-Luc",
    "ROUSSEL Fabien",
    "HIDALGO Anne",
    "LASSALLE Jean",
    "DUPONT-AIGNAN Nicolas",
    "LE PEN Marine",
    "PÉCRESSE Valérie",
    "ZEMMOUR Éric",
    "JADOT Yannick",
    "POUTOU Philippe",
    "MACRON Emmanuel"
]


# FAMILLES POLITIQUES

FAMILLES_POLITIQUES = {
    "MACRON Emmanuel": "Centre",
    "LE PEN Marine": "Extrême droite",
    "MÉLENCHON Jean-Luc": "Gauche",
    "ZEMMOUR Éric": "Extrême droite",
    "PÉCRESSE Valérie": "Droite",
    "JADOT Yannick": "Gauche",
    "ROUSSEL Fabien": "Gauche",
    "HIDALGO Anne": "Gauche",
    "LASSALLE Jean": "Centre",
    "DUPONT-AIGNAN Nicolas": "Droite",
    "ARTHAUD Nathalie": "Extrême gauche",
    "POUTOU Philippe": "Extrême gauche"
}


# SCHÉMA DES DONNÉES D'ENTRÉE

class CommuneScenarioInput(BaseModel):
    commune: str
    population_totale_2022: float
    population_15_29_ans: float
    population_45_59_ans: float
    population_60_74_ans: float
    revenu_median_menages_2021: float
    nombre_total_etablissements_2023: float
    entreprises_par_habitant: float
    taux_chomage_global: float
    taux_pauvrete_total_2021: float
    taux_hlm: float
    associations_par_habitant: float
    participation_reelle: float
    taux_logements_vacants: float
    taux_cambriolage: float


# ROUTE PRINCIPALE

@app.get("/")
def accueil():
    return {
        "message": "Bienvenue sur l'API Electio-Analytics"
    }


# ROUTE DE TEST API

@app.get("/health")
def health():
    return {
        "status": "OK",
        "message": "API opérationnelle"
    }


# PRÉDICTION DU GAGNANT

@app.post("/predict_winner")
def predict_winner(data: CommuneScenarioInput):
    results = []

    # Boucle sur les 12 candidats
    for candidat in CANDIDATS:
        row = data.dict()

        # Ajout du candidat courant
        row["nom_complet"] = candidat

        # Suppression du nom de commune car il n'est pas utilisé par le modèle
        commune = row.pop("commune")

        # Transformation en DataFrame
        input_df = pd.DataFrame([row])

        # Encodage des variables catégorielles
        input_df = pd.get_dummies(input_df)

        # Alignement avec les colonnes utilisées pendant l'entraînement
        input_df = input_df.reindex(columns=model_columns, fill_value=0)

        # Prédiction brute du modèle
        score_brut = float(model.predict(input_df)[0])

        results.append({
            "commune": commune,
            "candidat": candidat,
            "famille_politique": FAMILLES_POLITIQUES.get(candidat, "Non défini"),
            "score_brut_modele": round(score_brut, 4)
        })

    # Transformation des scores bruts en voix prédites
    # Chaque score est réparti proportionnellement sur la population totale.
    total_score = sum(item["score_brut_modele"] for item in results)

    for item in results:
        item["voix_predites_estimees"] = int(
            round(
                (item["score_brut_modele"] / total_score)
                * data.population_totale_2022
            )
        )

        # Suppression du score brut pour garder un résultat métier lisible
        del item["score_brut_modele"]

    # Classement des candidats par voix prédites
    classement = sorted(
        results,
        key=lambda x: x["voix_predites_estimees"],
        reverse=True
    )

    gagnant = classement[0]

    return {
        "commune": data.commune,
        "gagnant_predit": gagnant["candidat"],
        "famille_politique_gagnant": gagnant["famille_politique"],
        "voix_estimees": gagnant["voix_predites_estimees"],
        "classement_complet": classement
    }