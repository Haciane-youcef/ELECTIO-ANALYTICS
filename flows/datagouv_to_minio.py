"""
datagouv_to_minio.py
────────────────────
Flow Prefect 3 – Téléchargement parallèle des datasets data.gouv.fr
et stockage dans MinIO (bucket datalake / raw zone).

Structure cible dans MinIO :
  datalake/
    raw_2/
      elections/
        resultats-par-niveau-burvot-t1-france-entiere.xlsx
      chomage/
        taux-chomage-trimestriel.csv
      securite/
        crimes-delits-enregistres.csv
      ...
"""

import os
import io
import requests
from datetime import datetime

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from minio import Minio
from minio.error import S3Error

# ─────────────────────────────────────────────────────────────────────────────
# Configuration MinIO (récupérée depuis les variables d'env injectées par Docker)
# ─────────────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "ea-minio-storage:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "eaadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "eaadmin123")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "datalake")
RAW_PREFIX       = "raw_2"          # dossier racine de la raw zone

# ─────────────────────────────────────────────────────────────────────────────
# Catalogue des datasets à télécharger
# Ajoute / retire des entrées selon tes besoins.
# ─────────────────────────────────────────────────────────────────────────────
DATASETS = [
    # ── Élections ──────────────────────────────────────────────────────────
    {
        "category": "elections",
        "filename": "resultats-par-niveau-burvot-t1-france-entiere.xlsx",
        "url": (
            "https://static.data.gouv.fr/resources/"
            "election-presidentielle-des-10-et-24-avril-2022-resultats-definitifs-du-1er-tour/"
            "20220414-152612/resultats-par-niveau-burvot-t1-france-entiere.xlsx"
        ),
    },
    {
        "category": "elections",
        "filename": "resultats-par-niveau-burvot-t2-france-entiere.xlsx",
        "url": (
            "https://static.data.gouv.fr/resources/"
            "election-presidentielle-des-10-et-24-avril-2022-resultats-definitifs-du-2eme-tour/"
            "20220427-084513/resultats-par-niveau-burvot-t2-france-entiere.xlsx"
        ),
    },

    # ── Chômage ────────────────────────────────────────────────────────────
    {
        "category": "chomage",
        "filename": "taux-chomage-trimestriel-departements.csv",
        "url": (
            "https://static.data.gouv.fr/resources/"
            "taux-de-chomage-trimestriels-departements-regions-france/"
            "20240101-000000/taux-chomage-trimestriel.csv"
        ),
    },

    # ── Sécurité / Criminalité ─────────────────────────────────────────────
    {
        "category": "securite",
        "filename": "crimes-et-delits-enregistres.csv",
        "url": (
            "https://static.data.gouv.fr/resources/"
            "crimes-et-delits-enregistres-par-la-police-et-la-gendarmerie-nationales/"
            "20240101-000000/crimes-delits.csv"
        ),
    },

    # ── Démographie / Population ────────────────────────────────────────────
    {
        "category": "demographie",
        "filename": "population-communes-2021.csv",
        "url": (
            "https://static.data.gouv.fr/resources/"
            "population-des-communes-france/"
            "20230101-000000/population-communes.csv"
        ),
    },

    # ── Éducation ──────────────────────────────────────────────────────────
    {
        "category": "education",
        "filename": "taux-reussite-baccalaureat.csv",
        "url": (
            "https://static.data.gouv.fr/resources/"
            "taux-de-reussite-au-baccalaureat/"
            "20230901-000000/taux-reussite-bac.csv"
        ),
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_minio_client() -> Minio:
    """Crée et renvoie un client MinIO."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,          # HTTP en interne Docker
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    """Crée le bucket s'il n'existe pas encore."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


# ─────────────────────────────────────────────────────────────────────────────
# Tasks Prefect
# ─────────────────────────────────────────────────────────────────────────────

@task(retries=3, retry_delay_seconds=10, name="download-and-upload")
def download_and_upload(dataset: dict) -> dict:
    """
    1. Télécharge le fichier depuis data.gouv.fr
    2. L'upload dans MinIO sous  raw/<category>/<filename>
    Retourne un dict avec le statut de l'opération.
    """
    logger = get_run_logger()
    category = dataset["category"]
    filename  = dataset["filename"]
    url       = dataset["url"]

    object_path = f"{RAW_PREFIX}/{category}/{filename}"

    logger.info(f"⬇️  Téléchargement : {url}")
    try:
        response = requests.get(url, timeout=60, stream=True)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error(f"❌ Échec téléchargement [{filename}] : {exc}")
        return {"dataset": filename, "status": "ERREUR_DOWNLOAD", "error": str(exc)}

    content      = response.content
    content_size = len(content)
    content_type = response.headers.get("Content-Type", "application/octet-stream")

    logger.info(f"📦 Upload MinIO : {MINIO_BUCKET}/{object_path}  ({content_size} octets)")
    try:
        client = get_minio_client()
        ensure_bucket(client, MINIO_BUCKET)

        client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_path,
            data=io.BytesIO(content),
            length=content_size,
            content_type=content_type,
        )
    except S3Error as exc:
        logger.error(f"❌ Échec upload MinIO [{filename}] : {exc}")
        return {"dataset": filename, "status": "ERREUR_MINIO", "error": str(exc)}

    logger.info(f"✅ Succès : {object_path}")
    return {
        "dataset":     filename,
        "category":    category,
        "object_path": f"{MINIO_BUCKET}/{object_path}",
        "size_bytes":  content_size,
        "status":      "OK",
        "uploaded_at": datetime.utcnow().isoformat(),
    }


@task(name="log-summary")
def log_summary(results: list[dict]) -> None:
    """Affiche un résumé des uploads dans les logs Prefect."""
    logger = get_run_logger()
    ok     = [r for r in results if r.get("status") == "OK"]
    errors = [r for r in results if r.get("status") != "OK"]

    logger.info("═" * 60)
    logger.info(f"📊 RÉSUMÉ : {len(ok)}/{len(results)} fichiers uploadés avec succès")
    for r in ok:
        logger.info(f"  ✅ {r['object_path']}  ({r['size_bytes']} octets)")
    for r in errors:
        logger.warning(f"  ❌ {r['dataset']} → {r['status']} : {r.get('error')}")
    logger.info("═" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# Flow principal
# ─────────────────────────────────────────────────────────────────────────────

@flow(
    task_runner=ConcurrentTaskRunner(),
    name="DataGouv → MinIO Raw Zone",
    description=(
        "Télécharge en parallèle les datasets data.gouv.fr "
        "(élections, chômage, sécurité…) et les dépose dans "
        "la raw zone du bucket MinIO datalake."
    ),
)
def datagouv_to_minio_flow(datasets: list[dict] = DATASETS) -> list[dict]:
    """
    Flow principal : téléchargement parallèle + upload MinIO.
    Le paramètre `datasets` peut être surchargé depuis l'UI Prefect
    pour cibler un sous-ensemble de fichiers.
    """
    logger = get_run_logger()
    logger.info(f"🚀 Démarrage – {len(datasets)} dataset(s) à traiter")

    # Lancement en parallèle via ConcurrentTaskRunner
    futures = download_and_upload.map(datasets)
    results = [f.result() for f in futures]

    log_summary(results)
    return results


if __name__ == "__main__":
    datagouv_to_minio_flow()