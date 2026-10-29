"""Collecte des 5 sources brutes depuis les API publiques, au format attendu par etl.py.

Remplace l'API Airflow : Paris Data (Belib'), data.gouv.fr / transport.data.gouv.fr (IRVE)
et INSEE Melodi (population). Aucune clé d'API n'est nécessaire.

Utilisation en ligne de commande :
    python sources_publiques.py                 # écrit les CSV dans ./data/sources/
    python sources_publiques.py --sortie dossier
    python sources_publiques.py --s3            # envoie aussi les CSV sur s3://$S3_BUCKET/raw/data/
"""
import argparse
import io
import os
import re
import requests
import pandas as pd

PARIS_DATA = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets"
URL_BELIB_STAT = f"{PARIS_DATA}/belib-points-de-recharge-pour-vehicules-electriques-donnees-statiques/exports/json"
URL_BELIB_RT = f"{PARIS_DATA}/belib-points-de-recharge-pour-vehicules-electriques-disponibilite-temps-reel/exports/json"
# Lien stable data.gouv.fr vers la dernière consolidation du schéma IRVE statique
URL_IRVE_STAT = "https://www.data.gouv.fr/api/1/datasets/r/eb76d20a-8501-400e-b336-d85724de5435"
# Base nationale consolidée IRVE dynamique (transport.data.gouv.fr)
URL_IRVE_DYN = "https://transport.data.gouv.fr/resources/84098/download"
URL_MELODI = "https://api.insee.fr/melodi/data/DS_RP_TD_POPULATION_AGESEX_PRINC"

TIMEOUT = 120

# Libellés de statut harmonisés avec ceux de Belib'
STATUTS_IRVE = {
    "libre": "Disponible",
    "occupe": "Occupé (en charge)",
    "reserve": "Réservé",
    "inconnu": "Inconnu",
}


def _export_json(url):
    """Télécharge un export JSON Opendatasoft (liste d'enregistrements) en DataFrame."""
    response = requests.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return pd.DataFrame(response.json())


def belib_stat():
    """Points de charge Belib' (données statiques) — colonnes lat / lon comme l'ancien fichier."""
    df = _export_json(URL_BELIB_STAT)
    coords = df.pop("coordonneesxy").apply(lambda c: c if isinstance(c, dict) else {})
    df["lat"] = coords.apply(lambda c: c.get("lat"))
    df["lon"] = coords.apply(lambda c: c.get("lon"))
    if "observations" in df.columns:
        df["observations"] = df["observations"].apply(lambda o: " ; ".join(o) if isinstance(o, list) else o)
    df["puissance_nominale"] = pd.to_numeric(df["puissance_nominale"], errors="coerce")
    print(f"[belib_stat] {len(df)} points de charge (Paris Data)")
    return df


def belib_rt():
    """Statuts temps réel Belib' — id_pdc, statut_pdc et snapshot_at comme l'ancien fichier."""
    df = _export_json(URL_BELIB_RT)
    df = df.drop(columns=["coordonneesxy"], errors="ignore")
    df["snapshot_at"] = pd.to_datetime(df["last_updated"], format="ISO8601", utc=True, errors="coerce")
    print(f"[belib_rt] {len(df)} statuts (Paris Data)")
    return df


def _est_parisien(df):
    """Lignes situées à Paris : code INSEE 751xx ou code postal 750xx / 75116 dans l'adresse."""
    insee = df["code_insee_commune"].fillna("").str.match(r"^751(0[1-9]|1\d|20)$")
    adresse = df["adresse_station"].fillna("").str.contains(r"\b75(?:0(?:0[1-9]|1\d|20)|116)\b", regex=True)
    return insee | adresse


def irve_conso():
    """Fichier national consolidé IRVE (statique), filtré sur Paris par morceaux pour limiter la mémoire."""
    morceaux = []
    with requests.get(URL_IRVE_STAT, stream=True, timeout=TIMEOUT) as response:
        response.raise_for_status()
        response.raw.decode_content = True
        for chunk in pd.read_csv(response.raw, dtype=str, chunksize=50_000):
            morceaux.append(chunk[_est_parisien(chunk)])
    df = pd.concat(morceaux, ignore_index=True)
    df["puissance_nominale"] = pd.to_numeric(df["puissance_nominale"], errors="coerce")
    print(f"[irve_conso] {len(df)} points de charge à Paris (data.gouv.fr)")
    return df


def irve_dyn(ids_pdc=None):
    """Statuts IRVE dynamiques — statut_actuel et snapshot_at comme l'ancien fichier.

    ids_pdc : identifiants id_pdc_itinerance à conserver (sinon tout le fichier national).
    """
    response = requests.get(URL_IRVE_DYN, timeout=TIMEOUT)
    response.raise_for_status()
    df = pd.read_csv(io.StringIO(response.text), dtype=str)
    if ids_pdc is not None:
        df = df[df["id_pdc_itinerance"].isin(set(ids_pdc))]
    df["statut_actuel"] = df["occupation_pdc"].map(STATUTS_IRVE).fillna("Inconnu")
    df.loc[df["etat_pdc"] == "hors_service", "statut_actuel"] = "En maintenance"
    df["snapshot_at"] = pd.to_datetime(df["horodatage"], format="ISO8601", utc=True, errors="coerce")
    df = df.drop(columns=["occupation_pdc"])
    print(f"[irve_dyn] {len(df)} statuts (transport.data.gouv.fr)")
    return df


def paris_population():
    """Population des 20 arrondissements par âge détaillé (INSEE Melodi, dernier recensement)."""
    lignes = []
    for num in range(1, 21):
        codgeo = f"751{num:02d}"
        response = requests.get(URL_MELODI, params={"GEO": f"2023-ARM-{codgeo}", "SEX": "_T", "maxResult": 1000},
                                timeout=TIMEOUT)
        response.raise_for_status()
        obs = response.json().get("observations", [])
        valeurs = {o["dimensions"]["AGE"]: o["measures"]["OBS_VALUE_NIVEAU"]["value"] for o in obs}
        annee = max((o["dimensions"]["TIME_PERIOD"] for o in obs), default=None)
        if "_T" not in valeurs:
            continue
        mineurs = sum(v for age, v in valeurs.items() if re.fullmatch(r"Y(\d|1[0-7])", age))
        total = valeurs["_T"]
        lignes.append({
            "CODGEO": codgeo,
            "pop_total": round(total),
            "pop_majeurs_18plus": round(total - mineurs),
            "pop_mineurs_0_17": round(mineurs),
            "annee": annee,
        })
    df = pd.DataFrame(lignes)
    print(f"[paris_population] {len(df)} arrondissements (INSEE Melodi)")
    return df


SOURCES = {
    "belib_stat": belib_stat,
    "belib_rt": belib_rt,
    "irve_conso": irve_conso,
    "irve_dyn": irve_dyn,
    "paris_population": paris_population,
}


def lire_source(source):
    """Lit une source depuis son API publique ; None si la source est inconnue ou indisponible."""
    if source not in SOURCES:
        return None
    try:
        return SOURCES[source]()
    except Exception as e:
        print(f"Attention : [{source}] API publique KO : {type(e).__name__} {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Télécharge les 5 sources brutes depuis les API publiques.")
    parser.add_argument("--sortie", default=os.path.join("data", "sources"), help="dossier des CSV produits")
    parser.add_argument("--s3", action="store_true", help="envoie aussi les CSV sur s3://$S3_BUCKET/raw/data/")
    args = parser.parse_args()
    os.makedirs(args.sortie, exist_ok=True)

    fichiers = {}
    conso = None
    for source in SOURCES:
        if source == "irve_dyn":
            # Fichier national : on ne garde que les points de charge parisiens du fichier statique
            ids = conso["id_pdc_itinerance"] if conso is not None else None
            df = irve_dyn(ids)
        else:
            df = SOURCES[source]()
        if source == "irve_conso":
            conso = df
        chemin = os.path.join(args.sortie, f"{source}.csv")
        df.to_csv(chemin, index=False, encoding="utf-8")
        fichiers[source] = chemin
        print(f"  -> {chemin}")

    if args.s3:
        import boto3
        bucket = os.environ.get("S3_BUCKET")
        if not bucket:
            raise SystemExit("S3_BUCKET n'est pas défini.")
        s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-north-1"))
        for source, chemin in fichiers.items():
            s3.upload_file(chemin, bucket, f"raw/data/{source}.csv")
            print(f"  -> s3://{bucket}/raw/data/{source}.csv")


if __name__ == "__main__":
    main()
