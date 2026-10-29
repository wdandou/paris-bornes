import io
import os
import numpy as np
import requests
import pandas as pd
import boto3
import database
import etl

# Dossier de travail pour les CSV intermédiaires
os.makedirs("./data", exist_ok=True)

# La lecture des API Airflow / S3 est centralisée dans etl.collect_data()


def collect_webdata(url):
    response = requests.get(url)
    if response.status_code == 200:
        data = pd.read_csv(io.StringIO(response.text), low_memory=False)
        return data
    # on récupère les données de l'API et on les renvoie
    else:
        print(f"Erreur {response.status_code} lors de la récupération des données pour l'URL {url}")
    #si le status code de la réponse est différent de 200, on affiche le code d'erreur.

def calculer_pression(df_bornes, df_vehicules):
    """Calcule la pression par arrondissement."""
    #Rappel de la formule
    #Pression par arrondissement = Nombre de véhicules électriques immatriculés / Nombre de bornes
    
    # Dernier trimestre disponible
    dernier_trimestre = df_vehicules["date_arrete"].max()
    ve_derniers = df_vehicules[df_vehicules["date_arrete"] == dernier_trimestre]
    
    # véhicules électriques par arrondissement
    ve_par_arr = ve_derniers.groupby("num_arrondissement").agg(
        nb_ve=("nb_vp_rechargeables_el", "sum"),
        nb_vp_total=("nb_vp", "sum")
    ).reset_index()
    
    # Bornes par arrondissement
    bornes_par_arr = (
        df_bornes.groupby("num_arrondissement")
        .agg(nb_pdc=("id_pdc_itinerance", "count"))
        .reset_index()
    )
    
    # Fusion et calcul de pression dans la fonction
    pression = ve_par_arr.merge(bornes_par_arr, on="num_arrondissement", how="left")
    pression["nb_pdc"] = pression["nb_pdc"].fillna(0).astype(int)
    # Un arrondissement sans point de charge a une pression indéfinie (NaN) plutôt qu'infinie
    pression["pression"] = (pression["nb_ve"] / pression["nb_pdc"].replace(0, np.nan)).round(1)
    pression["taux_ve"] = (pression["nb_ve"] / pression["nb_vp_total"].replace(0, np.nan) * 100).round(1)

    # Trier par pression décroissante (les plus sous-équipés en premier, puis ceux sans borne)
    pression = pression.sort_values("pression", ascending=False, na_position="first")
    
    print(f"\nPression VE par arrondissement (trimestre {dernier_trimestre}) :")
    print(pression.to_string(index=False))  
    return pression

def calculer_projections(pression):
    """Projette le déficit de bornes sur 1 à 5 ans selon 3 scénarios."""
    
    if pression is None or pression.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    # Taux de croissance annuel du parc VE par scénario
    scenarios = {
        "bas": 0.20,       # +20%/an
        "central": 0.40,   # +40%/an
        "haut": 0.60,      # +60%/an
    }
    
    # Pression cible : 10 VE par borne (seuil raisonnable)
    PRESSION_CIBLE = 10
    
    lignes_arrdt = []
    lignes_paris = []
    
    for scenario, taux in scenarios.items():
        for horizon in range(1, 6):
            total_deficit = 0
            
            for _, row in pression.iterrows():
                arr = int(row["num_arrondissement"])
                nb_ve_actuel = row.get("nb_ve", 0) or 0
                nb_pdc_actuel = row.get("nb_pdc", 0) or 0
                
                # Projection du nombre de VE
                ve_projete = int(nb_ve_actuel * (1 + taux) ** horizon)
                
                # Nombre de bornes nécessaires pour atteindre la pression cible
                bornes_cible = max(int(ve_projete / PRESSION_CIBLE), nb_pdc_actuel)
                
                # Déficit = bornes nécessaires - bornes existantes
                deficit = max(0, bornes_cible - nb_pdc_actuel)
                
                # Pression projetée (avec les bornes actuelles)
                pression_projetee = round(ve_projete / nb_pdc_actuel, 1) if nb_pdc_actuel > 0 else 999
                
                # Énergie additionnelle estimée (2.5 MWh/an par borne ajoutée)
                energie_add = round(deficit * 2.5, 1)
                
                lignes_arrdt.append({
                    "arr_num": arr,
                    "scenario": scenario,
                    "horizon_years": horizon,
                    "ve_actuel": int(nb_ve_actuel),
                    "ve_projete": ve_projete,
                    "bornes_actuelles": int(nb_pdc_actuel),
                    "bornes_cible": bornes_cible,
                    "deficit_bornes": deficit,
                    "pression_projetee": pression_projetee,
                    "energie_add_mwh": energie_add,
                })
                
                total_deficit += deficit
            
            lignes_paris.append({
                "scenario": scenario,
                "horizon_years": horizon,
                "deficit_total": total_deficit,
            })
    
    df_arrdt = pd.DataFrame(lignes_arrdt)
    df_paris = pd.DataFrame(lignes_paris)
    
    print(f"Projections : {len(df_arrdt)} lignes (arrdt), {len(df_paris)} lignes (Paris)")
    return df_arrdt, df_paris

def evolution_parc_ve(df_vehicules):
    """Calcule l'évolution du parc VE par arrondissement dans le temps."""
    evolution = df_vehicules.groupby(["date_arrete", "num_arrondissement"]).agg(
        nb_ve=("nb_vp_rechargeables_el", "sum"),
        nb_vp_total=("nb_vp", "sum")
    ).reset_index()
    evolution["taux_ve"] = (evolution["nb_ve"] / evolution["nb_vp_total"] * 100).round(2)    
    return evolution

stations_belib = etl.recuperer_liste_stations_belib()
stations_gireve = etl.recuperer_liste_stations_gireve()
listestations = etl.fusionner_sources(stations_belib, stations_gireve)
liste_ve = etl.recuperer_vehicules_electriques()
parc_ve = evolution_parc_ve(liste_ve)
energie = etl.enedis_paris_data(2022)
population = etl.recuperer_population()

if not parc_ve.empty:
    database.sauvegarder_parc_vehicules(parc_ve)

if not energie.empty:
    database.sauvegarder_energie(energie)

if not population.empty:
    database.sauvegarder_population(population)

# Sans bornes (S3 et Airflow indisponibles), pression et projections ne sont pas calculables
if not listestations.empty:
    listestations.to_csv("./data/stations_paris.csv", index=False)
    database.sauvegarder_totalite_bornes(listestations)

    pression = calculer_pression(listestations,liste_ve)
    pression.to_csv("./data/pression_paris.csv", index=False, encoding="utf-8-sig")
    if not pression.empty:
        database.sauvegarder_pression(pression)

    df_arrdt, df_paris = calculer_projections(pression)
    if not df_arrdt.empty:
        # Sauvegarder les CSV pour streamlit_additions.py
        df_arrdt.to_csv("./data/energie_by_arrdt.csv", index=False)
        df_paris.to_csv("./data/soutenabilite_scenarios.csv", index=False)

        # Sauvegarder en SQLite aussi
        database.sauvegarder_projections(df_arrdt, df_paris)
        print("Projections sauvegardées")
else:
    print("Aucune station de recharge récupérée : vérifier S3_BUCKET / identifiants AWS ou ALH_EMAIL / ALH_PASSWORD.")

# Upload S3
S3_BUCKET = etl.S3_BUCKET
if S3_BUCKET:

    s3 = boto3.client("s3", region_name=etl.AWS_REGION)

    fichiers = {
        "bornes": "./data/stations_paris.csv",
        "pression": "./data/pression_paris.csv",
        "vehicules": "./data/vehicules_electriques_paris_ORE.csv",
        "energie": "./data/energie_paris.csv",
    }
    
    for nom, chemin in fichiers.items():
        if os.path.exists(chemin):
            cle = f"raw/data/{nom}.csv"
            s3.upload_file(chemin, S3_BUCKET, cle)
            os.remove(chemin)
            print(f"Uploadé s3://{S3_BUCKET}/{cle}")
else:
    print("S3 non configuré, sauvegarde locale uniquement")

