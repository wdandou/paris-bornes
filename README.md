---
title: Paris Bornes
emoji: 🚀
colorFrom: red
colorTo: red
sdk: docker
app_port: 8501
tags:
- streamlit
pinned: false
short_description: Prioriser les zones d'installation des bornes de recharge
---

# ⚡ Bornes de recharge VE — Paris

**Où installer des bornes de recharge pour véhicules électriques à Paris, combien, et quand ?**

Paris compte environ 6 400 points de charge (671 stations), répartis de façon inégale entre les arrondissements, alors que le parc de véhicules électriques (VE) progresse rapidement. Le règlement européen **AFIR** fixe un objectif d'**une borne pour 10 VE**.

Cette application Streamlit croise les données de bornes, de parc automobile, de consommation électrique et de population. Pour chaque arrondissement, elle :

- mesure la **pression** actuelle : `nombre de VE immatriculés / nombre de points de charge` ;
- **projette le déficit de bornes** à 1–5 ans selon 3 scénarios de croissance du parc VE ;
- évalue la **soutenabilité réseau** des installations nécessaires.

Application en ligne : https://huggingface.co/spaces/wdandou/paris-bornes (code source : https://github.com/wdandou/paris-bornes)

Équipe : Aymeric LA HONDE, Wilfrid DANDOU, Antoine Charles MBEPA, Grégory HAAS — Projet final Jedha (DSFS).

### Résultats clés (collecte du 30/10/2026)

| Indicateur | Valeur |
|---|---|
| Points de charge / stations à Paris | 6 409 / 671 |
| VE immatriculés (Agence ORE, 31/08/2026) | 101 530 |
| Pression moyenne Paris | 15,8 VE par point de charge (objectif AFIR : 10) |
| Arrondissements au-dessus de l'objectif | 16 sur 20 ; les plus tendus : 7e (53), 9e (49), 13e (34), 15e (29) |
| Bornes manquantes à 3 ans | 11 321 (bas) · 21 458 (central) · 35 167 (haut) |

---

## Architecture

```
 Sources                     Collecte / nettoyage      Calculs & orchestration       Stockage                 Restitution
 ───────                     ────────────────────      ───────────────────────       ────────                 ───────────
 S3 (optionnel) ──────┐
 API publiques ───────┤     sources_publiques.py
  (Paris Data,        ├──►  etl.py  ───────────────►  bornes_arrondissements.py ──► bornes.db (SQLite) ──► streamlit_app.py
   data.gouv, INSEE)  │     (collect_data,             (pression, projections,       ./data/*.csv           + streamlit_additions.py
 Agence ORE (VE) ─────┤      harmonisation, fusion)     persistance, upload S3)       S3 (optionnel)
 Enedis (conso) ──────┤
 API Airflow (opt.) ──┘
```

| Fichier | Rôle |
|---|---|
| `etl.py` | Collecte des sources (S3, puis API publiques, puis API Airflow), harmonisation et fusion Belib' / IRVE |
| `sources_publiques.py` | Lecture des 5 sources brutes sur les API publiques, au format attendu par `etl.py` ; utilisable aussi en ligne de commande |
| `bornes_arrondissements.py` | Pipeline complet : ETL → pression → projections → SQLite / CSV → upload S3 |
| `models.py` | Modèles SQLAlchemy (`bornes`, `vehicules_electriques`, `pression`, `energie`, `population`) |
| `database.py` | Accès à la base SQLite `bornes.db` (sauvegarde, chargement, jointures) |
| `streamlit_app.py` | Tableau de bord : filtres, KPIs, cartes et 4 premiers onglets |
| `streamlit_additions.py` | Onglets de prospective : projection, évolution du déficit, soutenabilité réseau |
| `data/arrondissements.geojson` | Contours des 20 arrondissements (clé de jointure `properties.c_ar`) |
| `Dockerfile` | Image de déploiement (Hugging Face Spaces) |
| `.github/workflows/sync-to-hf.yml` | GitHub Action : recopie la branche `main` sur le Space Hugging Face à chaque push |
| `requirements.txt` | Dépendances Python |

---

## Sources de données

| Donnée | Source | Clé `source` / accès |
|---|---|---|
| Bornes Belib' (position, puissance) | [Paris Data — Belib' données statiques](https://opendata.paris.fr/explore/dataset/belib-points-de-recharge-pour-vehicules-electriques-donnees-statiques/) | `belib_stat` |
| Statut temps réel Belib' | [Paris Data — Belib' disponibilité temps réel](https://opendata.paris.fr/explore/dataset/belib-points-de-recharge-pour-vehicules-electriques-disponibilite-temps-reel/) | `belib_rt` |
| Bornes IRVE consolidées (statique) | [data.gouv.fr — Base nationale des IRVE](https://www.data.gouv.fr/fr/datasets/fichier-consolide-des-bornes-de-recharge-pour-vehicules-electriques/), filtrée sur Paris | `irve_conso` |
| Statut temps réel IRVE | [transport.data.gouv.fr — base consolidée IRVE dynamique](https://transport.data.gouv.fr/datasets/beta-bases-nationales-des-points-de-recharge-pour-vehicules-electriques-en-france-irve) | `irve_dyn` |
| Population par âge et par arrondissement | [INSEE Melodi](https://portail-api.insee.fr/) — `DS_RP_TD_POPULATION_AGESEX_PRINC` (recensement 2023) | `paris_population` |
| Parc de véhicules par énergie et par commune | Agence ORE (open data), filtré sur Paris côté serveur | API publique |
| Consommation électrique par IRIS (2022) | Enedis (open data), pagination par curseur | `energie` sur S3, sinon API publique |

Aucune de ces API ne demande de clé. Les sources marquées d'une clé sont lues par `etl.collect_data()`, dans cet ordre :

1. **S3** : `s3://$S3_BUCKET/raw/data/<source>.csv`, si le bucket est configuré et que le fichier existe. Les statuts temps réel (`belib_rt`, `irve_dyn`) sautent cette étape, car une copie sur S3 serait périmée ;
2. **API publiques** (`sources_publiques.py`) ;
3. **API Airflow** (`https://alh-consulting.com/api/bornes-ve/data`), seulement si `ALH_EMAIL` / `ALH_PASSWORD` sont définis ;
4. si rien ne répond, `None` est renvoyé et l'étape concernée est ignorée.

Pour obtenir les 5 fichiers bruts sans lancer l'application :
```bash
python sources_publiques.py                 # écrit les CSV dans ./data/sources/
python sources_publiques.py --s3            # les envoie aussi sur s3://$S3_BUCKET/raw/data/
```

---

## Mise en œuvre étape par étape

### 1. Collecte et harmonisation des bornes (`etl.py`)
- **Belib'** :
  - l'arrondissement est extrait de l'adresse (code postal `750xx`) et le code INSEE `751xx` est reconstruit ;
  - le statut temps réel le plus récent de chaque point de charge est rattaché par une jointure à gauche ; à défaut, il vaut `Inconnu`.
- **IRVE / Gireve** : même traitement. Le champ `coordonneesXY` est découpé en longitude et latitude, et seuls les codes INSEE 75101 à 75120 sont conservés.
- **Fusion** : les deux sources, ramenées à 13 colonnes communes, sont concaténées. Les doublons sur `id_pdc_itinerance` sont supprimés en gardant **Belib' en priorité**.

### 2. Données de contexte
- **Parc VE** (Agence ORE) : filtré sur les 20 arrondissements, puis agrégé par trimestre (`evolution_parc_ve`) et sauvegardé dans la table `vehicules_electriques`.
- **Consommation électrique** (Enedis, 2022) : paginée par IRIS et rattachée à l'arrondissement.
- **Population** (INSEE, recensement 2023) : mineurs = somme des âges 0 à 17 ans, puis calcul de la part des majeurs.

### 3. Pression actuelle (`calculer_pression`)
- **Pression = nombre de VE / nombre de points de charge**, sur le dernier trimestre disponible.
- Un arrondissement sans point de charge a une pression indéfinie (`NaN`) et apparaît en tête du classement.
- L'objectif réglementaire est une pression inférieure à 10.

### 4. Projections à 1–5 ans (`calculer_projections`)
| Scénario | Croissance annuelle du parc VE |
|---|---|
| Bas | +20 % |
| Central | +40 % |
| Haut | +60 % |

Pour chaque arrondissement, scénario et horizon `h` :
- `ve_projete = nb_ve × (1 + taux)^h`
- `bornes_cible = max(ve_projete / 10, bornes actuelles)`
- `deficit_bornes = bornes_cible − bornes actuelles`
- `pression_projetee = ve_projete / bornes actuelles` (999 s'il n'y a aucune borne)
- `energie_add_mwh = deficit_bornes × 2,5 MWh/an`

Niveau de stress affiché : **vert** si la pression projetée est inférieure à 15, **ambre** si elle est inférieure à 35, **rouge** au-delà.

### 5. Persistance
- **SQLite** `bornes.db` : tables `bornes`, `vehicules_electriques`, `pression`, `energie`, `population`, `projections_arrdt` et `projections_paris`.
- **CSV** dans `./data/` : `stations_paris.csv`, `pression_paris.csv`, `energie_by_arrdt.csv`, `soutenabilite_scenarios.csv`, etc.
- **S3** (si `S3_BUCKET` est défini) : les CSV de bornes, de pression, de véhicules et d'énergie sont envoyés sur `raw/data/`, puis supprimés en local.

### 6. Tableau de bord (`streamlit_app.py`)
- **Barre latérale** :
  - filtre par arrondissement, appliqué à tous les onglets ;
  - bouton **« Rafraîchir les statuts »**, qui recharge les statuts Belib' temps réel.
- **KPIs** : stations, points de charge, bornes disponibles, puissance moyenne.
- **Onglets** :
  1. *Répartition des bornes* : carte par disponibilité, par puissance (lente ≤ 7 kW, normale 7–22 kW, rapide > 22 kW) et carte choroplèthe du taux de disponibilité ;
  2. *Pression sur les équipements* : carte et classement par arrondissement ;
  3. *Consommation électrique* : carte, MWh totaux et MWh par habitant ;
  4. *Données démographiques* : majeurs et mineurs, part des majeurs ;
  5. *Projection déficitaire* : choix du scénario et de l'horizon, carte du déficit, top 10 des arrondissements à équiper ;
  6. *Évolution déficit* : courbes des 3 scénarios de 1 à 5 ans, sur les arrondissements sélectionnés ;
  7. *Soutenabilité réseau* : niveau de stress à 3 ans et énergie additionnelle.

> Au **premier lancement**, si `bornes.db` ou le marqueur `data/.etl_termine` est absent, l'application exécute automatiquement `bornes_arrondissements.py` pour construire la base. Un verrou partagé garantit qu'une seule collecte tourne et qu'aucune session ne lit une base incomplète. Ce premier démarrage prend 1 à 2 minutes.

---

## Configuration

Variables d'environnement, à définir en local ou dans les *Secrets* du Space Hugging Face :

| Variable | Obligatoire | Description |
|---|---|---|
| `S3_BUCKET` | non | Bucket S3 : sources prioritaires dans `raw/data/*.csv`, et dépôt des CSV produits |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | si S3 | Identifiants AWS utilisés par boto3 |
| `AWS_REGION` | non | Région S3 (défaut : `eu-north-1`) |
| `ALH_EMAIL` / `ALH_PASSWORD` | non | Identifiants de l'API Airflow, dernier recours |
| `ETL_DECALAGE_HEURES` | non | Retard, en heures UTC, du dernier snapshot temps réel demandé à l'API (défaut : `2`) |

**Aucune variable n'est obligatoire** : sans S3 ni Airflow, toutes les données proviennent des API publiques. La première collecte prend alors environ une minute, dont le téléchargement du fichier national IRVE (environ 115 Mo).

---

## Lancer le projet

### En local
```bash
python -m venv .venv
source .venv/bin/activate          # Windows : .venv\Scripts\activate
pip install -r requirements.txt

# Variables d'environnement (exemple bash)
export S3_BUCKET=mon-bucket AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...

# (optionnel) construire ou rafraîchir la base sans lancer l'interface
python bornes_arrondissements.py

streamlit run streamlit_app.py
```
L'application est alors accessible sur http://localhost:8501.

Pour **rafraîchir toutes les données**, supprimez `bornes.db` puis relancez `python bornes_arrondissements.py` ou l'application.

### Avec Docker
```bash
docker build -t paris-bornes .
docker run -p 8501:8501 \
  -e S3_BUCKET=mon-bucket -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \
  paris-bornes
```

### Sur Hugging Face Spaces (déploiement continu)
L'en-tête YAML de ce README configure le Space (`sdk: docker`, `app_port: 8501`). La chaîne de déploiement est la suivante :

```
poste local ──git push──► GitHub (wdandou/paris-bornes, main) ──GitHub Action──► Space HF ──build Docker──► application en ligne
```

1. pousser sur la branche `main` du dépôt GitHub ;
2. la GitHub Action `sync-to-hf.yml` recopie la branche sur le Space (`git push --force`, avec le secret GitHub `HF_TOKEN`, un jeton Hugging Face en écriture) ;
3. Hugging Face reconstruit l'image Docker et redémarre l'application ; la première visite déclenche la collecte des données ;
4. les variables optionnelles ci-dessus se renseignent dans *Settings → Variables and secrets* du Space.

---

## Limites et pistes d'amélioration
- **Projections** : croissance exponentielle à taux fixe, avec une cible de 10 VE par borne. Ce sont des hypothèses simplificatrices, et un modèle de Machine Learning pourrait affiner la prévision.
- **Énergie additionnelle** : estimée forfaitairement à 2,5 MWh/an par borne ; la capacité réelle des postes du réseau n'est pas modélisée.
- **Données** : la consommation électrique date de 2022 et la population du recensement 2023. Les statuts IRVE temps réel ne couvrent qu'une partie des points de charge parisiens (beaucoup restent « Inconnu ») ; ceux de Belib' sont complets.
- **Performances** : sur le matériel gratuit de Hugging Face (2 vCPU partagés), les 7 onglets sont recalculés à chaque interaction ; les cartes apparaissent après 10 à 35 secondes. N'exécuter que l'onglet affiché et simplifier le GeoJSON réduiraient ce délai.
- **Persistance** : le disque du conteneur est éphémère ; la base est reconstruite à chaque redémarrage du Space.
- **Évolutions envisagées** :
  - enrichir et harmoniser davantage les sources ;
  - automatiser le pipeline avec Airflow ;
  - créer une version de l'application destinée aux usagers.
