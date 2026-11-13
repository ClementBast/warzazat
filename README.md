# Projet Big Data — NBA : déroulé d'un match, choix de tir et stratégies de fin de match

Analyse du dataset Kaggle [`wyattowalsh/basketball`](https://www.kaggle.com/datasets/wyattowalsh/basketball)
(~2,2 Go de play-by-play, 13,6 M d'événements, matchs depuis 1946).

## Sujet

- **Déroulé d'un match** : répartition des actions minute par minute, comparaison des quart-temps
- **Choix de tir** : part des 3 pts / proche cercle / mi-distance par QT et par saison, assisté vs isolation
- **Fin de match** : fautes volontaires, gestion du chrono par l'équipe qui mène, efficacité des temps morts
- **Dynamiques du score** : séries (runs) et remontées — à partir de quel écart un match est « plié »

## Architecture

```
BRONZE  data/archive/csv/          (brut, jamais modifié)
   |    silver.py (Spark) : schémas, normalisation, dédoublonnage,
   |    quarantaine des conflits, contrôles qualité
   v
SILVER  data/silver/games          (1 ligne / game_id, + line_score/info/summary/other_stats)
        data/silver/play_by_play   (1 ligne / (game_id,eventnum), horloge parsée, partitionné par saison)
        data/silver/_quarantine/   (conflits et anomalies écartés, pas supprimés)
   |    analyses.py (Spark) : enrichissement métier + agrégats
   v
GOLD    data/gold/ (parquet : pbp enrichi + tables agrégées)
        MongoDB nba (14 collections)  ->  dashboard Streamlit :8501
        resultats/*.png               ->  figures du rapport
```

| Étape | Techno |
|---|---|
| Ingestion | kagglehub (dataset public, anonyme) |
| Traitement | PySpark — DataFrames, fenêtres, jointures, contrôles qualité |
| Persistance silver | Parquet local (`data/silver/`), partitionné par saison |
| Persistance serving | MongoDB |
| Viz | Streamlit + Plotly ; matplotlib pour les figures du rapport |

Pas de Kafka : la donnée est historique (batch), pas un flux temps réel — Kafka est cité comme piste d'amélioration (rejouer un match en stream).

## Lancer

```bash
docker compose up -d          # monte spark master/worker/history, jupyter, mongo, hdfs, dashboard

# pipeline (dans le conteneur jupyter)
docker exec projetgroupe-jupyter-1 python3 /work/jobs/ingest_kaggle.py    # Kaggle -> data/archive/ (anonyme)
docker exec projetgroupe-jupyter-1 python3 /work/jobs/profile_bronze.py   # rapport -> resultats/profilage_bronze.md
docker exec projetgroupe-jupyter-1 python3 /work/jobs/silver.py           # bronze -> data/silver/ (ajouter --sample N pour un test rapide)
docker exec projetgroupe-jupyter-1 python3 /work/jobs/analyses.py         # silver -> Mongo + resultats/*.png (couche gold)
docker exec projetgroupe-jupyter-1 python3 /work/jobs/test_silver.py      # tests unitaires des règles silver

# spark UI : http://localhost:8080 | HDFS : http://localhost:9870 | history : http://localhost:18080
# dashboard : http://localhost:8501 (Streamlit, lit les collections Mongo)
```

## Résultats clés

- Part des tirs à 3 pts : 0 % avant 1979 -> ~40 % aujourd'hui (le recul du mi-distance)
- ~2,9 runs ≥ 8-0 par match, 9,6 pts de moyenne
- Les temps morts pendant un run adverse ne le cassent que 13 % du temps
- Déficit max subi : 15-19 pts -> 17 % de victoires ; 20+ -> ~3 % (« match plié »)

## Structure

- `jobs/` — pipeline en scripts (workflow propre) :
  - `ingest_kaggle.py` — télécharge le dataset Kaggle → `data/archive/` (`--hdfs` : pousse aussi vers `hdfs:///nba/raw`)
  - `schemas.py` — schémas Spark explicites (ids en string)
  - `profile_bronze.py` — profilage source → `resultats/profilage_bronze.md`
  - `silver.py` — bronze → `data/silver/` : normalisation, dédup exact, quarantaine des conflits, contrôles (`--sample N` pour itérer vite)
  - `test_silver.py` — tests des règles de normalisation/dédoublonnage
  - `analyses.py` — silver → enrichissement + agrégats → MongoDB + `resultats/*.png`
- `dashboard/app.py` — dashboard Streamlit (couche serving Mongo → Plotly)
- `docker-compose.yml`, `hadoop.env`, `docker/`, `conf/` — environnement
- `resultats/` — les 10 figures pour le rapport
- `data/` — dataset Kaggle (ignoré par git)
