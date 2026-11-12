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
data/archive/csv/  ->  Spark (master + worker)  ->  HDFS  /nba/pbp (parquet)
                                              ->  MongoDB nba (14 collections d'agregats)
                                              ->  resultats/*.png (figures)
```

| Étape | Techno |
|---|---|
| Ingestion | Spark batch (dataset statique) |
| Traitement | PySpark — DataFrames, fenêtres, jointures |
| Persistance traitée | HDFS (parquet) |
| Persistance serving | MongoDB |
| Viz | matplotlib / seaborn, JupyterLab |

Pas de Kafka : la donnée est historique (batch), pas un flux temps réel — Kafka est cité comme piste d'amélioration (rejouer un match en stream).

## Lancer

```bash
docker compose up -d          # monte spark master/worker/history, jupyter, mongo, hdfs
# dataset : placer l'extraction Kaggle dans data/archive/ (non versionne, ~4,5 Go)
# jupyter : http://localhost:8888 -> notebooks/analyse_nba.ipynb
# spark UI : http://localhost:8080 | HDFS : http://localhost:9870 | history : http://localhost:18080
```

## Résultats clés

- Part des tirs à 3 pts : 0 % avant 1979 -> ~40 % aujourd'hui (le recul du mi-distance)
- ~2,9 runs ≥ 8-0 par match, 9,6 pts de moyenne
- Les temps morts pendant un run adverse ne le cassent que 13 % du temps
- Déficit max subi : 15-19 pts -> 17 % de victoires ; 20+ -> ~3 % (« match plié »)

## Structure

- `notebooks/analyse_nba.ipynb` — pipeline + analyses exécutées (le cœur du projet)
- `notebooks/gen_nb.py` — générateur du notebook
- `docker-compose.yml`, `hadoop.env`, `docker/`, `conf/` — environnement
- `resultats/` — les 10 figures pour le rapport
- `data/` — dataset Kaggle (ignoré par git)
