# Projet Big Data — Analyse NBA Play-by-Play
## Résumé pour le PowerPoint (slide par slide)

---

## 1. Présentation du sujet

- **Dataset** : `wyattowalsh/basketball` (Kaggle) — historique NBA complet
- **Cœur de l'étude** : `play_by_play.csv` = 2,26 Go, **13 592 899 événements**, 29 818 matchs, nov. 1996 → juin 2023
- `game.csv` : 65 698 matchs, 1946 → 2023
- **Questions analysées** :
  - déroulé du match : actions, points, fautes, turnovers par minute/quart
  - sélection de tir : 3 pts / proche cercle / mi-distance, par saison et quart
  - paniers assistés vs isolation
  - fin de match : fautes tardives, gestion du chrono, temps morts (arrêtent-ils un run adverse ?)
  - dynamique de score : runs et probabilité de remontée selon le déficit max

---

## 2. Étapes suivies

1. **Ingestion** : téléchargement Kaggle → CSV bruts déposés en Bronze, **jamais modifiés**
2. **Profilage Bronze** : audit automatique (lignes, dates, nulls, doublons, couverture par saison) → `resultats/profilage_bronze.md`
3. **Silver** : schémas explicites, normalisation, dédoublonnage, quarantaine des conflits, contrôles qualité → parquet partitionné
4. **Gold** : enrichissement métier (côté, points, type de tir, marge courante) + agrégats → `data/gold/` + MongoDB
5. **Visualisation** : PNG statiques (Matplotlib) + dashboard Streamlit sur Mongo
6. **Qualité** : tests unitaires sur les règles de normalisation/dédoublonnage (6 tests)

---

## 3. Justification des technologies

| Choix | Justification | Alternative écartée |
|---|---|---|
| **Apache Spark** (standalone, 1 master + 1 worker) | traitement distribué des 13,6 M lignes : groupBy, fenêtres, jointures | Pandas : pas fait pour ce volume ni pour le sujet du cours |
| **Docker Compose** | reproductibilité : `docker compose up` = cluster complet identique partout | installation manuelle machine par machine |
| **CSV conservés en Bronze** | source de vérité immuable, rejouable | modifier la source = perte d'information |
| **Parquet en Silver/Gold** | format colonnaire typé, ~10× plus rapide à relire, compression (506 Mo vs 2,26 Go) | CSV relu à chaque passe = goulot |
| **MongoDB en serving** | documents JSON = colle aux tables d'agrégats ; Streamlit le lit nativement | SQL relationnel : migrations lourdes pour des agrégats simples |
| **Scripts Python** (pas de notebook) | pipeline rejouable, versionné, testable | notebook : état implicite, difficile à automatiser |
| **Kafka écarté** | dataset **historique figé** — pas de flux temps réel → sur-ingénierie | — |
| **HDFS écarté pour Silver** | cluster mono-nœud pédagogique : le disque monté suffit, Pierre lit le parquet directement | HDFS : réplication inutile à 1 nœud |

---

## 4. Schéma d'architecture (à dessiner)

```
                        ┌─────────────────────────────────────────────┐
                        │               Docker Compose                 │
                        │                                             │
   Kaggle (API) ──► [ingest_kaggle.py]                                │
                        │                                             │
                        ▼                                             │
   BRONZE   data/archive/csv/   (16 CSV bruts, immuables)             │
            2,26 Go — 13,6 M évts                                     │
                        │                                             │
              [profile_bronze.py] ──► profilage_bronze.md             │
                        │                                             │
                [silver.py]  (Spark: master + worker)                 │
                        │                                             │
                        ▼                                             │
   SILVER   data/silver/                                              │
            ├── games/            65 642 lignes (1/match)             │
            ├── play_by_play/     13 585 539 lignes, partition annee  │
            └── _quarantine/      conflits & anomalies tracés         │
                        │                                             │
                [analyses.py]  (Spark)                                │
                        │                                             │
                        ▼                                             │
   GOLD     data/gold/<12 tables parquet>                             │
            resultats/*.png (9 figures)                               │
                        │                                             │
                        ▼                                             │
   SERVING  MongoDB (base "nba", ~12 collections)                     │
                        │                                             │
                [dashboard Streamlit :8501]                           │
                        └─────────────────────────────────────────────┘

Services Docker : spark-master, spark-worker (4c/6Go), spark-history,
jupyter (driver), mongo, dashboard. (namenode/datanode : optionnels,
HDFS non utilisé dans la version finale — choix KIS.)
```

---

## 5. Diagramme d'activités (flux de traitement)

```
[Kaggle CSV]
     │
     ▼
Bronze : dépôt brut + profilage ──► rapport qualité source
     │
     ▼
Silver :
  schéma explicite (ids en string)
       │
  normalisation season_type / dates / saison
       │
  dédupliquer doublons EXACTS
       │
  clés encore dupliquées ? ──oui──► QUARANTAINE (visible, pas supprimé)
       │ non
  rattacher chaque événement à son match ──orphelin──► QUARANTAINE
       │
  parser horloge "M:SS" → sec restantes/écoulées (flag clock_ok)
  parser score "88-82" → score_home/away (flag score_ok)
       │
  écrire parquet partitionné par saison
       │
  contrôles finaux : score final vs game.csv, couverture par saison
       │
     ▼
Gold :
  enrichissement métier (side, pts, type tir, marge courante, assists)
       │
  ~12 agrégats métier ──► data/gold/ + MongoDB + PNG
       │
     ▼
Dashboard Streamlit
```

---

## 6. Résultats obtenus

### Qualité de la donnée (Silver)
| Contrôle | Résultat |
|---|---|
| pbp : 13 592 899 → **13 585 539** | −7 360 doublons exacts détectés dans la source |
| game : 65 698 → **65 642** | −56 doublons (variantes « All Star »/« All-Star ») |
| Conflits de clé | 94 dans `game_summary` → quarantaine |
| Orphelins / clés nulles / périodes / horloges invalides | **0** |
| Score final pbp ≠ game.csv | 216 matchs → quarantaine documentée |
| Compression | 2,26 Go CSV → ~506 Mo parquet |

### Figures produites (dossier `resultats/`)
- `02_actions_minute.png`, `02_quart_temps.png` — déroulé du match
- `03_evolution_3pts.png`, `03_tirs_quart.png`, `03_assists.png` — tirs & passes
- `04_chrono.png`, `04_fautes.png`, `04_timeouts.png` — fin de match
- `05_runs.png`, `05_remontees.png` — séries & remontées
- Dashboard Streamlit interactif (port 8501) sur MongoDB

---

## 7. Pistes IA / ML (perspectives)

Le projet est fondé sur des agrégats ; le ML peut enrichir :
- **Win probability model** : prédire la victoire à chaque instant (features : marge, temps restant, possession) — régression logistique / gradient boosting sur la Silver
- **Détection de « clutch »** : clustering des joueurs sur performance en fin de match serré
- **Prédiction de runs** : modèle séquentiel (l'état de momentum précède-t-il un run ?)
- **Détection de fautes intentionnelles** : classification sur contexte (temps, marge, cible) — aujourd'hui on ne les **compte** que par heuristique

---

## 8. Limites et perspectives

### Limites constatées
- **Couverture pbp** : 1996→2023 seulement — pas d'analyse par minute avant 1996
- **Pas de coordonnées de tir** : la « zone » est inférée depuis la description textuelle, pas mesurée
- **Fautes intentionnelles** : identifiées par contexte, pas avec certitude
- **Incohérences de source** : 27 193 divergences `pts` entre `game` et `line_score`, 216 scores finaux discordants — documentés, pas résolus (ambiguïté de la source)
- **Mono-nœud** : cluster pédagogique — pas de tolérance de panne réelle

### Perspectives
- Passer en **vrai lakehouse** : MinIO + Delta Lake/Iceberg (ACID, time travel, catalog)
- Ajouter les coordonnées de tir (shot charts) via un dataset complémentaire
- Industrialiser les contrôles qualité (Great Expectations / dbt tests)
- Scaling : cluster multi-workers réel, ou service managé (EMR/Dataproc)
- Croiser avec données de paris/blessures pour le volet ML

---

## Chiffres clés à retenir

- **13 592 899** événements bruts → **13 585 539** en Silver
- **29 818** matchs play-by-play, 27 saisons (1996–2022)
- **506 Mo** parquet vs 2,26 Go CSV
- **6/6** tests unitaires — **0** orphelin, **0** clé nulle
- **94 + 216** anomalies réelles trouvées et tracées en quarantaine
