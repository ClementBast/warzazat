import argparse
import os
import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, Window

sys.path.insert(0, os.path.dirname(__file__))
from schemas import SCHEMAS  # schemas Spark explicites declares dans schemas.py

# Tout le pipeline vit dans HDFS : bronze en entree (/nba/raw), silver en sortie
CSV = "hdfs://namenode:9000/nba/raw"
OUT = "hdfs://namenode:9000/nba/silver"   # data lake HDFS : namenode service docker
Q = f"{OUT}/_quarantine"          # lignes douteuses ecartees : tracees, pas supprimees
REPORT = "/work/resultats/controles_silver.md"

PERIOD_MAX = 14  # 4 quarts + jusqu'a 10 prolongations


# ============================================================================
# FONCTIONS UTILITAIRES : normalisation, dedup, parsing, ecriture
# ============================================================================

def normalize_season_type(df):
    # Uniformise les variantes d'ecriture : "All Star", "All-Star",
    # "ALL-STAR" -> "All-Star". La source code la meme chose de plusieurs
    # facons ; sans ca, deux lignes "identiques" ne le sont pas pour Spark.
    # Fait AVANT la dedup : c'est precisement ce qui fait s'effondrer les
    # 56 doublons All-Star (meme match ecrit deux fois avec libelle different).
    st = F.lower(F.trim(F.regexp_replace("season_type", "[\\-_]", " ")))
    return df.withColumn("season_type",
        F.when(st.like("%all%star%"), F.lit("All-Star"))
         .when(st.like("%regular%"), F.lit("Regular Season"))
         .when(st.like("%playoff%"), F.lit("Playoffs"))
         .when(st.like("%pre%season%"), F.lit("Pre Season"))
         .when(st.like("%cup%"), F.lit("NBA Cup"))
         .otherwise(F.col("season_type")))


def dedup_exact(df):
    # Supprime uniquement les lignes STRICTEMENT identiques (les 37 colonnes
    # egales). Supprimer des doublons "presque pareils" serait destructeur.
    return df.dropDuplicates()


def split_key_conflicts(df, keys):
    # Apres la dedup exacte, il peut rester des cles en double avec des
    # VALEURS DIFFERENTES (ex : meme game_id, deux pts differents).
    # Ce sont des conflits : impossible de choisir la bonne ligne ->
    # on les Ecarte en quarantaine plutot que de les supprimer en silence.
    # Retourne (dataframe propre, dataframe des conflits).
    # left_anti = jointure "garde les lignes SANS correspondance" (le propre)
    # inner      = jointure classique "garde les correspondances" (les conflits)
    bad = (df.groupBy(*keys).count().filter(F.col("count") > 1).select(*keys))
    return df.join(bad, keys, "left_anti"), df.join(bad, keys, "inner")


def parse_clock(df):
    # pctimestring = texte "MM:SS" du temps restant dans la periode ("8:30").
    # On le transforme en rest = secondes restantes (8*60+30 = 510).
    # Le flag clock_ok dit si le format etait lisible :
    #   True  -> rest contient les secondes
    #   False -> le texte etait absent/bizarre, rest = NULL
    # On met NULL et pas 0 : "inconnu" n'est pas "zero seconde".
    # rlike = test par expression reguliere sur la colonne entiere.
    m = F.split("pctimestring", ":")
    ok = F.col("pctimestring").rlike("^[0-9]+:[0-9]{2}$")
    return (df.withColumn("clock_ok", F.coalesce(ok, F.lit(False)))
              .withColumn("rest", F.when(ok, m.getItem(0).cast("int") * 60
                                          + m.getItem(1).cast("int"))))


def parse_score(df):
    # score = texte "88 - 82" (ext - dom). On en tire 2 colonnes numeriques.
    # ATTENTION : le score n'est renseigne QUE sur les actions de points.
    # Sur un rebond ou une faute la case est vide -> score_ok = False et
    # les deux colonnes restent NULL. Mettre 0 fausserait tout : "pas de
    # score affiche" ne veut pas dire "score 0-0".
    ok = F.col("score").rlike("^ *[0-9]+ *- *[0-9]+ *$")
    s = F.split("score", "-")
    return (df.withColumn("score_ok", F.coalesce(ok, F.lit(False)))
              .withColumn("score_away", F.when(ok, s.getItem(0).cast("int")))
              .withColumn("score_home", F.when(ok, s.getItem(1).cast("int"))))


def write_parquet(df, path, part=None):
    # Ecriture parquet en ecrasement ; part="annee" -> dossiers annee=1996/...
    w = df.write.mode("overwrite")
    (w.partitionBy(part) if part else w).parquet(path)


def md_table(headers, rows):
    # Fabrique un tableau markdown pour le rapport de controle
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def main(sample=None):
    spark = SparkSession.builder.appName("nba-silver").getOrCreate()
    L = ["# Contrôles Silver\n"]  # L = lignes du rapport markdown
    R = {}                        # R = compteurs des controles

    # ========================================================================
    # TABLE games : game.csv + 4 tables auxiliaires -> 1 ligne par game_id
    # ========================================================================
    game = (spark.read.schema(SCHEMAS["game"]).option("header", "true")
            .csv(f"{CSV}/game.csv"))
    game = normalize_season_type(game)
    n0 = game.count()
    game = dedup_exact(game)
    R["game_lignes"] = n0
    R["game_doublons_exact"] = n0 - game.count()  # 56 doublons All-Star attendus
    game, gconf = split_key_conflicts(game, ["game_id"])
    R["game_conflits"] = gconf.count()
    if R["game_conflits"]:
        write_parquet(gconf, f"{Q}/games_conflicts")
    # annee = annee de debut de saison, extraite de season_id.
    # season_id est code "type+annee" : 22017 -> 2017 (saison 2017-18).
    games = (game.withColumn("annee", F.col("season_id").cast("int") % 10000)
                 .withColumn("game_date", F.to_date("game_date")))
    R["games_silver"] = games.count()
    R["games_cle_nulle"] = games.filter(F.col("game_id").isNull()).count()

    # AUX = pour chaque table auxiliaire, la liste des colonnes a apporter
    # dans games. Piege : ces tables redeclarent des colonnes deja dans game
    # (team_id, abbreviations...) ; si on les joignait telles quelles, Spark
    # obtiendrait 2 colonnes du meme nom -> erreur COLUMN_ALREADY_EXISTS.
    # Donc on ne selectionne que le nouveau : scores par quart (line_score),
    # attendance (game_info), statut (game_summary), stats paint/leads
    # (other_stats).
    AUX = {"line_score": ["game_id"] + [c for c in SCHEMAS["line_score"].fieldNames()
                                        if c.startswith("pts_qtr") or c.startswith("pts_ot")]
                          + ["ls_pts_home", "ls_pts_away"],
           "game_info": ["game_id", "attendance", "game_time"],
           "game_summary": ["game_id", "game_status_text", "season",
                            "natl_tv_broadcaster_abbreviation"],
           "other_stats": [c for c in SCHEMAS["other_stats"].fieldNames()
                          if c not in ("team_id_home", "team_id_away",
                                       "team_abbreviation_home", "team_abbreviation_away",
                                       "team_city_home", "team_city_away", "league_id")]}
    for name, cols in AUX.items():
        a = spark.read.schema(SCHEMAS[name]).option("header", "true").csv(f"{CSV}/{name}.csv")
        if name == "line_score":
            a = (a.withColumnRenamed("pts_home", "ls_pts_home")
                  .withColumnRenamed("pts_away", "ls_pts_away"))
        n = a.count()
        a = dedup_exact(a)
        R[f"{name}_doublons_exact"] = n - a.count()
        a, conf = split_key_conflicts(a, ["game_id"])
        R[f"{name}_conflits"] = conf.count()
        if R[f"{name}_conflits"]:
            write_parquet(conf, f"{Q}/{name}_conflicts")
        games = games.join(a.select(*cols), "game_id", "left")
        # left join : si le match n'a pas de ligne dans l'auxiliaire, on le
        # garde avec NULL (un match ne doit pas disparaitre pour ca)

    # Controle croise de coherence : le score final de game.csv (pts_home/
    # pts_away) devrait egaler le total de line_score (ls_pts_*). On compte
    # les ecarts sans rien corriger : divergence entre sources, documentee.
    R["pts_game_vs_linescore_ecarts"] = games.filter(
        F.col("pts_home").isNotNull() & F.col("ls_pts_home").isNotNull()
        & ((F.col("pts_home") != F.col("ls_pts_home"))
           | (F.col("pts_away") != F.col("ls_pts_away")))).count()

    write_parquet(games, f"{OUT}/games")

    # ========================================================================
    # TABLE play_by_play : 13,6 M evenements -> 1 ligne par (game_id, eventnum)
    # ========================================================================

    # --sample N : mode test rapide. On prend les N matchs les plus recents
    # et on filtre le pbp par jointure. broadcast(keep) = Spark envoie la
    # petite table (N ids) en entier a chaque worker au lieu de melanger les
    # deux grosses tables sur le reseau (shuffle) -> beaucoup plus rapide.
    pbp = (spark.read.schema(SCHEMAS["play_by_play"]).option("header", "true")
           .csv(f"{CSV}/play_by_play.csv"))
    if sample:
        keep = (games.orderBy(F.desc("annee")).limit(sample)
                .select("game_id"))
        pbp = pbp.join(F.broadcast(keep), "game_id")
        R["mode"] = f"sample ({sample} matchs)"

    # Controle structurel, UNE SEULE PASSE sur le CSV : chaque ligne donne
    # +1 a chaque compteur defaillant. agg() sans groupBy = agregation
    # globale ; .first() recupere la ligne de resultats.
    # Optimisation cle : 3 controles en 1 scan au lieu de 3 scans de 2,26 Go.
    st = pbp.agg(
        F.count("*").alias("n"),
        F.sum(F.when(F.col("game_id").isNull() | F.col("eventnum").isNull(), 1)
              .otherwise(0)).alias("cles_nulles"),
        F.sum(F.when(F.col("period").isNull() | (F.col("period") < 1)
                     | (F.col("period") > PERIOD_MAX), 1).otherwise(0)).alias("periode_inv"),
        F.sum(F.when(~F.coalesce(F.col("pctimestring").rlike("^[0-9]+:[0-9]{2}$"),
                                 F.lit(False)), 1).otherwise(0)).alias("horloge_inv")
    ).first()
    R["pbp_lignes"] = st.n
    R["pbp_cles_nulles"] = st.cles_nulles
    R["pbp_periodes_invalides"] = st.periode_inv
    R["pbp_horloges_invalides"] = st.horloge_inv

    # Filtre structurel : on enleve les lignes inutilisables (cle nulle ou
    # periode hors 1..14 — elles sont deja comptees dans le rapport).
    # PUIS persist(DISK_ONLY) : Spark est PARESSEUX — rien n'est calcule
    # tant qu'on ne demande pas un resultat. Sans persist, chaque etape
    # ci-dessous relirait et re-parserait le CSV de 2,26 Go. Avec, le
    # dataframe filtre est fige sur disque UNE fois et reutilise partout.
    pbp = pbp.filter(F.col("game_id").isNotNull() & F.col("eventnum").isNotNull()
                     & F.col("period").isNotNull() & (F.col("period") >= 1)
                     & (F.col("period") <= PERIOD_MAX))
    # DISK_ONLY = cache local des workers (pas HDFS) : c'est juste un cache
    # de calcul, la SORTIE elle part bien sur HDFS via write_parquet
    pbp = pbp.persist(StorageLevel.DISK_ONLY)
    cached = pbp  # reference gardee pour liberer le cache a la fin

    # Dedoublonnage en 2 temps (l'optimisation qui compte) :
    # Comparer 37 colonnes sur 13,6 M de lignes = enorme. A la place :
    #   1) dupkeys = groupBy sur 2 petites colonnes (game_id, eventnum) ->
    #      trouve les CLES dupliquees. Operation legere.
    #   2) dup_rows = on ne touche qu'aux lignes de ces cles-la (peu nombreuses)
    #   3) distinct_rows = le vrai dropDuplicates() se fait seulement sur
    #      dup_rows : compare 37 colonnes mais sur ~10k lignes, pas 13,6 M
    #   4) conf_keys = cles qui survivent ENCORE en double apres dedup exact
    #      = lignes differentes sous la meme cle = CONFLITS -> quarantaine
    dupkeys = (pbp.groupBy("game_id", "eventnum").count()
               .filter(F.col("count") > 1).select("game_id", "eventnum"))
    dup_rows = pbp.join(dupkeys, ["game_id", "eventnum"])
    distinct_rows = dup_rows.dropDuplicates()
    conf_keys = (distinct_rows.groupBy("game_id", "eventnum").count()
                 .filter(F.col("count") > 1).select("game_id", "eventnum"))
    conf = dup_rows.join(conf_keys, ["game_id", "eventnum"], "left_semi")
    # left_semi = "garde les lignes qui ONT une correspondance" (filtre par join)
    R["pbp_conflits"] = conf.count()
    R["pbp_doublons_exact"] = dup_rows.count() - conf.count() \
        - distinct_rows.join(conf_keys, ["game_id", "eventnum"], "left_anti").count()
    if R["pbp_conflits"]:
        write_parquet(conf, f"{Q}/pbp_conflits")
    # Recomposition de la table propre =
    #   (toutes les lignes jamais dupliquees : left_anti contre dupkeys)
    # + (un seul exemplaire de chaque doublon sain : distinct_rows moins
    #    les conflits) -> unionByName empile les deux morceaux
    pbp = (pbp.join(dupkeys, ["game_id", "eventnum"], "left_anti")
           .unionByName(distinct_rows.join(conf_keys, ["game_id", "eventnum"],
                                         "left_anti")))

    # Rattachement aux matchs : jointure left sur game_id avec games.
    # - ga fait ~65k lignes -> broadcast : chaque worker recoit la petite
    #   table entiere, la jointure se fait en local, sans shuffle reseau
    # - si un evenement a un game_id absent de games, annee vaut NULL :
    #   c'est un ORPHELIN -> compte puis quarantaine
    # - bonus : la jointure nous apporte "annee" gratuitement, qui servira
    #   de colonne de partition pour l'ecriture parquet
    ga = games.select("game_id", "annee")
    pbp = pbp.join(F.broadcast(ga), "game_id", "left")
    orphans = pbp.filter(F.col("annee").isNull())
    R["pbp_orphelins"] = orphans.count()
    if R["pbp_orphelins"]:
        write_parquet(orphans, f"{Q}/pbp_orphelins")
    pbp = pbp.filter(F.col("annee").isNotNull())

    # Parsing : horloge "M:SS" -> rest (sec restantes), score "a-b" ->
    # score_away/home, puis sec = TEMPS ECOULE DEPUIS LE DEBUT du match :
    #   periode 1-4 : (period-1)*720 s deje jouees + (720 - rest) dans celle-ci
    #   prolongation : 4*720 = 2880 s + (period-5)*300 + (300 - rest)
    # C'est "sec" qui permet de comparer les actions entre elles sur une
    # seule echelle de temps (0 -> 2880+), quel que soit le quart.
    pbp = parse_score(parse_clock(pbp))
    pbp = pbp.withColumn("sec",
        F.when(F.col("period") <= 4, (F.col("period") - 1) * 720 + 720 - F.col("rest"))
         .otherwise(2880 + (F.col("period") - 5) * 300 + 300 - F.col("rest")))
    # Ecriture partitionnee par saison : cree annee=1996/, annee=1997/...
    # Relire "la saison 2017" ne touche que son dossier, pas les 27 saisons.
    write_parquet(pbp, f"{OUT}/play_by_play", part="annee")
    # On RELIT le parquet ecrit pour compter : si ca plante ou que le chiffre
    # est incoherent, on le sait tout de suite (preuve que la sortie est bonne)
    R["pbp_silver"] = spark.read.parquet(f"{OUT}/play_by_play").count()

    # ========================================================================
    # CONTROLES FINAUX + RAPPORT
    # ========================================================================

    # Controle du score final : le DERNIER score affiche dans le pbp d'un
    # match devrait egaler le score final officiel de game.csv.
    # Window = fonction fenetre : on groupe par match, on trie par eventnum
    # decroissant, et row_number() numerote 1, 2, 3... -> rn=1 = le dernier
    # evenement AVEC un score (d'ou le filtre score_ok avant).
    w = Window.partitionBy("game_id").orderBy(F.desc("eventnum"))
    last = (pbp.filter(F.col("score_ok"))
            .withColumn("rn", F.row_number().over(w)).filter("rn = 1")
            .select("game_id", "score_home", "score_away"))
    cmp_ = last.join(games.select("game_id", F.col("pts_home").alias("h"),
                                  F.col("pts_away").alias("a")), "game_id")
    R["score_final_matchs_verifies"] = cmp_.count()
    R["score_final_ecarts"] = cmp_.filter((F.col("score_home") != F.col("h"))
                                          | (F.col("score_away") != F.col("a"))).count()
    if R["score_final_ecarts"]:
        write_parquet(cmp_.filter((F.col("score_home") != F.col("h"))
                                  | (F.col("score_away") != F.col("a"))),
                      f"{Q}/score_final_ecarts")

    # Couverture : matchs + evenements par saison -> tableau du rapport,
    # montre visuellement que le pbp ne couvre que 1996->2023
    cov = (pbp.groupBy("annee").agg(F.countDistinct("game_id").alias("matchs_pbp"),
                                   F.count("*").alias("evenements"))
           .orderBy("annee").collect())
    cached.unpersist()  # libere le cache disque

    # Rapport final : on verse dans controles_silver.md 1) la table
    # Bronze -> Silver (lignes avant/apres + ce qui a ete ecarte),
    # 2) tous les compteurs R, 3) la couverture par saison.
    L.append("## État des données — Bronze → Silver\n")
    evol = [("game + aux", "CSV /nba/raw", R["game_lignes"],
             R["games_silver"], "parquet data/silver/games",
             f"{R['game_doublons_exact']} doublons exacts, {R['game_conflits']} conflits -> quarantaine"),
            ("play_by_play", "CSV /nba/raw", R["pbp_lignes"],
             R["pbp_silver"], "parquet data/silver/play_by_play (partition annee)",
             f"{R['pbp_doublons_exact']} doublons exacts, {R['pbp_conflits']} conflits, "
             f"{R['pbp_orphelins']} orphelins, {R['pbp_cles_nulles']} cles nulles, "
             f"{R['pbp_periodes_invalides']} periodes invalides")]
    L.append(md_table(["table", "bronze", "lignes bronze", "lignes silver", "sortie", "écarts"],
                      evol))
    L.append("\n## Comptes\n")
    L.append(md_table(["contrôle", "valeur"],
                      [(k, f"{v:,}".replace(",", " ") if isinstance(v, (int, float)) else v)
                       for k, v in R.items()]))
    L.append("\n## Couverture play-by-play par saison (silver)\n")
    L.append(md_table(["saison", "matchs", "événements"],
                      [(r.annee, r.matchs_pbp, r.evenements) for r in cov]))
    os.makedirs("/work/resultats", exist_ok=True)
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    for k, v in R.items():
        print(f"{k}: {v}")
    print(f"rapport : {REPORT}")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=None,
                    help="limiter aux N matchs les plus recents (test rapide)")
    main(ap.parse_args().sample)
