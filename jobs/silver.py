import argparse
import os
import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, Window

sys.path.insert(0, os.path.dirname(__file__))
from schemas import SCHEMAS

CSV = "hdfs://namenode:9000/nba/raw"
OUT = "hdfs://namenode:9000/nba/silver"
Q = f"{OUT}/_quarantine"
REPORT = "/work/resultats/controles_silver.md"

PERIOD_MAX = 14


def normalize_season_type(df):
    st = F.lower(F.trim(F.regexp_replace("season_type", "[\\-_]", " ")))
    return df.withColumn("season_type",
        F.when(st.like("%all%star%"), F.lit("All-Star"))
         .when(st.like("%regular%"), F.lit("Regular Season"))
         .when(st.like("%playoff%"), F.lit("Playoffs"))
         .when(st.like("%pre%season%"), F.lit("Pre Season"))
         .when(st.like("%cup%"), F.lit("NBA Cup"))
         .otherwise(F.col("season_type")))


def dedup_exact(df):
    return df.dropDuplicates()


def split_key_conflicts(df, keys):
    bad = (df.groupBy(*keys).count().filter(F.col("count") > 1).select(*keys))
    return df.join(bad, keys, "left_anti"), df.join(bad, keys, "inner")


def parse_clock(df):
    m = F.split("pctimestring", ":")
    ok = F.col("pctimestring").rlike("^[0-9]+:[0-9]{2}$")
    return (df.withColumn("clock_ok", F.coalesce(ok, F.lit(False)))
              .withColumn("rest", F.when(ok, m.getItem(0).cast("int") * 60
                                          + m.getItem(1).cast("int"))))


def parse_score(df):
    ok = F.col("score").rlike("^ *[0-9]+ *- *[0-9]+ *$")
    s = F.split("score", "-")
    return (df.withColumn("score_ok", F.coalesce(ok, F.lit(False)))
              .withColumn("score_away", F.when(ok, s.getItem(0).cast("int")))
              .withColumn("score_home", F.when(ok, s.getItem(1).cast("int"))))


def write_parquet(df, path, part=None):
    w = df.write.mode("overwrite")
    (w.partitionBy(part) if part else w).parquet(path)


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def main(sample=None):
    spark = SparkSession.builder.appName("nba-silver").getOrCreate()
    L = ["# Contrôles Silver\n"]
    R = {}

    game = (spark.read.schema(SCHEMAS["game"]).option("header", "true")
            .csv(f"{CSV}/game.csv"))
    game = normalize_season_type(game)
    n0 = game.count()
    game = dedup_exact(game)
    R["game_lignes"] = n0
    R["game_doublons_exact"] = n0 - game.count()
    game, gconf = split_key_conflicts(game, ["game_id"])
    R["game_conflits"] = gconf.count()
    if R["game_conflits"]:
        write_parquet(gconf, f"{Q}/games_conflicts")
    games = (game.withColumn("annee", F.col("season_id").cast("int") % 10000)
                 .withColumn("game_date", F.to_date("game_date")))
    R["games_silver"] = games.count()
    R["games_cle_nulle"] = games.filter(F.col("game_id").isNull()).count()

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

    R["pts_game_vs_linescore_ecarts"] = games.filter(
        F.col("pts_home").isNotNull() & F.col("ls_pts_home").isNotNull()
        & ((F.col("pts_home") != F.col("ls_pts_home"))
           | (F.col("pts_away") != F.col("ls_pts_away")))).count()

    write_parquet(games, f"{OUT}/games")

    pbp = (spark.read.schema(SCHEMAS["play_by_play"]).option("header", "true")
           .csv(f"{CSV}/play_by_play.csv"))
    if sample:
        keep = (games.orderBy(F.desc("annee")).limit(sample)
                .select("game_id"))
        pbp = pbp.join(F.broadcast(keep), "game_id")
        R["mode"] = f"sample ({sample} matchs)"
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

    pbp = pbp.filter(F.col("game_id").isNotNull() & F.col("eventnum").isNotNull()
                     & F.col("period").isNotNull() & (F.col("period") >= 1)
                     & (F.col("period") <= PERIOD_MAX))
    pbp = pbp.persist(StorageLevel.DISK_ONLY)
    cached = pbp

    dupkeys = (pbp.groupBy("game_id", "eventnum").count()
               .filter(F.col("count") > 1).select("game_id", "eventnum"))
    dup_rows = pbp.join(dupkeys, ["game_id", "eventnum"])
    distinct_rows = dup_rows.dropDuplicates()
    conf_keys = (distinct_rows.groupBy("game_id", "eventnum").count()
                 .filter(F.col("count") > 1).select("game_id", "eventnum"))
    conf = dup_rows.join(conf_keys, ["game_id", "eventnum"], "left_semi")
    R["pbp_conflits"] = conf.count()
    R["pbp_doublons_exact"] = dup_rows.count() - conf.count() \
        - distinct_rows.join(conf_keys, ["game_id", "eventnum"], "left_anti").count()
    if R["pbp_conflits"]:
        write_parquet(conf, f"{Q}/pbp_conflits")
    pbp = (pbp.join(dupkeys, ["game_id", "eventnum"], "left_anti")
           .unionByName(distinct_rows.join(conf_keys, ["game_id", "eventnum"],
                                         "left_anti")))

    ga = games.select("game_id", "annee")
    pbp = pbp.join(F.broadcast(ga), "game_id", "left")
    orphans = pbp.filter(F.col("annee").isNull())
    R["pbp_orphelins"] = orphans.count()
    if R["pbp_orphelins"]:
        write_parquet(orphans, f"{Q}/pbp_orphelins")
    pbp = pbp.filter(F.col("annee").isNotNull())

    pbp = parse_score(parse_clock(pbp))
    pbp = pbp.withColumn("sec",
        F.when(F.col("period") <= 4, (F.col("period") - 1) * 720 + 720 - F.col("rest"))
         .otherwise(2880 + (F.col("period") - 5) * 300 + 300 - F.col("rest")))
    write_parquet(pbp, f"{OUT}/play_by_play", part="annee")
    R["pbp_silver"] = spark.read.parquet(f"{OUT}/play_by_play").count()

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

    cov = (pbp.groupBy("annee").agg(F.countDistinct("game_id").alias("matchs_pbp"),
                                   F.count("*").alias("evenements"))
           .orderBy("annee").collect())
    cached.unpersist()

    L.append("## État des données — Bronze → Silver\n")
    evol = [("game + aux", "CSV /nba/raw", R["game_lignes"],
             R["games_silver"], "parquet /nba/silver/games",
             f"{R['game_doublons_exact']} doublons exacts, {R['game_conflits']} conflits -> quarantaine"),
            ("play_by_play", "CSV /nba/raw", R["pbp_lignes"],
             R["pbp_silver"], "parquet /nba/silver/play_by_play (partition annee)",
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
