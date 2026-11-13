import os
import sys

from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, os.path.dirname(__file__))
from schemas import SCHEMAS

CSV = "hdfs://namenode:9000/nba/raw"
OUT = "/work/resultats/profilage_bronze.md"

TABLES = ["game", "line_score", "game_info", "game_summary", "other_stats", "play_by_play"]
KEYS = {"game": "game_id", "line_score": "game_id", "game_info": "game_id",
        "game_summary": "game_id", "other_stats": "game_id", "play_by_play": None}
DATES = {"game": "game_date", "line_score": "game_date_est", "game_summary": "game_date_est"}


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def main():
    spark = SparkSession.builder.appName("nba-profile-bronze").getOrCreate()
    dfs = {t: spark.read.schema(SCHEMAS[t]).option("header", "true").csv(f"{CSV}/{t}.csv")
           for t in TABLES}
    L = []

    L.append("# Profilage Bronze — hdfs /nba/raw\n")
    rows = []
    for t in TABLES:
        df = dfs[t]
        n = df.count()
        r = [f"`{t}.csv`", f"{n:,}".replace(",", " ")]
        if t in DATES:
            mm = df.agg(F.min(F.to_date(df[DATES[t]])), F.max(F.to_date(df[DATES[t]]))).first()
            r += [str(mm[0])[:10], str(mm[1])[:10]]
        else:
            r += ["-", "-"]
        k = KEYS[t]
        if k:
            dup = df.groupBy(k).count().filter(F.col("count") > 1).count()
            nullk = df.filter(F.col(k).isNull()).count()
            r += [dup, nullk]
        else:
            dup = df.groupBy("game_id", "eventnum").count().filter(F.col("count") > 1).count()
            nullk = df.filter(F.col("game_id").isNull() | F.col("eventnum").isNull()).count()
            ng = df.select("game_id").distinct().count()
            r += [f"{dup} (sur game_id+eventnum)", f"{nullk} / matchs: {ng}"]
        rows.append(r)
    L.append("## Tables source\n")
    L.append(md_table(["table", "lignes", "date min", "date max", "clés dupliquées", "clés nulles"],
                      rows))

    L.append("\n## Complétude par champ (taux de non-null)\n")
    for t in TABLES:
        df = dfs[t]
        n = df.count()
        fill = df.select([F.round(F.mean(F.when(F.col(c).isNull(), 0).otherwise(1)) * 100, 1)
                          .alias(c) for c in df.columns]).first()
        worst = sorted(((fill[c], c) for c in df.columns))[:8]
        L.append(f"### `{t}`")
        L.append(md_table(["champ", "% rempli"],
                          [[f"`{c}`", v] for v, c in worst if v < 100] or [["-", "100 %"]]))
        L.append("")

    L.append("## game.csv — saisons et types\n")
    g = dfs["game"].withColumn("annee", F.col("season_id").cast("int") % 10000)
    st = g.groupBy("season_type").count().orderBy(F.desc("count")).collect()
    L.append(md_table(["season_type brut", "n"], [(r.season_type, r["count"]) for r in st]))
    L.append("")
    dec = (g.withColumn("decennie", F.floor(F.col("annee") / 10) * 10)
            .groupBy("decennie")
            .agg(F.count("*").alias("matchs"),
                 F.round(F.mean(F.col("fg3a_home").isNotNull().cast("int")) * 100, 1)
                 .alias("pct_fg3a_rempli"),
                 F.round(F.mean(F.col("oreb_home").isNotNull().cast("int")) * 100, 1)
                 .alias("pct_oreb_rempli"))
            .orderBy("decennie").collect())
    L.append(md_table(["décennie", "matchs", "% fg3a renseigné", "% oreb renseigné"],
                      [(r.decennie, r.matchs, r.pct_fg3a_rempli, r.pct_oreb_rempli) for r in dec]))

    L.append("\n## play_by_play — couverture des matchs par saison\n")
    pbp_g = dfs["play_by_play"].select("game_id").distinct()
    cov = (g.select("game_id", "annee")
            .join(pbp_g.withColumn("has_pbp", F.lit(1)), "game_id", "left")
            .groupBy("annee")
            .agg(F.count("*").alias("matchs"),
                 F.sum(F.coalesce("has_pbp", F.lit(0))).alias("avec_pbp"))
            .orderBy("annee").collect())
    L.append(md_table(["saison", "matchs game.csv", "avec play-by-play", "%"],
                      [(r.annee, r.matchs, r.avec_pbp,
                        round(r.avec_pbp / r.matchs * 100, 1)) for r in cov]))

    L.append("\n## Doublons exacts (lignes strictement identiques)\n")
    dup_rows = []
    for t in TABLES:
        df = dfs[t]
        if t == "play_by_play":
            dk = (df.groupBy("game_id", "eventnum").count()
                  .filter(F.col("count") > 1).select("game_id", "eventnum"))
            sub = df.join(dk, ["game_id", "eventnum"])
        else:
            sub = df
        d = sub.groupBy(sub.columns).count().filter(F.col("count") > 1)
        ndup_groups = d.count()
        ndup_rows = d.agg(F.sum("count") - F.count("*")).first()[0] or 0
        dup_rows.append((f"`{t}`", ndup_groups, ndup_rows))
    L.append(md_table(["table", "groupes de doublons", "lignes excédentaires"], dup_rows))

    L.append("\n## Constat de couverture\n")
    L.append("- `game.csv` couvre 1946 → 2023 ; `play_by_play` couvre ~1996-97 → 2023 "
             "(détail à vérifier via la jointure ci-dessus).")
    L.append("- Les champs de boxscore (3 pts, rebonds offensifs…) sont vides pour les "
             "saisons anciennes : valeurs historiquement non renseignées, à conserver nulles.")

    os.makedirs("/work/resultats", exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(L))
    print(f"rapport ecrit : {OUT}")
    spark.stop()


if __name__ == "__main__":
    main()
