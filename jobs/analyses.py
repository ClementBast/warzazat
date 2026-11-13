import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import pymongo
from pyspark.sql import SparkSession, functions as F, Window

RES = "/work/resultats"
spark = SparkSession.builder.appName("nba-analyses").getOrCreate()

SILVER = "hdfs://namenode:9000/nba/silver"
pbp = spark.read.parquet(f"{SILVER}/play_by_play")
g = spark.read.parquet(f"{SILVER}/games").select("game_id", "season_type", "wl_home")
game = spark.read.parquet(f"{SILVER}/games")

w = Window.partitionBy("game_id").orderBy("eventnum").rowsBetween(Window.unboundedPreceding, 0)
rs = (pbp.join(g, "game_id")
    .filter(F.col("season_type") == "Regular Season")
    .withColumn("hd", F.col("homedescription"))
    .withColumn("vd", F.col("visitordescription"))
    .withColumn("passeur", F.col("player2_name"))
    .withColumn("side", F.when(F.col("hd").isNotNull(), "H")
                       .when(F.col("vd").isNotNull(), "A").otherwise("N"))
    .withColumn("descr", F.coalesce("hd", "vd"))
    .withColumn("pts", F.when(F.col("eventmsgtype") == 1,
                              F.when(F.col("descr").contains("3PT"), 3).otherwise(2))
                       .when((F.col("eventmsgtype") == 3) & (~F.col("descr").contains("MISS")), 1)
                       .otherwise(0))
    .withColumn("tir", F.when(F.col("descr").contains("3PT"), "3 points")
                       .when(F.col("descr").rlike("Dunk|Layup|Tip|Alley Oop|Finger Roll"), "proche cercle")
                       .when(F.col("eventmsgtype").isin(1, 2), "mi-distance"))
    .withColumn("score_f", F.last(F.when(F.length("score") > 0, F.col("score")), True).over(w))
    .withColumn("marge_dom", F.split("score_f", " - ").getItem(1).cast("int")
                           - F.split("score_f", " - ").getItem(0).cast("int")))
RESULTATS = {}
n_matchs = rs.select("game_id").distinct().count()
print("matchs RS:", n_matchs)

par_min = (rs.filter(F.col("sec") < 2880).withColumn("minute", F.floor(F.col("sec") / 60))
    .groupBy("minute")
    .agg(F.count(F.when(F.col("eventmsgtype") == 6, 1)).alias("fautes"),
         F.count(F.when(F.col("eventmsgtype") == 5, 1)).alias("pertes"),
         F.sum("pts").alias("pts"))
    .orderBy("minute").toPandas())
par_min[["fautes", "pertes", "pts"]] /= n_matchs
RESULTATS["actions_minute"] = par_min

fig, ax = plt.subplots(1, 2, figsize=(14, 4))
ax[0].plot(par_min.minute, par_min.pts); ax[0].set_title("Points marques par minute de jeu")
ax[0].set_xlabel("minute"); ax[0].axvline(24, ls="--", c="grey")
ax[1].plot(par_min.minute, par_min.fautes, label="fautes")
ax[1].plot(par_min.minute, par_min.pertes, label="pertes de balle")
ax[1].set_title("Fautes et pertes de balle par minute"); ax[1].legend()
for x in (12, 24, 36): ax[1].axvline(x, ls="--", c="grey")
plt.tight_layout(); plt.savefig(f"{RES}/02_actions_minute.png"); plt.close()

par_qt = (rs.filter(F.col("period") <= 4).groupBy("period")
    .agg((F.count(F.when(F.col("eventmsgtype") == 6, 1)) / (n_matchs * 12)).alias("fautes_min"),
         (F.count(F.when(F.col("eventmsgtype") == 5, 1)) / (n_matchs * 12)).alias("pertes_min"),
         (F.count(F.when(F.col("eventmsgtype").isin(1, 2), 1)) / (n_matchs * 12)).alias("tirs_min"),
         (F.sum("pts") / (n_matchs * 12)).alias("pts_min"))
    .orderBy("period").toPandas())
RESULTATS["par_quart_temps"] = par_qt
par_qt.plot(x="period", kind="bar", figsize=(10, 4), title="Rythme par quart-temps (par minute de jeu)")
plt.savefig(f"{RES}/02_quart_temps.png"); plt.close()

saisons = (game.filter(F.col("season_type") == "Regular Season")
    .withColumn("t3", F.col("fg3a_home").cast("double") + F.col("fg3a_away").cast("double"))
    .withColumn("tt", F.col("fga_home").cast("double") + F.col("fga_away").cast("double"))
    .groupBy("annee")
    .agg((F.sum("t3") / F.sum("tt")).alias("part_3pts"),
         (F.avg(F.col("pts_home").cast("double")) + F.avg(F.col("pts_away").cast("double"))).alias("pts_match"))
    .orderBy("annee").toPandas())
RESULTATS["tirs_par_saison"] = saisons

fig, ax = plt.subplots(figsize=(10, 4))
ax.plot(saisons.annee, saisons.part_3pts * 100); ax.set_ylabel("% tirs a 3 pts")
ax2 = ax.twinx(); ax2.plot(saisons.annee, saisons.pts_match, c="orange", alpha=.6)
ax2.set_ylabel("pts / match / equipe", c="orange"); ax.set_title("Le recul du tir a mi-distance")
plt.savefig(f"{RES}/03_evolution_3pts.png"); plt.close()

tirs_qt = (rs.filter(F.col("tir").isNotNull() & (F.col("period") <= 4))
    .groupBy("period", "tir").count().toPandas()
    .pivot(index="period", columns="tir", values="count").fillna(0))
tirs_qt = tirs_qt.div(tirs_qt.sum(axis=1), axis=0)
RESULTATS["tirs_par_quart"] = tirs_qt.reset_index()
tirs_qt.plot(kind="bar", stacked=True, figsize=(9, 5), title="Types de tir par quart-temps")
plt.legend(loc="lower right"); plt.savefig(f"{RES}/03_tirs_quart.png"); plt.close()

assist = (rs.filter(F.col("eventmsgtype") == 1)
    .withColumn("decennie", (F.col("annee") / 10).cast("int") * 10)
    .groupBy("decennie")
    .agg(F.mean(F.col("passeur").isNotNull().cast("int")).alias("part_assistee"))
    .orderBy("decennie").toPandas())
RESULTATS["assist_decennie"] = assist

par_equipe = (rs.filter((F.col("eventmsgtype") == 1) & (F.col("annee") >= 2014))
    .groupBy("player1_team_abbreviation")
    .agg(F.mean(F.col("passeur").isNotNull().cast("int")).alias("part_assistee"))
    .orderBy(F.desc("part_assistee")).toPandas())
RESULTATS["assist_equipe"] = par_equipe

fig, ax = plt.subplots(1, 2, figsize=(14, 4))
ax[0].plot(assist.decennie, assist.part_assistee * 100, marker="o")
ax[0].set_title("% de paniers apres passe decisive"); ax[0].set_ylabel("%")
par_equipe.plot(x="player1_team_abbreviation", y="part_assistee", kind="bar",
                ax=ax[1], title="Assist % par equipe (depuis 2014-15)", legend=False)
plt.tight_layout(); plt.savefig(f"{RES}/03_assists.png"); plt.close()

rs4 = rs.filter(F.col("period") == 4)
n4 = rs4.select("game_id").distinct().count()
fin = rs4.filter(F.col("rest") <= 120).withColumn(
    "marge_side", F.when(F.col("side") == "H", F.col("marge_dom")).otherwise(-F.col("marge_dom")))

fautes_fin = fin.filter(F.col("eventmsgtype") == 6)
n_fin = fautes_fin.count()
n_reste = rs.filter(~((F.col("period") == 4) & (F.col("rest") <= 120))) \
            .filter(F.col("eventmsgtype") == 6).count()
taux = pd.DataFrame({
    "moment": ["reste du match (moy. par 2 min)", "2 dernieres min du 4e QT"],
    "fautes_par_match": [n_reste / n_matchs / 23, n_fin / n4]})

par_sit = (fautes_fin.groupBy(F.when(F.col("marge_side") < 0, "equipe menee")
                             .when(F.col("marge_side") > 0, "equipe menante")
                             .otherwise("egalite").alias("qui_faute"))
    .count().toPandas())
RESULTATS["fautes_fin"] = par_sit

fig, ax = plt.subplots(1, 2, figsize=(12, 4))
ax[0].bar(taux.moment, taux.fautes_par_match); ax[0].set_title("Fautes par fenetre de 2 min")
ax[1].bar(par_sit.qui_faute, par_sit["count"])
ax[1].set_title("Qui commet les fautes dans les 2 dern. min ?")
plt.tight_layout(); plt.savefig(f"{RES}/04_fautes.png"); plt.close()

sc = rs.filter((F.col("pts") > 0) & (F.col("period") == 4) & (F.col("rest") <= 300))
w2 = Window.partitionBy("game_id", "side").orderBy("sec")
sc = (sc.withColumn("delta", F.col("sec") - F.lag("sec").over(w2))
        .withColumn("marge_side", F.when(F.col("side") == "H", F.col("marge_dom")).otherwise(-F.col("marge_dom"))))
rythme = (sc.filter(F.col("delta").isNotNull() & (F.col("delta") < 200))
    .groupBy(F.when(F.col("marge_side") > 0, "equipe menante").otherwise("equipe menee").alias("situation"))
    .agg(F.round(F.avg("delta"), 1).alias("sec_entre_pts"), F.count("*").alias("n"))
    .toPandas())
RESULTATS["ralentissement"] = rythme
rythme.plot(x="situation", y="sec_entre_pts", kind="bar", legend=False,
            title="Intervalle moyen entre 2 paniers (5 dern. min du 4e QT)")
plt.ylabel("secondes"); plt.savefig(f"{RES}/04_chrono.png"); plt.close()

to = rs.filter((F.col("eventmsgtype") == 9) & (F.col("period") == 4)) \
       .select("game_id", "eventnum", "sec", "rest", "side")
sc4 = rs.filter((F.col("pts") > 0) & (F.col("period") == 4)) \
        .select("game_id", "sec", "side", "pts")

dist_to = (to.withColumn("min_restante", F.floor(F.col("rest") / 60))
    .groupBy("min_restante").count().orderBy("min_restante").toPandas())
RESULTATS["timeouts_distrib"] = dist_to

avant = (to.alias("t").join(sc4.alias("s"),
            (F.col("t.game_id") == F.col("s.game_id"))
            & F.col("s.sec").between(F.col("t.sec") - 120, F.col("t.sec") - 1), "left")
    .groupBy(F.col("t.game_id"), F.col("t.eventnum"), F.col("t.sec"), F.col("t.side"))
    .agg(F.sum(F.when(F.col("s.side") != F.col("t.side"), F.col("s.pts"))).alias("adv"),
         F.sum(F.when(F.col("s.side") == F.col("t.side"), F.col("s.pts"))).alias("propres"))
    .fillna(0)
    .withColumn("run_adv", F.col("adv") - F.col("propres") >= 8))

apres = (avant.alias("t").join(sc4.alias("s"),
            (F.col("t.game_id") == F.col("s.game_id"))
            & F.col("s.sec").between(F.col("t.sec"), F.col("t.sec") + 60)
            & (F.col("s.side") != F.col("t.side")), "left")
    .groupBy(F.col("t.game_id"), F.col("t.eventnum"), F.col("t.run_adv"))
    .agg(F.count(F.col("s.pts")).alias("pts_adv_60s"))
    .withColumn("casse", F.col("pts_adv_60s") == 0))

eff = (apres.groupBy("run_adv")
    .agg(F.count("*").alias("n"), F.mean(F.col("casse").cast("int")).alias("taux_casse"))
    .toPandas())
RESULTATS["timeouts_efficacite"] = eff

fig, ax = plt.subplots(1, 2, figsize=(13, 4))
ax[0].bar(dist_to.min_restante, dist_to["count"]); ax[0].invert_xaxis()
ax[0].set_title("Temps morts du 4e QT par minute restante")
ax[1].bar(eff.run_adv.map({True: "pendant un run adv.", False: "hors run"}), eff.taux_casse * 100)
ax[1].set_title("% de temps morts qui cassent la serie (0 pt adverse sur 60s)")
plt.tight_layout(); plt.savefig(f"{RES}/04_timeouts.png"); plt.close()

sc_all = rs.filter(F.col("pts") > 0).select("game_id", "eventnum", "sec", "period", "side", "pts")
w3 = Window.partitionBy("game_id").orderBy("eventnum")
sc_all = (sc_all.withColumn("new", F.when(F.col("side") != F.lag("side").over(w3), 1).otherwise(0))
                .withColumn("gid", F.sum("new").over(w3)))
runs = (sc_all.groupBy("game_id", "gid")
    .agg(F.first("side").alias("side"), F.sum("pts").alias("taille"),
         F.min("period").alias("period"), F.min("sec").alias("debut"))
    .filter(F.col("taille") >= 8))

nb_runs = runs.count()
par_match = runs.groupBy("game_id").count().groupBy().avg("count").first()[0]
taille_moy = runs.groupBy().avg("taille").first()[0]
par_per = runs.groupBy("period").count().orderBy("period").toPandas()
top = runs.orderBy(F.desc("taille")).limit(10).toPandas()
RESULTATS["runs_resume"] = pd.DataFrame({"nb_runs": [nb_runs], "par_match": [par_match],
                                         "taille_moy": [taille_moy]})
RESULTATS["runs_par_periode"] = par_per
RESULTATS["top_runs"] = top
print(f"runs >= 8 pts : {nb_runs}  ({par_match:.2f} par match, {taille_moy:.1f} pts)")

fig, ax = plt.subplots(1, 2, figsize=(12, 4))
runs.select("taille").toPandas().plot.hist(bins=30, ax=ax[0], title="Ampleur des runs (>=8 pts)")
ax[1].bar(par_per.period, par_per["count"]); ax[1].set_title("Runs par quart-temps")
plt.tight_layout(); plt.savefig(f"{RES}/05_runs.png"); plt.close()

marges = (rs.filter(F.col("side").isin("H", "A") & F.col("score_f").isNotNull())
    .withColumn("marge_side", F.when(F.col("side") == "H", F.col("marge_dom")).otherwise(-F.col("marge_dom")))
    .withColumn("gagne", F.when((F.col("side") == "H") & (F.col("wl_home") == "W"), True)
                         .when((F.col("side") == "A") & (F.col("wl_home") == "L"), True)
                         .otherwise(False)))
deficit = (marges.groupBy("game_id", "side")
    .agg((-F.min("marge_side")).alias("deficit_max"), F.first("gagne").alias("gagne"))
    .withColumn("tranche", F.when(F.col("deficit_max") <= 0, "0")
                           .when(F.col("deficit_max") <= 4, "01-04")
                           .when(F.col("deficit_max") <= 9, "05-09")
                           .when(F.col("deficit_max") <= 14, "10-14")
                           .when(F.col("deficit_max") <= 19, "15-19")
                           .otherwise("20+")))
taux = (deficit.groupBy("tranche").agg(F.mean(F.col("gagne").cast("int")).alias("win_pct"),
                                       F.count("*").alias("n"))
    .toPandas().sort_values("tranche").reset_index(drop=True))
RESULTATS["remontees"] = taux

plt.figure(figsize=(8, 4))
plt.bar(taux.tranche, taux.win_pct * 100)
plt.title("Victoire selon le plus gros deficit subi dans le match"); plt.ylabel("% victoires")
for i, r in taux.iterrows(): plt.text(i, r.win_pct * 100 + 1, f"n={r.n}", ha="center", fontsize=8)
plt.savefig(f"{RES}/05_remontees.png"); plt.close()

rs.write.mode("overwrite").partitionBy("annee").parquet("hdfs://namenode:9000/nba/gold/pbp_enrichi")

mongo = pymongo.MongoClient("mongodb://mongo:27017")["nba"]
for nom, df in RESULTATS.items():
    col = mongo[nom]
    col.drop()
    col.insert_many(json.loads(df.to_json(orient="records")))
    clean = df.rename(columns={c: c.replace(" ", "_") for c in df.columns})
    spark.createDataFrame(clean).write.mode("overwrite").parquet(f"hdfs://namenode:9000/nba/gold/{nom}")
    print(f"{nom}: {col.count_documents({})} docs")

with open("/work/resultats/controles_silver.md", "a", encoding="utf-8") as f:
    f.write("\n\n## État des données — Silver → Gold\n\n")
    f.write("| couche | emplacement | contenu |\n|---|---|---|\n")
    f.write("| silver | hdfs /nba/silver/play_by_play + games (parquet) | "
            "événements typés + horloge parsée, uniques par (game_id, eventnum) |\n")
    f.write("| gold | hdfs /nba/gold/ (parquet) + mongo nba | " + ", ".join(
            f"`{n}` ({mongo[n].count_documents({})} docs)" for n in RESULTATS) + " |\n")

spark.stop()
print("analyses terminees")
