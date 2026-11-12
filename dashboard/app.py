import streamlit as st
import pandas as pd
import pymongo
import plotly.express as px

st.set_page_config(page_title="NBA Big Data", layout="wide")


@st.cache_data(ttl=300)
def col(nom):
    db = pymongo.MongoClient("mongodb://mongo:27017")["nba"]
    return pd.DataFrame(list(db[nom].find({}, {"_id": 0})))


def graphe(df, fig_fn):
    if df.empty:
        st.warning("collection vide — lancer analyse_nba.ipynb d'abord")
    else:
        st.plotly_chart(fig_fn(df), use_container_width=True)


st.title("NBA — déroulé d'un match, choix de tir, stratégies de fin de match")
st.caption("13,6 M d'événements play-by-play | Spark → HDFS → MongoDB")

tab1, tab2, tab3, tab4 = st.tabs(
    ["Déroulé d'un match", "Choix de tir", "Fin de match", "Runs & remontées"])

with tab1:
    met = st.selectbox("Métrique par minute de jeu", ["pts", "fautes", "pertes"])
    graphe(col("actions_minute"),
           lambda d: px.line(d, x="minute", y=met,
                             title=f"{met} par minute de jeu (moyenne / match)"))
    graphe(col("par_quart_temps"),
           lambda d: px.bar(d, x="period", barmode="group",
                            y=["pts_min", "tirs_min", "fautes_min", "pertes_min"],
                            title="Rythme par quart-temps (par minute)"))

with tab2:
    s = col("tirs_par_saison")
    if not s.empty:
        lo, hi = int(s.annee.min()), int(s.annee.max())
        a, b = st.slider("Période", lo, hi, (1980, hi))
        s = s[s.annee.between(a, b)]
        graphe(s, lambda d: px.line(d, x="annee", y="part_3pts",
                                    title="Part des tirs à 3 pts (évolution)"))
        graphe(s, lambda d: px.line(d, x="annee", y="pts_match",
                                    title="Points par match et par équipe"))
    graphe(col("tirs_par_quart"),
           lambda d: px.bar(d, x="period", barmode="stack",
                            y=[c for c in d.columns if c != "period"],
                            title="Types de tir par quart-temps"))
    graphe(col("assist_equipe"),
           lambda d: px.bar(d, x="player1_team_abbreviation", y="part_assistee",
                            title="% paniers après passe décisive (depuis 2014-15)"))

with tab3:
    graphe(col("fautes_fin"),
           lambda d: px.bar(d, x="qui_faute", y="count",
                            title="Qui commet les fautes dans les 2 dernières minutes ?"))
    graphe(col("ralentissement"),
           lambda d: px.bar(d, x="situation", y="sec_entre_pts",
                            title="Intervalle entre 2 paniers (5 dernières min du 4e QT)"))
    graphe(col("timeouts_distrib"),
           lambda d: px.bar(d, x="min_restante", y="count",
                            title="Temps morts du 4e QT par minute restante"))
    e = col("timeouts_efficacite")
    if not e.empty:
        e["contexte"] = e.run_adv.map({True: "pendant un run adverse", False: "hors run"})
        graphe(e, lambda d: px.bar(d, x="contexte", y="taux_casse",
                                   title="Temps morts qui cassent la série (0 pt adverse / 60 s)"))

with tab4:
    rr = col("runs_resume")
    if not rr.empty:
        r0 = rr.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("runs ≥ 8 pts", int(r0.nb_runs))
        c2.metric("par match", f"{r0.par_match:.2f}")
        c3.metric("taille moyenne", f"{r0.taille_moy:.1f} pts")
    graphe(col("runs_par_periode"),
           lambda d: px.bar(d, x="period", y="count", title="Runs par quart-temps"))
    st.subheader("Top 10 des runs")
    st.dataframe(col("top_runs"), hide_index=True)
    t = col("remontees")
    if not t.empty:
        t["win_pct"] *= 100
        graphe(t, lambda d: px.bar(d, x="tranche", y="win_pct",
                                   labels={"win_pct": "% victoires"},
                                   title="Victoire selon le plus gros déficit subi"))
