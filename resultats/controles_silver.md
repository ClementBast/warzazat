# Contrôles Silver

## État des données — Bronze → Silver

| table | bronze | lignes bronze | lignes silver | sortie | écarts |
|---|---|---|---|---|---|
| game + aux | CSV data/archive/csv | 65698 | 65642 | parquet data/silver/games | 56 doublons exacts, 0 conflits -> quarantaine |
| play_by_play | CSV data/archive/csv | 13592899 | 13585539 | parquet data/silver/play_by_play (partition annee) | 7360 doublons exacts, 0 conflits, 0 orphelins, 0 cles nulles, 0 periodes invalides |

## Comptes

| contrôle | valeur |
|---|---|
| game_lignes | 65 698 |
| game_doublons_exact | 56 |
| game_conflits | 0 |
| games_silver | 65 642 |
| games_cle_nulle | 0 |
| line_score_doublons_exact | 40 |
| line_score_conflits | 0 |
| game_info_doublons_exact | 40 |
| game_info_conflits | 0 |
| game_summary_doublons_exact | 40 |
| game_summary_conflits | 94 |
| other_stats_doublons_exact | 10 |
| other_stats_conflits | 0 |
| pts_game_vs_linescore_ecarts | 27 193 |
| pbp_lignes | 13 592 899 |
| pbp_cles_nulles | 0 |
| pbp_periodes_invalides | 0 |
| pbp_horloges_invalides | 0 |
| pbp_conflits | 0 |
| pbp_doublons_exact | 7 360 |
| pbp_orphelins | 0 |
| pbp_silver | 13 585 539 |
| score_final_matchs_verifies | 29 818 |
| score_final_ecarts | 216 |

## Couverture play-by-play par saison (silver)

| saison | matchs | événements |
|---|---|---|
| 1996 | 1137 | 507595 |
| 1997 | 1130 | 512461 |
| 1998 | 688 | 311570 |
| 1999 | 1046 | 482487 |
| 2000 | 1113 | 503025 |
| 2001 | 1037 | 462101 |
| 2002 | 1127 | 506379 |
| 2003 | 1132 | 505935 |
| 2004 | 1153 | 524310 |
| 2005 | 1069 | 481257 |
| 2006 | 1162 | 524896 |
| 2007 | 1167 | 522510 |
| 2008 | 1161 | 516006 |
| 2009 | 1181 | 528540 |
| 2010 | 1189 | 532937 |
| 2011 | 942 | 418572 |
| 2012 | 78 | 35466 |
| 2013 | 1303 | 594888 |
| 2014 | 1283 | 588914 |
| 2015 | 1295 | 599981 |
| 2016 | 1293 | 597211 |
| 2017 | 1272 | 582237 |
| 2018 | 1201 | 568810 |
| 2019 | 1040 | 494692 |
| 2020 | 1113 | 515683 |
| 2021 | 1257 | 583093 |
| 2022 | 1249 | 583983 |