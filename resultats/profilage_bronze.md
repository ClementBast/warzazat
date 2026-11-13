# Profilage Bronze — data/archive/csv

## Tables source

| table | lignes | date min | date max | clés dupliquées | clés nulles |
|---|---|---|---|---|---|
| `game.csv` | 65 698 | 1946-11-01 | 2023-06-12 | 56 | 0 |
| `line_score.csv` | 58 053 | 1946-11-01 | 2023-06-12 | 40 | 0 |
| `game_info.csv` | 58 053 | - | - | 40 | 0 |
| `game_summary.csv` | 58 110 | 1946-11-01 | 2023-06-12 | 85 | 0 |
| `other_stats.csv` | 28 271 | - | - | 10 | 0 |
| `play_by_play.csv` | 13 592 899 | - | - | 7360 (sur game_id+eventnum) | 0 / matchs: 29818 |

## Complétude par champ (taux de non-null)

### `game`
| champ | % rempli |
|---|---|
| `fg3_pct_home` | 71.0 |
| `dreb_away` | 71.1 |
| `dreb_home` | 71.1 |
| `fg3_pct_away` | 71.1 |
| `oreb_away` | 71.2 |
| `oreb_home` | 71.2 |
| `stl_away` | 71.3 |
| `stl_home` | 71.3 |

### `line_score`
| champ | % rempli |
|---|---|
| `pts_ot10_away` | 21.5 |
| `pts_ot10_home` | 21.5 |
| `pts_ot5_away` | 21.5 |
| `pts_ot5_home` | 21.5 |
| `pts_ot6_away` | 21.5 |
| `pts_ot6_home` | 21.5 |
| `pts_ot7_away` | 21.5 |
| `pts_ot7_home` | 21.5 |

### `game_info`
| champ | % rempli |
|---|---|
| `game_time` | 51.6 |
| `attendance` | 90.7 |

### `game_summary`
| champ | % rempli |
|---|---|
| `live_pc_time` | 3.5 |
| `natl_tv_broadcaster_abbreviation` | 10.7 |
| `game_status_text` | 55.3 |
| `game_sequence` | 56.1 |

### `other_stats`
| champ | % rempli |
|---|---|
| `pts_off_to_away` | 92.5 |
| `pts_off_to_home` | 92.5 |
| `team_rebounds_away` | 92.9 |
| `team_rebounds_home` | 92.9 |
| `total_turnovers_away` | 98.9 |
| `total_turnovers_home` | 98.9 |

### `play_by_play`
| champ | % rempli |
|---|---|
| `person1type` | 0.0 |
| `person2type` | 0.0 |
| `person3type` | 0.0 |
| `neutraldescription` | 2.4 |
| `player3_name` | 2.5 |
| `player3_team_abbreviation` | 2.5 |
| `player3_team_city` | 2.5 |
| `player3_team_id` | 2.5 |

## game.csv — saisons et types

| season_type brut | n |
|---|---|
| Regular Season | 60192 |
| Playoffs | 3842 |
| Pre Season | 1536 |
| All Star | 65 |
| All-Star | 63 |

| décennie | matchs | % fg3a renseigné | % oreb renseigné |
|---|---|---|---|
| 1940 | 1538 | 0.1 | 0.0 |
| 1950 | 3303 | 0.0 | 0.0 |
| 1960 | 3320 | 0.0 | 0.0 |
| 1970 | 6012 | 0.1 | 0.9 |
| 1980 | 10337 | 56.3 | 53.4 |
| 1990 | 11531 | 100.0 | 100.0 |
| 2000 | 13377 | 100.0 | 100.0 |
| 2010 | 12294 | 100.0 | 100.0 |
| 2020 | 3986 | 100.0 | 100.0 |

## play_by_play — couverture des matchs par saison

| saison | matchs game.csv | avec play-by-play | % |
|---|---|---|---|
| 1946 | 350 | 0 | 0.0 |
| 1947 | 215 | 0 | 0.0 |
| 1948 | 380 | 0 | 0.0 |
| 1949 | 593 | 0 | 0.0 |
| 1950 | 382 | 0 | 0.0 |
| 1951 | 357 | 0 | 0.0 |
| 1952 | 376 | 0 | 0.0 |
| 1953 | 348 | 0 | 0.0 |
| 1954 | 311 | 0 | 0.0 |
| 1955 | 313 | 0 | 0.0 |
| 1956 | 309 | 0 | 0.0 |
| 1957 | 290 | 0 | 0.0 |
| 1958 | 290 | 0 | 0.0 |
| 1959 | 327 | 0 | 0.0 |
| 1960 | 1 | 0 | 0.0 |
| 1961 | 31 | 0 | 0.0 |
| 1962 | 391 | 0 | 0.0 |
| 1963 | 388 | 0 | 0.0 |
| 1964 | 362 | 0 | 0.0 |
| 1965 | 388 | 0 | 0.0 |
| 1966 | 33 | 0 | 0.0 |
| 1967 | 534 | 0 | 0.0 |
| 1968 | 576 | 0 | 0.0 |
| 1969 | 616 | 0 | 0.0 |
| 1970 | 42 | 0 | 0.0 |
| 1971 | 735 | 0 | 0.0 |
| 1972 | 740 | 0 | 0.0 |
| 1973 | 739 | 0 | 0.0 |
| 1974 | 787 | 0 | 0.0 |
| 1975 | 51 | 0 | 0.0 |
| 1976 | 55 | 0 | 0.0 |
| 1977 | 955 | 0 | 0.0 |
| 1978 | 956 | 0 | 0.0 |
| 1979 | 952 | 0 | 0.0 |
| 1980 | 997 | 0 | 0.0 |
| 1981 | 992 | 0 | 0.0 |
| 1982 | 988 | 0 | 0.0 |
| 1983 | 1024 | 0 | 0.0 |
| 1984 | 1012 | 0 | 0.0 |
| 1985 | 1013 | 0 | 0.0 |
| 1986 | 1016 | 0 | 0.0 |
| 1987 | 1025 | 0 | 0.0 |
| 1988 | 1089 | 0 | 0.0 |
| 1989 | 1181 | 0 | 0.0 |
| 1990 | 1177 | 0 | 0.0 |
| 1991 | 1182 | 0 | 0.0 |
| 1992 | 1185 | 0 | 0.0 |
| 1993 | 1108 | 0 | 0.0 |
| 1994 | 1182 | 0 | 0.0 |
| 1995 | 1191 | 0 | 0.0 |
| 1996 | 1263 | 1137 | 90.0 |
| 1997 | 1261 | 1130 | 89.6 |
| 1998 | 791 | 688 | 87.0 |
| 1999 | 1191 | 1046 | 87.8 |
| 2000 | 1261 | 1113 | 88.3 |
| 2001 | 1191 | 1037 | 87.1 |
| 2002 | 1279 | 1127 | 88.1 |
| 2003 | 1273 | 1133 | 89.0 |
| 2004 | 1316 | 1154 | 87.7 |
| 2005 | 1343 | 1070 | 79.7 |
| 2006 | 1431 | 1163 | 81.3 |
| 2007 | 1424 | 1168 | 82.0 |
| 2008 | 1427 | 1161 | 81.4 |
| 2009 | 1432 | 1182 | 82.5 |
| 2010 | 1432 | 1190 | 83.1 |
| 2011 | 1105 | 942 | 85.2 |
| 2012 | 203 | 79 | 38.9 |
| 2013 | 1435 | 1303 | 90.8 |
| 2014 | 1431 | 1284 | 89.7 |
| 2015 | 1427 | 1296 | 90.8 |
| 2016 | 1412 | 1294 | 91.6 |
| 2017 | 1392 | 1273 | 91.5 |
| 2018 | 1314 | 1202 | 91.5 |
| 2019 | 1143 | 1040 | 91.0 |
| 2020 | 1216 | 1114 | 91.6 |
| 2021 | 1384 | 1257 | 90.8 |
| 2022 | 1386 | 1250 | 90.2 |

## Doublons exacts (lignes strictement identiques)

| table | groupes de doublons | lignes excédentaires |
|---|---|---|
| `game` | 0 | 0 |
| `line_score` | 40 | 40 |
| `game_info` | 40 | 40 |
| `game_summary` | 40 | 40 |
| `other_stats` | 10 | 10 |
| `play_by_play` | 7360 | 7360 |

## Constat de couverture

- `game.csv` couvre 1946 → 2023 ; `play_by_play` couvre ~1996-97 → 2023 (détail à vérifier via la jointure ci-dessus).
- Les champs de boxscore (3 pts, rebonds offensifs…) sont vides pour les saisons anciennes : valeurs historiquement non renseignées, à conserver nulles.