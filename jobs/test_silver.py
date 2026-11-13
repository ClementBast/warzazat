import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def spark():
    return (SparkSession.builder.master("local[1]")
            .appName("test-silver")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def sschema(cols, typ=None):
    typ = typ or StringType()
    return StructType([StructField(c, typ) for c in cols])


class TestSilver(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark()
        import silver as S
        cls.S = S

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_normalize_season_type(self):
        S = self.S
        df = self.spark.createDataFrame(
            [("Regular Season",), ("All Star",), ("All-Star",), ("All star",),
             ("Playoffs",), ("Pre Season",), ("BIZARRE",), (None,)],
            sschema(["season_type"]))
        got = {r[0] for r in S.normalize_season_type(df)
               .select("season_type").distinct().collect()}
        self.assertEqual(got, {"Regular Season", "All-Star", "Playoffs",
                               "Pre Season", "BIZARRE", None})

    def test_dedup_exact_apres_normalisation(self):
        S = self.S
        rows = [("001", "All Star", 100), ("001", "All-Star", 100), ("002", "Regular Season", 90)]
        df = self.spark.createDataFrame(rows, sschema(["game_id", "season_type", "pts"]))
        df = S.dedup_exact(S.normalize_season_type(df))
        self.assertEqual(df.count(), 2)
        clean, conf = S.split_key_conflicts(df, ["game_id"])
        self.assertEqual(conf.count(), 0)
        self.assertEqual(clean.count(), 2)

    def test_conflit_mis_en_quarantaine(self):
        S = self.S
        rows = [("001", "W"), ("001", "L"), ("002", "W")]
        df = self.spark.createDataFrame(rows, sschema(["game_id", "wl"]))
        clean, conf = S.split_key_conflicts(df, ["game_id"])
        self.assertEqual(clean.count(), 1)
        self.assertEqual(conf.count(), 2)

    def test_parse_clock(self):
        S = self.S
        df = self.spark.createDataFrame(
            [("12:00",), ("11:59",), ("0:00",), ("abc",), (None,)],
            sschema(["pctimestring"]))
        res = S.parse_clock(df).orderBy(F.col("pctimestring").asc_nulls_last()).collect()
        vals = {r.pctimestring: (r.rest, r.clock_ok) for r in res}
        self.assertEqual(vals["12:00"], (720, True))
        self.assertEqual(vals["11:59"], (719, True))
        self.assertEqual(vals["0:00"], (0, True))
        self.assertEqual(vals["abc"], (None, False))
        self.assertEqual(vals[None], (None, False))

    def test_parse_score(self):
        S = self.S
        df = self.spark.createDataFrame(
            [("68 - 66",), ("4 - 2",), ("TIE",), (None,)],
            sschema(["score"]))
        res = {r.score: (r.score_away, r.score_home, r.score_ok)
               for r in S.parse_score(df).collect()}
        self.assertEqual(res["68 - 66"], (68, 66, True))
        self.assertEqual(res["4 - 2"], (4, 2, True))
        self.assertEqual(res["TIE"], (None, None, False))
        self.assertEqual(res[None], (None, None, False))

    def test_ids_restent_chaines(self):
        from schemas import SCHEMAS
        skip = {"game_status_id"}
        for name, sch in SCHEMAS.items():
            for f in sch.fields:
                if f.name in skip:
                    continue
                if f.name.endswith("_id") or f.name in ("game_id", "season_id"):
                    self.assertIsInstance(f.dataType, type(StringType()),
                                          f"{name}.{f.name} n'est pas StringType")


if __name__ == "__main__":
    unittest.main(verbosity=2)
