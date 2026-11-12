"""Ingestion : telecharge le dataset Kaggle -> data/archive/ (+ option --hdfs).

Usage (dans le conteneur jupyter) :
    python3 /work/jobs/ingest_kaggle.py            # download + extract
    python3 /work/jobs/ingest_kaggle.py --hdfs     # + pousse les CSV vers hdfs:///nba/raw

kagglehub telecharge les datasets publics sans credentials
(kaggle.json utilise automatiquement si present dans ./.kaggle).
"""
import argparse
import os
import shutil

import requests

DATASET = "wyattowalsh/basketball"
NAMENODE = "http://namenode:9870"
HDFS_RAW = "/nba/raw"


def download(dest):
    import kagglehub
    os.makedirs(dest, exist_ok=True)
    print(f"telechargement {DATASET} ...")
    cache = kagglehub.dataset_download(DATASET)
    print(f"cache kagglehub : {cache}")
    for item in os.listdir(cache):
        s, d = os.path.join(cache, item), os.path.join(dest, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, dirs_exist_ok=True)
        else:
            shutil.copy2(s, d)
    print(f"extrait dans {dest} :", sorted(os.listdir(dest)))


def put_webhdfs(local_path, hdfs_path):
    """Upload d'un fichier vers HDFS via WebHDFS (CREATE + redirect datanode)."""
    mkdir = requests.put(
        f"{NAMENODE}/webhdfs/v1{os.path.dirname(hdfs_path)}"
        f"?op=MKDIRS&user.name=root", timeout=30)
    mkdir.raise_for_status()
    r = requests.put(
        f"{NAMENODE}/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true&user.name=root",
        allow_redirects=False, timeout=30)
    r.raise_for_status()
    with open(local_path, "rb") as f:
        up = requests.put(r.headers["Location"], data=f, timeout=600)
    up.raise_for_status()
    print(f"  -> {hdfs_path} ({os.path.getsize(local_path) / 1e6:.0f} Mo)")


def push_csvs_to_hdfs(csv_dir):
    csvs = sorted(f for f in os.listdir(csv_dir) if f.endswith(".csv"))
    print(f"push {len(csvs)} csv -> hdfs://namenode:9000{HDFS_RAW}/")
    for f in csvs:
        put_webhdfs(os.path.join(csv_dir, f), f"{HDFS_RAW}/{f}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dest", default="/work/data/archive",
                    help="dossier d'extraction du dataset")
    ap.add_argument("--hdfs", action="store_true",
                    help="pousse aussi les CSV vers la zone brute HDFS /nba/raw")
    args = ap.parse_args()

    download(args.dest)
    if args.hdfs:
        push_csvs_to_hdfs(os.path.join(args.dest, "csv"))
    print("ingestion terminee")
