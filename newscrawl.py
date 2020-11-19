#!/usr/bin/env python

import wget
import os
import itertools
from zipfile import ZipFile
from newspaper import Article
import re
from tqdm import tqdm
import json
import csv
import logging as log
log.basicConfig(
    level=log.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    filename="newscrawl.log")

with open("config.json") as f:
    config = json.load(f)

GDELT_URL = "http://data.gdeltproject.org/gdeltv2/{0}.translation.gkg.csv.zip"

with open('gdelt_headers.txt') as f:
    headers = [line.strip() for line in f.readlines()]

def retrieve(item):
    source = re.sub("(^http.?://)|/.*$", "", item["DocumentIdentifier"])
    article = Article(item["DocumentIdentifier"])
    try:
        article.download()
        article.parse()
        return {
            "timestamp": item["DATE"], 
            "url": item["DocumentIdentifier"], 
            "source": source, 
            "title": article.title.replace("\n", " ").replace("\t", " "),
            "text": article.text
        }
    except:
        raise Exception("article file not found: {0}".format(item["DocumentIdentifier"]))

def filter(path):
    results = []
    with ZipFile(path) as fz:
        with fz.open(fz.filelist[0].filename) as f:
            for line in f:
                try:
                    fields = line.decode("latin1").strip().split('\t')    
                    _, langs, src = fields[25].split(":")
                    source = re.sub("(^http.?://)|/.*$", "", fields[4])
                    lang_orig, lang_trans = langs.split(";")
                    if len(config['sources'])>0 and not source in config['sources']:
                        continue
                    if lang_orig in config["languages"]:
                        results.append({headers[i]:v for i, v in enumerate(fields)})
                except:
                    log.error("cannot parse line in file {0}: {1}".format(path, line))
    return results

# main loop
for year, month, day, hour, minute in itertools.product(
    config['years'], config['months'], config['days'], config['hours'], config['minutes']):
    timestamp = "{0:04}{1:02}{2:02}{3:02}{4:02}00".format(
        year,
        month,
        day,
        hour,
        minute)

    # don't do anything if the file is already processed
    out_file = os.path.join(config["out_dir"], "{0}.csv".format(timestamp))
    if os.path.isfile(out_file):
        continue

    url = GDELT_URL.format(timestamp)
    try:
        tmp_filename = wget.download(url, out=config["tmp_dir"])
    except:
        log.warning("data file not found: {0}".format(url))
        continue
    items = filter(tmp_filename)

    with open(out_file, "w") as fo:
        writer = csv.DictWriter(fo, fieldnames=[
                                    "timestamp", 
                                    "url",
                                    "source",
                                    "title",
                                    "text"])
        
        first = True
        for item in tqdm(items):
            try:
                if first:
                    writer.writeheader()
                    first = False
                writer.writerow(retrieve(item))
            except Exception as e:
                log.error(e)
    if first:
        os.remove(out_file)    
    os.remove(tmp_filename)
