import json
import pandas as pd
from pyspark import SparkContext
from pyspark.rdd import RDD

sc = SparkContext(appName="SubsetJSON")

meta_file = 's3://msds-694-cohort-13-4/data/meta_Amazon_Fashion.jsonl'
data_file = 's3://msds-694-cohort-13-4/data/Amazon_Fashion.jsonl'
data = []

with open(data_file, 'r') as fp:
    for i, line in enumerate(fp):
        data.append(json.loads(line))

metadata = []
with open(meta_file, 'r') as fp:
    for i, line in enumerate(fp):
        metadata.append(json.loads(line))

subset_rdd = sc.parallelize(data)
meta_subset_rdd = sc.parallelize(metadata)

meta_rdd = meta_subset_rdd.map(lambda review: list(review.values()))
meta_pair_rdd = meta_rdd.map(lambda row: (row[-2], row[:-2] + [row[-1]]))

values_rdd = subset_rdd.map(lambda review: list(review.values()))

pair_rdd = values_rdd.map(lambda row: (row[5], row[:5] + row[6:]))

avg_stars = {}
for i in range(5):
    avg_stars[i+1] = values_rdd.filter(lambda row: row[0] == float(i+1)).map(lambda row: len(row[2])).mean()

print(avg_stars)