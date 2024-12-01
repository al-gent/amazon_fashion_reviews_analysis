import json
import pandas as pd
from pyspark import SparkContext
from pyspark.rdd import RDD

sc = SparkContext(appName="SubsetJSON")

# local_meta = '/Users/adamgent/dc/project/subset_meta.jsonl'
# local_data = "/Users/adamgent/dc/project/subsetAF.jsonl"

meta_file = 's3://msds-694-cohort-13-4/data/meta_Amazon_Fashion.jsonl'
data_file = 's3://msds-694-cohort-13-4/data/Amazon_Fashion.jsonl'
data = []

data_rdd = sc.textFile(data_file).map(json.loads)
meta_rdd = sc.textFile(meta_file).map(json.loads)


meta_rdd = meta_rdd.map(lambda review: list(review.values()))
meta_pair_rdd = meta_rdd.map(lambda row: (row[-2], row[:-2] + [row[-1]]))

values_rdd = data_rdd.map(lambda review: list(review.values()))

pair_rdd = values_rdd.map(lambda row: (row[5], row[:5] + row[6:]))

avg_stars = {}
for i in range(5):
    avg_stars[i+1] = values_rdd.filter(lambda row: row[0] == float(i+1)).map(lambda row: len(row[2])).mean()

print(avg_stars)