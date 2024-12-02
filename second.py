from pyspark.sql import SparkSession
import json

import pandas as pd

meta_file = 's3://msds-694-cohort-13-4/data/meta_Amazon_Fashion.jsonl'
data_file = 's3://msds-694-cohort-13-4/data/Amazon_Fashion.jsonl'

local_meta = '/Users/adamgent/dc/project/subset_meta.jsonl'
local_data = "/Users/adamgent/dc/project/subsetAF.jsonl"

sc = SparkSession.builder.appName("JSONReader").getOrCreate()
sc.sparkContext.setLogLevel("OFF")

rdd = sc.sparkContext.textFile(data_file)

reviews_rdd = rdd.map(lambda line: json.loads(line.strip()))

rdd_2 = sc.sparkContext.textFile(meta_file)

meta_rdd = rdd_2.map(lambda line: json.loads(line.strip()))

reviews_df = reviews_rdd.toDF()

from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, MapType, IntegerType

# Define the schema based on the metadata fields
metadata_schema = StructType([
    StructField("main_category", StringType(), True),
    StructField("title", StringType(), True),
    StructField("average_rating", FloatType(), True),
    StructField("rating_number", IntegerType(), True),
    StructField("features", ArrayType(StringType()), True),
    StructField("description", ArrayType(StringType()), True),
    StructField("price", FloatType(), True),
    StructField("images", ArrayType(MapType(StringType(), StringType())), True),  # List of dictionaries
    StructField("videos", ArrayType(MapType(StringType(), StringType())), True),  # List of dictionaries
    StructField("store", StringType(), True),
    StructField("categories", ArrayType(StringType()), True),
    StructField("details", MapType(StringType(), StringType()), True),  # Dictionary
    StructField("parent_asin", StringType(), True),
    StructField("bought_together", ArrayType(StringType()), True)
])

# Convert `meta_rdd` to a DataFrame with the specified schema
metadata_df = sc.createDataFrame(meta_rdd, schema=metadata_schema)

# Rename one of the `parent_asin` columns before joining to make them distinct
reviews_df = reviews_df.withColumnRenamed("parent_asin", "review_parent_asin")
metadata_df = metadata_df.withColumnRenamed("parent_asin", "meta_parent_asin")

combined_df = reviews_df.join(metadata_df, reviews_df.review_parent_asin == metadata_df.meta_parent_asin, "inner")

from pyspark.sql.functions import when, col, length, avg

# Categorize average ratings into Low, Medium, and High
categorized_df = combined_df.withColumn(
    "rating_category",
    when(col("average_rating") < 2, "Low")
    .when((col("average_rating") >= 2) & (col("average_rating") < 4), "Medium")
    .otherwise("High")
)

# Calculate review length
categorized_df = categorized_df.withColumn("review_length", length(col("text")))

# Group by rating categories and calculate average review length
result_df = categorized_df.groupBy("rating_category").agg(avg("review_length").alias("avg_review_length"))

# Show the results
print('----------- Average Review Length by Rating Category -----------')
for row in result_df.collect():
    print(row.rating_category,':', row.avg_review_length)
print('----------- End Average Review Length by Rating Category -----------')
print('\n')
rating_dist = combined_df.groupBy("rating").count().orderBy("rating")
print('----------- Distribution of Ratings -----------')
for row in rating_dist.collect():
    print(row.rating,":", row[1])
print('----------- End Distribution of Ratings -----------')

from pyspark.sql.functions import col

# Filter the DataFrame for verified purchases
verified_df = combined_df.filter(col('verified_purchase') == 'true')

# Count the total number of verified and non-verified reviews
# computationally faster than filtering for false
total_verified_reviews = verified_df.count()
total_reviews = combined_df.count()
non_verified_reviews = total_reviews - total_verified_reviews
print('----------- Verified Reviews -----------')

print(f'Total Verified Reviews: {total_verified_reviews}')
print(f'Total Reviews: {total_reviews}')
print(f'Non Verified Reviews: {non_verified_reviews}')
print('----------- End Verified Reviews -----------')

augmented_rdd = (
    reviews_rdd
    .map(
        lambda record: (
            record["rating"], 
            len(record["title"])
        )
    )
)
print('----------- Average Title Length by Rating -----------')

grouped_rdd = augmented_rdd.groupByKey()
average_rdd = grouped_rdd.mapValues(lambda lengths: sum(lengths) / len(lengths))
result = average_rdd.collect()
for row in rating_dist.collect():
    print(row[0],":", row[1])
print('----------- End Average Title Length by Rating -----------')

