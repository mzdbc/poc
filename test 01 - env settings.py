# Databricks notebook source
# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

from pyspark.ml.feature import Imputer
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, translate
from pyspark.sql.functions import when

# COMMAND ----------

file_path = f"{DA.paths.datasets}/airbnb/sf-listings/sf-listings-2019-03-06.csv"
raw_df = spark.read.csv(file_path, header="true", inferSchema="true", multiLine="true", escape='"')
raw_df.display()

# COMMAND ----------

host_col=['host_id',
 'host_url',
 'host_name',
 'host_since',
 'host_location',
 'host_about',
 'host_response_time',
 'host_response_rate',
 'host_acceptance_rate',
 'host_is_superhost',
 'host_thumbnail_url',
 'host_picture_url',
 'host_neighbourhood',
 'host_listings_count',
 'host_total_listings_count',
 'host_verifications',
 'host_has_profile_pic',
 'host_identity_verified']

# COMMAND ----------

host_df=raw_df.select(host_col).drop_duplicates()
nonhost_df=raw_df.drop(*host_col[1:]).drop_duplicates()

# COMMAND ----------

host_df.repartition(1).write.format("csv").options(header='true').mode("append").save(f"{DA.paths.working_dir}/split_data")
nonhost_df.repartition(1).write.format("parquet").options(header='true').mode("append").save(f"{DA.paths.working_dir}/split_data")

# COMMAND ----------

# nonhost_df.join(host_df,nonhost_df.host_id==host_df.host_id,'left').count()

# COMMAND ----------

path=f'{DA.paths.working_dir}/split_data/'

# COMMAND ----------

# dbutils.fs.ls(path)

# COMMAND ----------

for i in range(0,len(dbutils.fs.ls(path))):
  tp=dbutils.fs.ls(path)[i].path.split('.')[-1]
  if tp=='csv':
      df0 = spark.read.csv(dbutils.fs.ls(path)[i].path, header="true", inferSchema="true", multiLine="true", escape='"')
  elif tp=='parquet':
      df1 = spark.read.parquet(dbutils.fs.ls(path)[i].path, header="true", inferSchema="true", multiLine="true", escape='"')
raw_df=df0.join(df1,df0.host_id==df1.host_id,'right')

# COMMAND ----------

columns_to_keep = [
    "host_is_superhost",
    "cancellation_policy",
    "instant_bookable",
    "host_total_listings_count",
    "neighbourhood_cleansed",
    "latitude",
    "longitude",
    "property_type",
    "room_type",
    "accommodates",
    "bathrooms",
    "bedrooms",
    "beds",
    "bed_type",
    "minimum_nights",
    "number_of_reviews",
    "review_scores_rating",
    "review_scores_accuracy",
    "review_scores_cleanliness",
    "review_scores_checkin",
    "review_scores_communication",
    "review_scores_location",
    "review_scores_value",
    "price"
]

# COMMAND ----------

def df_cleansing(columns_to_keep,raw_df):
    base_df = raw_df.select(columns_to_keep)
    fixed_price_df = base_df.withColumn("price", translate(col("price"), "$,", "").cast("double"))
    pos_prices_df = fixed_price_df.filter(col("price") > 0)
    min_nights_df = pos_prices_df.filter(col("minimum_nights") <= 365)
    integer_columns = [x.name for x in min_nights_df.schema.fields if x.dataType == IntegerType()]
    doubles_df = min_nights_df

    for c in integer_columns:
        doubles_df = doubles_df.withColumn(c, col(c).cast("double"))
    
    impute_cols = ["bedrooms","bathrooms","beds","review_scores_rating","review_scores_accuracy","review_scores_cleanliness","review_scores_checkin","review_scores_communication","review_scores_location","review_scores_value"]

    for c in impute_cols:
        doubles_df = doubles_df.withColumn(c + "_na", when(col(c).isNull(), 1.0).otherwise(0.0))
        
    imputer = Imputer(strategy="median", inputCols=impute_cols, outputCols=impute_cols)
    imputer_model = imputer.fit(doubles_df)
    imputed_df = imputer_model.transform(doubles_df)
    
    imputed_df.write.format("delta").mode("overwrite").save(f"{DA.paths.working_dir}/imputed_results")
    
    
    return dbutils.data.summarize(fixed_price_df)
    

# COMMAND ----------

df_cleansing(columns_to_keep,raw_df)

# COMMAND ----------



# COMMAND ----------

# raw_df0 = spark.read.csv(f"{DA.paths.working_dir}/split_data/part-00000-tid-2074984299231131329-e95ad724-406d-4824-a97f-8b3c8316e835-644-1-c000.csv", header="true", inferSchema="true", multiLine="true", escape='"')
# raw_df1 = spark.read.parquet(f"{DA.paths.working_dir}/split_data/part-00000-tid-2852088437837493957-e87e4ba4-e9b1-44fa-81ad-f8c500b075ab-646-1-c000.snappy.parquet", header="true", inferSchema="true", multiLine="true", escape='"')
# raw_df2 = spark.read.json(f"{DA.paths.working_dir}/split_data/part-00000-tid-1254634605665645173-8d6e5ad0-8375-4d19-93f5-dd7e3fb93c88-648-1-c000.json", multiLine="false")
