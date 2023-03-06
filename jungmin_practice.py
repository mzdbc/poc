# Databricks notebook source
# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

file_path = f"{DA.paths.datasets}/airbnb/sf-listings/sf-listings-2019-03-06.csv"

raw_df = spark.read.csv(file_path, header="true", inferSchema="true", multiLine="true", escape='"')

display(raw_df)

# COMMAND ----------

dbutils.data.summarize(raw_df.select("host_is_superhost"))
#superhost 수=2932

# COMMAND ----------

from pyspark.sql.functions import col, translate
fixed_df=raw_df.filter(col("price") > 0)

# COMMAND ----------

gtk=[
    'host_id',
    'host_is_superhost',
    'price',
    'security_deposit',
    'cleaning_fee',
    'host_total_listings_count',
    'accommodates',
    'bedrooms',
    'bathrooms',
    'review_scores_rating',
    'number_of_reviews'
]

# COMMAND ----------

#price_df는 필요한 column들만 넣고, string으로 저장된 가격 정보들을 double로 변환
price_df = raw_df.select(gtk)
price_df = price_df.withColumn("price", translate(col("price"), "$,", "").cast("double"))
price_df = price_df.withColumn("security_deposit", translate(col("price"), "$,", "").cast("double"))
price_df = price_df.withColumn("cleaning_fee", translate(col("price"), "$,", "").cast("double"))
price_df.cache().count()
display(price_df)

# COMMAND ----------

total_df = price_df.withColumn('total_price', col('price') + col('cleaning_fee')+ col('security_deposit'))

# COMMAND ----------

#price에 cleaning_fee와 security_deposit 더한 테이블 = total_df
display(total_df)

# COMMAND ----------

from pyspark.sql.functions import corr,col, when

gtk_df = price_df.withColumn("host_is_superhost", when(col("host_is_superhost") == "t", 1).when(col("host_is_superhost") == "f", 0))
gtk2_df = total_df.withColumn("host_is_superhost", when(col("host_is_superhost") == "t", 1).when(col("host_is_superhost") == "f", 0))

display(gtk_df)
display(gtk2_df)

# 데이터프레임에서 price와 host_is_superhost 열을 선택
df = gtk_df.select("price", "host_is_superhost")

# corr 함수를 사용하여 두 열 간의 상관 관계 계산
#superhost면 가격이 낮아지는 경향이 있다, 하지만 0에 가까움
correlation = df.stat.corr("price", "host_is_superhost")

print(f"The correlation between price and superhost is: {correlation}")

# COMMAND ----------

df2 = gtk2_df.select("total_price", "host_is_superhost")
correlation = df2.stat.corr("total_price", "host_is_superhost")

print(f"The correlation between total_price and superhost is: {correlation}")

# COMMAND ----------

#양의 상관 관계이므로 bathroom 숫자가 증가하면 가격도 증가한다(당연함)
bath_df=price_df.select('price','bathrooms')
correlation = bath_df.stat.corr('price','bathrooms')

print(f"The correlation between total_price and superhost is: {correlation}")

# COMMAND ----------

#seaborn 라이브러리의 heatmap 함수 사용하여 시각화
import seaborn as sns
import numpy as np

eda_df=gtk_df.select("price", "host_is_superhost",'number_of_reviews')

sns.heatmap(eda_df.toPandas().corr(), annot=True, cmap='magma')

# COMMAND ----------

review_df= raw_df.select(
"host_is_superhost",
"review_scores_rating",
"review_scores_accuracy",
"review_scores_cleanliness",
"review_scores_checkin",
"review_scores_communication",
"review_scores_location",
"review_scores_value"
)

# COMMAND ----------

display(review_df.describe())

# COMMAND ----------

review_df.filter(col("review_scores_rating").isNull()).count()

# COMMAND ----------

얘기 들을 때 키워드 뜻이 뭔지 eda가 뭔지
method 변형시키면서 연습해봐라
있는것만하면 늘지 않는다
api 문서 찾아봐라
scrum엑셀시트에 뭐하는지 작성해라
