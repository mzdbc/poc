# Databricks notebook source
# MAGIC %run "./test 01 - env settings"

# COMMAND ----------

file_path = f"{DA.paths.datasets}/airbnb/sf-listings/sf-listings-2019-03-06-clean.delta/"
airbnb_df = spark.read.format("delta").load(file_path)

# COMMAND ----------

airbnb_df.count()

# COMMAND ----------

airbnb_df.rdd.getNumPartitions()

# COMMAND ----------

train_df,test_df=airbnb_df.randomSplit([.8, .2], seed=42)
import mlflow
import mlflow.spark
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

with mlflow.start_run(run_name="LR-one-Feature") as run:
    # Define pipeline
    vec_assembler = VectorAssembler(inputCols=["bedrooms"], outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="price")
    pipeline = Pipeline(stages=[vec_assembler, lr])
    pipeline_model = pipeline.fit(train_df)

    # Log parameters
    mlflow.log_param("label", "price")
    mlflow.log_param("features", "bedrooms")

    # Log model
    mlflow.spark.log_model(pipeline_model, "model", input_example=train_df.limit(5).toPandas()) 

    # Evaluate predictions
    pred_df = pipeline_model.transform(test_df)
    regression_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="price")
    rmse = regression_evaluator.setMetricName("rmse").evaluate(pred_df)
    r2 = regression_evaluator.setMetricName("r2").evaluate(pred_df)

    # Log metrics
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", rmse)

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()
experiment_id = run.info.experiment_id
runs_df = mlflow.search_runs(experiment_id)

display(runs_df)

# COMMAND ----------

runs = client.search_runs(experiment_id, order_by=["attributes.start_time desc"], max_results=1)
runs[0].data.metrics

# COMMAND ----------

runs[0].info.run_id

# COMMAND ----------

model_path = f"runs:/{run.info.run_id}/log-model"
loaded_model = mlflow.spark.load_model(model_path)

display(loaded_model.transform(test_df))

# COMMAND ----------

model_name = f"{DA.cleaned_username}_sklearn_lr_test0"
run_id = run.info.run_id
model_uri = f"runs:/{run_id}/model"

model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()
model_version_details = client.get_model_version(name=model_name, version=1)

model_version_details.status

# COMMAND ----------

client.update_registered_model(
    name=model_details.name,
    description="This model forecasts Airbnb housing list prices based on various listing inputs.version test0"
)

# COMMAND ----------

client.update_model_version(
    name=model_details.name,
    version=model_details.version,
    description="This model version was built using OLS linear regression with sklearn."
)

# COMMAND ----------

client.transition_model_version_stage(
    name=model_details.name,
    version=model_details.version,
    stage="Production"
)

# COMMAND ----------

model_version_details = client.get_model_version(
    name=model_details.name,
    version=model_details.version
)
print(f"The current model stage is: '{model_version_details.current_stage}'")

# COMMAND ----------

import mlflow.pyfunc

model_version_uri = f"models:/{model_name}/1"

print(f"Loading registered model version from URI: '{model_version_uri}'")
model_version_1 = mlflow.pyfunc.load_model(model_version_uri)

# COMMAND ----------

model_version_1.predict(test_df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

dataset.display()
