from pyspark import pipelines as dlt
from pyspark.sql.functions import col

@dlt.table
def sample_users_feb_20_1646():
    return (
        spark.read.table("samples.wanderbricks.users")
        .select("user_id", "email", "name", "user_type")
    )


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

@dp.table
def sample_aggregation_feb_20_1646():
    return (
        spark.read.table("sample_users_feb_20_1646")
        .withColumn("valid_email", utils.is_valid_email(col("email")))
        .groupBy(col("user_type"))
        .agg(
            count("user_id").alias("total_count"),
            count_if("valid_email").alias("count_valid_emails")
        )
    )
