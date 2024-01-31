# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table scd2(
# MAGIC   pk1 int ,
# MAGIC   pk2 string,
# MAGIC   dim1 int,
# MAGIC   dim2 int,
# MAGIC   dim3 int,
# MAGIC   dim4 int,
# MAGIC   active_status string,
# MAGIC   start_date timestamp,
# MAGIC   end_date timestamp)
# MAGIC   using delta
# MAGIC   location 'dbfs:/mnt/raw/scd2'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into scd2 values(111,"unit1",200,500,800,400,'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into scd2 values(222,"unit2",900,NULL,700,100,'Y',current_timestamp(),'9999-12-31');
# MAGIC insert into scd2 values(333,"unit3",300,900,250,650,'Y',current_timestamp(),'9999-12-31');

# COMMAND ----------

from delta import *
targetTable = DeltaTable.forPath(spark,"dbfs:/mnt/raw/scd2")
targetDF=targetTable.toDF()
display(targetDF)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *


schema = StructType([StructField("pk1",StringType(),True),\
                    StructField("pk2",StringType(),True),\
                    StructField("dim1",IntegerType(),True),\
                    StructField("dim2",IntegerType(),True),\
                    StructField("dim3",IntegerType(),True),\
                    StructField("dim4",IntegerType(),True),\
                        ])

# COMMAND ----------

data=[(111,"unit1",200,500,800,400),
      (222,"unit2",800,1300,800,500),
      (444,"unit4",100,None,700,300)]

sourceDF = spark.createDataFrame(data=data,schema=schema)
display(sourceDF)

# COMMAND ----------

joinDF=sourceDF.join(targetDF, (sourceDF.pk1 == targetDF.pk1)&\
        (sourceDF.pk2 == targetDF.pk2)&\
        (targetDF.active_status == "Y"),"leftouter")\
    .select(sourceDF["*"],\
            targetDF.pk1.alias("target_pk1"),\
            targetDF.pk2.alias("target_pk2"),\
            targetDF.dim1.alias("target_dim1"),\
            targetDF.dim2.alias("target_dim2"),\
            targetDF.dim3.alias("target_dim3"),\
            targetDF.dim4.alias("target_dim4"))
display(joinDF)

# COMMAND ----------

filterDF = joinDF.filter(xxhash64(joinDF.dim1,joinDF.dim2,joinDF.dim3,joinDF.dim4)!=xxhash64(joinDF.target_dim1,joinDF.target_dim2,joinDF.target_dim3,joinDF.target_dim4))

display(filterDF)

# COMMAND ----------

mergeDF = filterDF.withColumn("MERGEKEY",concat(filterDF.pk1,filterDF.pk2))

display(mergeDF)

# COMMAND ----------

dummyDF=filterDF.filter("target_pk1 is not null").withColumn("MERGEKEY",lit(None))
display(dummyDF)

# COMMAND ----------

scdDF = mergeDF.union(dummyDF)

display(scdDF)

# COMMAND ----------

targetTable.alias("target").merge(
    source = scdDF.alias("source"),
    condition = "concat(target.pk1,target.pk2) = source.MERGEKEY and target.active_status ='Y' "
).whenMatchedUpdate(set=
                    {
                        "target.active_status":"'N'",
                        "target.end_date" : "current_date"
                    }).whenNotMatchedInsert(values =
                                            {
                                                "pk1":"source.pk1",
                                                "pk2":"source.pk2",
                                                "dim1":"source.dim1",
                                                "dim2":"source.dim2",
                                                "dim3":"source.dim3",
                                                "dim4":"source.dim4",
                                                "active_status":"'Y'",
                                                "start_date":"current_date",
                                                "end_date":"""to_date('9999-12-31','yyyy-MM-dd')"""

                                            }).execute()

# COMMAND ----------

# from datetime import date
# from pyspark.sql.functions import col, current_date, to_date
# notactice_status="N"
# targetTable.alias("target").merge(
#      source=scdDF.alias("source"),
#     condition="concat(target.pk1, target.pk2) = source.MERGEKEY and target.active_status ='Y'"
# ).whenMatchedUpdate(set={
#     "target.active_status":'N', 
#     "target.end_date": current_date()
# }).whenNotMatchedInsert(values={
#     "pk1": col("source.pk1"),
#     "pk2": col("source.pk2"),
#     "dim1": col("source.dim1"),
#     "dim2": col("source.dim2"),
#     "dim3": col("source.dim3"),
#     "dim4": col("source.dim4"),
#     "active_status": "Y",
#     "start_date": current_date(),
#     "end_date": to_date("9999-12-31", "yyyy-MM-dd")
# }).execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd2;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history scd2;

# COMMAND ----------

df=spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/mnt/raw/newData")

# COMMAND ----------

df.write.mode("overwrite").option("header" ,"true" ).save("abfss://new-container@storagefordemotesting.dfs.core.windows.net/newData1")

# COMMAND ----------

df1=spark.read.format("delta").load("abfss://new-container@storagefordemotesting.dfs.core.windows.net/newData1/")

# COMMAND ----------

display(df1)
