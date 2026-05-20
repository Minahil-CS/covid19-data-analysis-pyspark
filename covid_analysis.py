from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, max as spark_max, avg, min as spark_min

spark = SparkSession.builder.appName("COVID19").getOrCreate()

# 1. Load Data
df = spark.read.csv("D:/download/owid-covid-data.csv", header=True, inferSchema=True)

# 2. Show Data
df.show(5)

# 3. Print Schema
df.printSchema()

# 4. Select
df.select("location","date","total_cases","total_deaths").show(10)
df.select("location","date","total_cases","total_deaths").show()

# 5. Filter - Pakistan
df.filter(df.location == "Pakistan").select("location","date","total_cases","total_deaths").show(10)

# 6. Where - Pakistan
df.where(df.location == "Pakistan").select("location","date","total_cases","total_deaths").show(10)

# 7. Select into df2
df2 = df.select("location","date","total_cases","total_deaths")

# 8. GroupBy Count
df2.groupBy("location").count().show()

# 9. GroupBy Aggregation
from pyspark.sql.functions import max as spark_max
df2.groupBy("location").agg(spark_max("total_cases").alias("Total_Cases"), spark_max("total_deaths").alias("Total_Deaths")).show(20)

# 10. Sort Ascending
df2.orderBy("total_cases").select("location","total_cases","total_deaths").show()

# 11. Sort Descending
from pyspark.sql.functions import desc
df2.orderBy(desc("total_cases")).select("location","total_cases","total_deaths").show()

# 12. Add Column
df3 = df2.withColumn("total_affected", df2.total_cases + df2.total_deaths)
df3.select("location","total_cases","total_deaths","total_affected").show()

# 13. Rename Column
df4 = df3.withColumnRenamed("total_cases","Total_Cases")
df4.select("location","Total_Cases","total_deaths").show()

# 14. Drop Column
df5 = df2.drop("iso_code")
df5.show(5)

# 15. Distinct Values
df2.select("location").distinct().show()

# 16. Count Rows
df2.count()

# 17. Describe
df2.select("total_cases","total_deaths").describe().show()

# 18. Drop NA
df3 = df.dropna()
df3.show(5)

# 19. Fill NA
df4 = df.fillna(0)
df4.select("location","date","total_cases","total_deaths").show(10)

# 20. Join Operation
from pyspark.sql.functions import col
df_asia = df2.filter(col("continent") == "Asia").select("location","total_cases")
df_deaths = df2.filter(col("continent") == "Asia").select("location","total_deaths")
df_joined = df_asia.join(df_deaths, "location")
df_joined.show(10)

# 21. Filter Cases > 1000000
df2.filter(df2.total_cases > 1000000).select("location","total_cases","total_deaths").show(10)

# 22. Avg Deaths per Continent
from pyspark.sql.functions import avg
df2.groupBy("continent").agg(avg("total_deaths").alias("Avg_Deaths")).show()

# 23. Min Max Cases
from pyspark.sql.functions import min as spark_min
df2.agg(spark_max("total_cases").alias("Max_Cases"), spark_min("total_cases").alias("Min_Cases")).show()

# 24. Save Output
df2.write.csv("D:/covid_output", header=True, mode="overwrite")