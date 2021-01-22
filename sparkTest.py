from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MyCitiesApp").getOrCreate()
    df = spark.read.csv("s3://nakuldefaultest/cities.csv", header=True)
    df_scotland = df.filter(df.country == 'Scotland')
    df_scotland.coalesce(1).write.save('s3://nakuldefaultest/Scotland.csv',header=True, format='csv')
    spark.stop()
