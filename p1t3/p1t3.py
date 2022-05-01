from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Connect to it 
spark = SparkSession.builder.getOrCreate()
df = spark.read.option("delimiter", "\t").csv('hdfs:/user/root/p1t2_large')

# remove the nans
df = df.filter("_c1 is not null")


df_count = df.groupBy("_c0").count()
# Set intial rank as 1
new_df = df_count.withColumn("rank", lit(1))
new_df = new_df.rdd.map(lambda x: (x[0], x[1], x[2], x[2]/x[1])).toDF(['article', 'count', 'rank', 'contribution'])

k = 10
for _ in range(k):
    new_df = df.join(new_df, df._c0 == new_df.article, 'inner')
    new_df = new_df.groupBy(['_c1']).sum('contribution').rdd.map(lambda x: (x[0], 0.15 + 0.85 * x[1])).toDF(['article', 'rank'])
    new_df = df_count.join(new_df,  new_df.article == df._c0 , 'left')
    # We get null values, we replace them at 0.15 as the base value
    new_df = new_df.na.fill(0.15)
    new_df = new_df.rdd.map(lambda x:(x[0], x[1], x[3], x[3]/x[1])).toDF(['article', 'count', 'rank', 'contribution'])

new_df = new_df.select('article', 'rank').orderBy('article', 'rank')
new_df.show()


new_df.repartition(10).write.option("delimiter", "\t").csv('pagerank_large')