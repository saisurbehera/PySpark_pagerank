from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf , explode,lower, rtrim, regexp_replace
from pyspark.sql.types import StringType,ArrayType
import regex

# Get the spark instance and get the file 
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_test.xml')

# Get the find all  of particular type 
def return_links(text):
    try:
        innerlink = regex.findall(r'\[\[((?:[^[\]]+|(?R))*+)\]\]', text)
        if innerlink:
            return innerlink
        return None
    except:
        return None    


return_links_udf = udf(lambda text: return_links(text), ArrayType(StringType()))
new_df = df.select(col('title'), col('revision.text._VALUE').alias('text'))
new_df = new_df.withColumn('article', explode(return_links_udf(lower(col('text')))))
new_df = new_df.withColumn('article', regexp_replace(col('article'),".*\#[^\|]+\|?",""))
new_df = new_df.withColumn('article', regexp_replace(col('article'),".*(?!category)\:[^\|]+\|?",""))
new_df = new_df.withColumn('article', regexp_replace(col('article'),"\|.*",""))
new_df = new_df.filter("article != ''")
new_df = new_df.select(lower(col('title')).alias('title'), 'article')
new_df.repartition(10).write.mode("overwrite").option("delimiter", "\t").csv('part2_testq2')

