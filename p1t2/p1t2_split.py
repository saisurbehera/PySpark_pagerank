
   
import regex
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, lower
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_test.xml')

def extractLink(text):
    try:
        results = regex.findall(r'\[\[((?:[^[\]]+|(?R))*+)\]\]', text)
    except:
        return []
    output = []
    for res in results:
        for link in res.split('|'):
            if '#' in link:
                continue
            elif ':' in link and 'Category:' not in link:
                continue
            else:
                output.append(link.lower())
                break
    return output


link_udf = udf(lambda text: extractLink(text), ArrayType(StringType()))
new_df = df.withColumn("article", explode(link_udf(col("revision.text._VALUE"))))
new_df = new_df.select(lower(col('title')).alias('title'), 'article').orderBy('title', 'article')
new_df.repartition(10).write.option("delimiter", "\t").csv('p1t2_test_split')