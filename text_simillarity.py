from pyspark import SQLContext
from pyspark.context import SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import levenshtein 
from pyspark.sql.functions import col


def calculate_simillarity(new_df):
    new_df = new_df.withColumn(\
    "matching_levenshtein_dist",
    (levenshtein(col("description_x"), col("description_y"))))
    print(new_df.collect())


def main():
    sc = SparkContext(appName='TextSimillarity')
    sqlcont = SQLContext(sc)
    rdd = sc.textFile("test.csv")
    header = rdd.first()
    newrdd = rdd.filter(lambda x: x!= header)\
		 .map(lambda x: x.split(','))\
		 .map(lambda x: Row(description_x = x[1], description_y = x[2]))
    new_df = sqlcont.createDataFrame(newrdd)
    calculate_simillarity(new_df)



if __name__ == '__main__':
    main()