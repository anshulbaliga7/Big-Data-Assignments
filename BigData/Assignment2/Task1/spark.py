#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, desc
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType
import sys

def criminalfinder(spark, inputs, outputs):
    cases_2012 = spark.read.csv(inputs[0], header=True, inferSchema=True)
    cases_2013 = spark.read.csv(inputs[1], header=True, inferSchema=True)
    cases_2014 = spark.read.csv(inputs[2], header=True, inferSchema=True)
    cases_state_key = spark.read.csv(inputs[3], header=True, inferSchema=True)
    judges_clean = spark.read.csv(inputs[4], header=True, inferSchema=True)
    judge_case_merge_key = spark.read.csv(inputs[5], header=True, inferSchema=True)
    acts_sections = spark.read.csv(inputs[6], header=True, inferSchema=True)

    cases_2012 = cases_2012.na.drop(subset=['ddl_case_id', 'state_code'])
    cases_2013 = cases_2013.na.drop(subset=['ddl_case_id', 'state_code'])
    cases_2014 = cases_2014.na.drop(subset=['ddl_case_id', 'state_code'])
    cases_state_key = cases_state_key.na.drop(subset=['year','state_code','state_name'])
    judge_case_merge_key = judge_case_merge_key.na.drop(subset=['ddl_case_id','ddl_decision_judge_id'])

    merged_data = cases_2012.union(cases_2013).union(cases_2014)
    cases_state_key_filtered = cases_state_key.filter(col('year').isin([2012, 2013, 2014]))
    merged_data = merged_data.join(cases_state_key_filtered, on=['year', 'state_code'])

    merged_data = merged_data.dropDuplicates(['ddl_case_id'])

    state_crime_data = merged_data.groupBy('state_code', 'state_name').agg(count('*').alias('state_code_find'))

    order_data_all = Window.orderBy(F.desc('state_code_find'))
    ranked_states = state_crime_data.withColumn('rank', F.rank().over(order_data_all))

    top_10_states = ranked_states.filter('rank <= 10').filter(col('state_code').isNotNull() & col('state_name').isNotNull())

    final_states_list = top_10_states.rdd.map(lambda row: row.state_name).collect()

    merged_judges = merged_data.join(judge_case_merge_key, on='ddl_case_id')

    merged_judges = merged_judges.join(acts_sections.select('ddl_case_id', 'criminal'), on='ddl_case_id', how='inner')

    judge_counts = merged_judges.filter(col('criminal') == 1).groupBy('ddl_decision_judge_id').agg(count('*').alias('criminal_case_count'))
    
    judge_counts = judge_counts.na.drop(subset=['ddl_decision_judge_id', 'criminal_case_count'])

    final_judge_counts = judge_counts.orderBy(col('criminal_case_count').desc())

    max_judge = final_judge_counts \
    .filter(col('ddl_decision_judge_id').isNotNull()) \
    .select('ddl_decision_judge_id') \
    .first()

    if max_judge:
         output_tuple = (final_states_list, max_judge.ddl_decision_judge_id)
         with open(outputs, "w") as finale:
             finale.write(str(output_tuple))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PES1UG21CS095_SparkAssignment").getOrCreate()

    inputs = [sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7]]  
    outputs = sys.argv[8]

    criminalfinder(spark, inputs, outputs)

    spark.stop()


