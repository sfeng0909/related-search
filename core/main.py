from path import *
from utils import *
import os
# spark = SparkSession.builder.getOrCreate()
# spark.stop()
spark = hive_spark('statistics')

date = (datetime.date.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")  # yesterday
days = -90
print('Related Search Algo is starting')

# step 1: Get the query, content data from S3
save_data_frame(load_searched_df(spark, searched_path, date, days), f'{medium_path}/query_content_count')
query_content_count_df = load_data_frame(spark, f'{medium_path}/query_content_count')
save_data_frame(get_query_count(query_content_count_df), f'{medium_path}/query_count')
query_count_df = load_data_frame(spark, f'{medium_path}/query_count')
print('Step 1: Get data from S3 (Finished)')

# step 2: Generate rewrited_query DataFrame
save_data_frame(gen_len_coef(spark), f'{medium_path}/len_coef')
len_coef_df = load_data_frame(spark, f'{medium_path}/len_coef')
save_data_frame(get_content2words(query_content_count_df, len_coef_df), f'{medium_path}/content2words')
content2words_df = load_data_frame(spark, f'{medium_path}/content2words')
save_data_frame(rewrite_query_v2(query_content_count_df, query_count_df,content2words_df), f'{medium_path}/rewrited_query_v2')
rewrited_query_v2_df = load_data_frame(spark, f'{medium_path}/rewrited_query_v2')
print('Step 2: Rewrite query (Finished)')

# step 3: Content-based CF
save_data_frame(gen_similarity(query_content_count_df, "content_id"), f'{medium_path}/content_similarity_v2')
content_similarity_df = load_data_frame(spark, f'{medium_path}/content_similarity_v2')  # 74017748
save_data_frame(get_q2q_by_content(query_content_count_df, content_similarity_df), f'{medium_path}/q2q_by_content_v2')
q2q_by_content_df = load_data_frame(spark, f'{medium_path}/q2q_by_content_v2')
save_data_frame(get_complete_top_q2q_v2(q2q_by_content_df, rewrited_query_v2_df, 'content'), f'{medium_path}/top_q2q_by_content_v2')
top_q2q_by_content_df = load_data_frame(spark, f'{medium_path}/top_q2q_by_content_v2')
print('Step 3: CF (Finished)')

# step 4: Save results for Airflow
q2q_df = filter_q2q(top_q2q_by_content_df, query_content_count_df)
save_data_frame(q2q_df, f'{us_data_path}/q2q_final/cd={date}')
q2q_df = load_data_frame(spark, f'{us_data_path}/q2q_final/cd={date}')
save_data_frame(get_q2q_output(q2q_df),  f'{sg_data_path}/q2q_final_csv/cd={date}', 'csv', True)
q2q_final_df = load_data_frame(spark, f'{sg_data_path}/q2q_final_csv/cd={date}', 'csv', True)
print('Step 4: Save results (Finished)')

path_df = spark.createDataFrame([f'{sg_data_path}/q2q_final_csv/cd={date}'], StringType()) \
    .withColumnRenamed('value', 'latest_path')
save_data_frame(path_df, f'{sg_data_path}/related_query_table_path.json', 'json')
print('Done!')

