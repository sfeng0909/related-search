us_root_path = 's3://hotstar-ads-ml-us-east-1-prod/related_search'
sg_root_path = "s3://adtech-ml-perf-ads-ap-southeast-1-prod-v1/related_search"
us_data_path = f'{us_root_path}/data'
sg_data_path = f'{sg_root_path}/data'
us_code_path = f'{us_root_path}/code'
medium_path = f'{us_data_path}/medium'

dp_datalake_bucket = 's3://hotstar-dp-datalake-processed-us-east-1-prod'
dp_event_path = f'{dp_datalake_bucket}/events'
searched_path = f'{dp_event_path}/searched'
clip_table = 'in_cms.clip_update_s3'
movie_table = 'in_cms.movie_update_s3'
episode_table = 'in_cms.episode_update_s3'
match_table = 'in_cms.match_update_s3'
tv_show_table = 'in_cms.tv_show_update_s3'
tv_season_table = 'in_cms.tv_season_update_s3'
tournament_table = 'in_cms.tournament_update_s3'
sports_season_table = 'in_cms.sports_season_update_s3'
channel_table = 'in_cms.channel_update_s3'
lang_table = 'in_cms.lang_update_s3'

QUERY_MAX_LENGTH = 50
MAX_TARGETS_PER_REFERENCE = 1000
TOP_SIMILAR_CONTENT = 20
TOP_CONTENT_PER_QUERY = 20
TOP_QUERIES_PER_CONTENT = 20
TOP_QUERIES_PER_QUERY = 10
RATIO_THRESHOLD = 0.5
TEXT_MAX_LENGTH = 1000
MIN_DAILY_QUERY_COUNT = 10
MIN_TOKEN_COUNT = 100
REMOVE_META_COL = ['description', 'short_summary', 'search_keyword']
PLATFORM = ['android','ios','web']
STOPWORDS = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd",
             'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers',
             'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which',
             'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been',
             'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but',
             'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against',
             'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down',
             'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when',
             'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no',
             'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don',
             "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't",
             'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven',
             "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan',
             "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn',
             "wouldn't", 'star', 'back']
SP_STOPWORDS = ['star', 'star plus']

import datetime
import getopt
import os
import sys
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel

storageLevel = StorageLevel.DISK_ONLY


def load_data_frame(spark: SparkSession, path: str, fmt: str = 'parquet', header: bool = False, delimiter: str = ','
                    ) -> DataFrame:
    if fmt == 'parquet':
        return spark.read.parquet(path)
    elif fmt == 'orc':
        return spark.read.orc(path)
    elif fmt == 'json':
        return spark.read.json(path)
    elif fmt == 'csv':
        return spark.read.option('header', header).option('delimiter', delimiter).csv(path)
    else:
        print("the format is not supported")
        return DataFrame(None, None)


def save_data_frame(df: DataFrame, path: str, fmt: str = 'parquet', header: bool = False, delimiter: str = ',') -> None:
    def save_data_frame_internal(df: DataFrame, path: str, fmt: str = 'parquet', header: bool = False,
                                 delimiter: str = ',') -> None:
        if fmt == 'parquet':
            df.write.mode('overwrite').parquet(path)
        elif fmt == 'orc':
            df.write.mode('overwrite').orc(path)
        elif fmt == 'csv':
            df.coalesce(1).write.option('header', header).option('delimiter', delimiter).mode('overwrite').csv(path)
        elif fmt == 'csv_zip':
            df.write.option('header', header).option('delimiter', delimiter).option("compression", "gzip").mode(
                'overwrite').csv(path)
        elif fmt == 'json':
            df.coalesce(1).write.mode('overwrite').json(path)
        else:
            print("the format is not supported")
    df.persist(storageLevel)
    try:
        save_data_frame_internal(df, path, fmt, header, delimiter)
    except Exception:
        try:
            save_data_frame_internal(df, path, fmt, header, delimiter)
        except Exception:
            save_data_frame_internal(df, path, fmt, header, delimiter)
    df.unpersist()


def check_date(date: str) -> None:
    try:
        datetime.datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        print('date should be in format YYYY-mm-dd')
        sys.exit()


def date_add(date: str, days: int) -> str:
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return (dt + datetime.timedelta(days=days)).strftime('%Y-%m-%d')


def get_date_list(date: str, days: int) -> list:
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    if -1 <= days <= 1:
        return [date]
    elif days > 1:
        return [(dt + datetime.timedelta(days=n)).strftime('%Y-%m-%d') for n in range(0, days)]
    else:
        return [(dt + datetime.timedelta(days=n)).strftime('%Y-%m-%d') for n in range(days + 1, 1)]


def check_argv(argv: list) -> str:
    date = ''
    try:
        opts, args = getopt.getopt(argv, 'd:', ['date='])
    except getopt.GetoptError:
        print('aggregate_app_installed.py -d <date>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ('-d', '--date'):
            date = arg
        else:
            print('aggregate_app_installed.py -d <date>')
            sys.exit()
    print(f'the running date is {date}')
    return date


def load_device_meta(spark: SparkSession, device_meta_state_path: str) -> DataFrame:
    device_meta_path = load_data_frame(spark, device_meta_state_path, 'json').select('snapshot_location').head()[0]
    return load_data_frame(spark, device_meta_path, 'orc').select(
        "_col0", "_col1", "_col3", "_col4", "_col5", "_col6", "_col7").toDF(
        "device_id", "pid", "advertisingid", "dw_d_id", "dw_p_id", "platform", "received_at").filter(
        'pid is not null and pid not in ("", "null")').filter(
        'device_id is not null and device_id not in ("", "null", "gdprDeviceId")').filter(
        'dw_p_id is not null and dw_p_id != ""').filter(
        'advertisingid != "00000000-0000-0000-0000-000000000000"').filter(
        'platform is not null').select(
        'dw_p_id', 'dw_d_id', 'device_id', 'platform', 'received_at')


def load_user_meta(spark: SparkSession, user_meta_state_path: str) -> DataFrame:
    user_meta_path = load_data_frame(spark, user_meta_state_path, 'json').select('snapshot_location').head()[0]
    return load_data_frame(spark, user_meta_path, 'orc'). \
        toDF('dw_pid', 'age', 'created_on', 'ext_auth_id', 'fname', 'gender', 'hid', 'joined_on',
             'last_updated_on', 'pid', 'signup_country_code', 'signup_platform', 'subs_details',
             'subscription', 'user_status', 'user_type', 'email', 'version')


def load_user_meta2(spark: SparkSession, user_meta_state_path: str) -> DataFrame:
    user_meta_path = load_data_frame(spark, user_meta_state_path, 'json').select('snapshot_location').head()[0]
    return load_data_frame(spark, user_meta_path, 'orc'). \
        toDF('key', 'subs', 'timestamp', 'acnlastupdatedon', 'extauthid', 'partners', 'lastupdatedon',
             'userstatus', 'signupdeviceid', 'lname', 'phonenumber', 'ispasswordrehashed', 'fname', 'email',
             'subscriptions', 'joinedon', 'signupcountrycode', 'usertype', 'isemailverified',
             'isuserconsentgiven', 'fblastupdatedon', 'gender', 'subsdetails', 'pid', 'isphoneverified', 'hid',
             'age', 'signupplatform', 'version', 'is_deleted')


def get_table_stat(spark: SparkSession, data_frame: DataFrame) -> DataFrame:
    table = "table"
    data_frame.createOrReplaceTempView(table)
    total_count = float(data_frame.count())
    def func(field: str, type_name: str) -> Row:
        count = spark.sql(f"""
        select count(`{field}`) as count
        from `{table}`
        where `{field}` is not null and cast(`{field}` as string) not in ('')
        """).rdd.map(lambda r: r[0]).take(1)[0]
        unique_count = spark.sql(f"""
        select count(distinct `{field}`) as count
        from `{table}`
        where `{field}` is not null and cast(`{field}` as string) not in ('')
        """).rdd.map(lambda r: r[0]).take(1)[0]
        typical_values = spark.sql(f"""
        select cast(`{field}` as string) as `{field}`, count(1) as count
        from `{table}`
        where `{field}` is not null and cast(`{field}` as string) not in ('')
        group by `{field}`
        order by count desc
        """).select(f'`{field}`').rdd.map(lambda r: r[0]).take(10)
        return Row(field, type_name, "", count, count / total_count, unique_count,
                   "" if len(typical_values) == 0 else reduce(lambda x, y: f'{x}, {y}', typical_values), "")
    stat = list(map(lambda f: func(f.name, f.dataType.typeName()), data_frame.schema.fields))
    schema = StructType([StructField("field", StringType(), nullable=True),
                         StructField("format", StringType(), nullable=True),
                         StructField("description", StringType(), nullable=True),
                         StructField("# of valid values", LongType(), nullable=True),
                         StructField("coverage", DoubleType(), nullable=True),
                         StructField("# of unique values", LongType(), nullable=True),
                         StructField("typical values", StringType(), nullable=True),
                         StructField("comment", StringType(), nullable=True)])
    return spark.createDataFrame(spark.sparkContext.parallelize(stat), schema).coalesce(1)


def remove_kids_profile(spark: SparkSession, df: DataFrame) -> DataFrame:
    print("before rm kids", df.count())
    kids_p_id = spark.sql("select dw_p_id from data_warehouse.user_profile where profile_type = 'KIDS'")
    res = df.join(kids_p_id, "dw_p_id", "left_anti")
    print("after rm kids", res.count())
    return res


def hive_spark(name: str) -> SparkSession:
    return SparkSession.builder \
        .appName(name) \
        .config("hive.metastore.uris", "thrift://10.10.243.137:9083") \
        .config("spark.kryoserializer.buffer.max", "128m") \
        .enableHiveSupport() \
        .getOrCreate()


def load_hive_table(spark: SparkSession, table: str, date: str = None) -> DataFrame:
    if date is None:
        return spark.sql(f'select * from {table}')
    else:
        return spark.sql(f'select * from {table} where cd = "{date}"')


def sendMsgToSlack(topic, title, message, region):
    cmd = "aws sns publish --topic-arn " + topic + " --subject " + title + " --message " + message + " --region " + region
    print(cmd)
    os.system(cmd)

import re
import unicodedata

import pyspark.sql.functions as F
from pyspark.sql.types import *

def normalize_query(query, max_length=50):
    if query is None:
        return query
    query = unicodedata.normalize('NFKD', query).lower()
    query = re.sub(r'[^\x00-\x7F]+', '', query)
    query = re.sub(r'[_,;]', ' ', query)
    query = re.sub(r'\s+', ' ', query).strip()
    if len(query) > max_length:
        pos = query[:max_length + 1].rfind(' ')
        query = query[:max(pos, 0)]
    return query if len(query) > 0 else None

def is_sub_text(query1, query2):
    if query2 in query1:
        return False
    else:
        return True

normalize_query_udf = F.udf(normalize_query, StringType())
is_sub_text_udf = F.udf(is_sub_text, BooleanType())

import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
import math


def load_searched_df(spark, searched_path, date, days):
    '''
    Return:
        query|content_id|count
    '''
    return reduce(lambda x, y: x.union(y),
                  [load_data_frame(spark, f'{searched_path}/cd={dt}')
                  .select(normalize_query_udf(F.col('query'), F.lit(QUERY_MAX_LENGTH)).alias('query'),
                          F.col('clicked_content_id').alias('content_id'), F.col('platform'))
                  .coalesce(16)
                   for dt in get_date_list(date, days)]) \
        .filter(F.length('query') > 2) \
        .filter(F.col('platform').isin(PLATFORM)) \
        .groupBy('query', 'content_id').count()


def get_query_count(query_content_count_df):
    '''
    Return:
        query|count
    '''
    return query_content_count_df \
        .groupBy('query').agg(F.sum('count').alias('count'))


def gen_similarity(query_content_count_df, target):
    '''
    Compute the query similarity matrix.
    Return:
        query similarity dataframe
    '''
    reference = 'content_id' if target == 'query' else 'query'
    query_content_df = query_content_count_df \
        .withColumn('rank', F.row_number().over(W.partitionBy(reference).orderBy(F.desc('count')))) \
        .filter(F.col('rank') <= MAX_TARGETS_PER_REFERENCE) \
        .select(reference, target) \
        .persist(storageLevel)
    common_reference_num_df = query_content_df \
        .join(query_content_df.withColumnRenamed(target, f'{target}2'), reference) \
        .filter(F.col(target) < F.col(f'{target}2')) \
        .groupBy(target, f'{target}2').count() \
        .withColumnRenamed('count', 'common')
    reference_num_df = query_content_df \
        .groupBy(target).count() \
        .withColumnRenamed('count', f'{reference}_num') \
        .persist(storageLevel)
    target_popularity_df = query_content_count_df \
        .groupBy(target) \
        .agg(F.sum('count').alias('popularity')) \
        .persist(storageLevel)
    similarity_df = common_reference_num_df \
        .join(reference_num_df, target) \
        .join(reference_num_df
              .select(F.col(target).alias(f'{target}2'), F.col(f'{reference}_num').alias(f'{reference}_num2')),
              f'{target}2') \
        .join(target_popularity_df, target) \
        .join(target_popularity_df
              .select(F.col(target).alias(f'{target}2'), F.col('popularity').alias('popularity2')),
              f'{target}2') \
        .withColumn('jaccard', F.expr(f'common / ({reference}_num + {reference}_num2 - common)')) \
        .select(target, f'{target}2', 'jaccard', 'popularity', 'popularity2')
    query_content_df.unpersist()
    reference_num_df.unpersist()
    target_popularity_df.unpersist()
    return similarity_df


def get_q2q_by_content(query_content_count_df, content_similarity_df):
    c2c_df = content_similarity_df \
        .withColumn('rank', F.row_number().over(W.partitionBy('content_id').orderBy(F.desc('jaccard')))) \
        .filter(F.col('rank') <= TOP_SIMILAR_CONTENT) \
        .select('content_id', 'content_id2', 'jaccard')
    q2c_df = query_content_count_df \
        .withColumn('rank', F.row_number().over(W.partitionBy('query').orderBy(F.desc('count')))) \
        .filter(F.col('rank') <= TOP_CONTENT_PER_QUERY) \
        .select('query', 'content_id', F.col('count').alias('freq'))
    c2q_df = query_content_count_df \
        .withColumn('rank', F.row_number().over(W.partitionBy('content_id').orderBy(F.desc('count')))) \
        .filter(F.col('rank') <= TOP_QUERIES_PER_CONTENT) \
        .select(F.col('content_id').alias('content_id2'), F.col('query').alias('query2'), F.col('count').alias('freq2'))
    return q2c_df \
        .join(c2c_df, 'content_id') \
        .join(c2q_df, 'content_id2') \
        .withColumn('weight', F.expr('freq * jaccard * freq2')) \
        .groupBy('query', 'query2') \
        .agg(F.sum('weight').alias('weight')) \
        .filter(F.col('query').isNotNull() & F.col('query2').isNotNull() & ~(F.col('query') == F.col('query2')))


def get_complete_top_q2q_v2(q2q, rewrited_query_df, target):
    eval = 'jaccard' if target == 'query' else 'weight'
    filtered_q2q = q2q.join(rewrited_query_df.select(F.col('query').alias('query2'), 'better_query'), 'query2') \
        .drop('query2') \
        .withColumnRenamed("better_query", "query2") \
        .filter(~F.col('query2').isin(STOPWORDS + SP_STOPWORDS)) \
        .filter(is_sub_text_udf('query', 'query2'))
    w = W.partitionBy('query', 'query2')
    rm_filtered_q2q = filtered_q2q.withColumn('max'+eval, F.max(eval).over(w)) \
        .where(F.col(eval) == F.col('max'+eval)) \
        .drop('max'+eval).orderBy('query', F.desc(eval)).dropDuplicates(['query','query2'])
    rank_filtered_q2q = rm_filtered_q2q.withColumn('rank', F.row_number().over(W.partitionBy('query').orderBy(F.desc(eval)))) \
        .filter(F.col('rank') <= TOP_QUERIES_PER_QUERY) \
        .select('query', 'query2', eval, F.lit(target).alias('source'))
    if eval == 'jaccard':
        rank_filtered_q2q = rank_filtered_q2q.withColumnRenamed("jaccard", "weight")
    return rank_filtered_q2q

def filter_q2q(q2q_df, query_content_count_df):
    query_content_rank_df = query_content_count_df \
        .groupBy('query').count() \
        .filter(F.col('count') >= 2) \
        .select('query') \
        .join(query_content_count_df, 'query') \
        .withColumn('rank', F.row_number().over(W.partitionBy('query').orderBy(F.desc('count')))) \
        .persist(storageLevel)
    query_content1_count_df = query_content_rank_df \
        .filter(F.col('rank') == 1) \
        .select('query', 'content_id', 'count')
    query_content2_count_df = query_content_rank_df \
        .filter(F.col('rank') == 2) \
        .select('query', F.col('content_id').alias('content_id2'), F.col('count').alias('count2'))
    query_info_df = query_content1_count_df \
        .join(query_content2_count_df, 'query') \
        .select('query', 'content_id', F.expr('count2 / count').alias('ratio')) \
        .persist(storageLevel)
    # inner join is aggressive, since it removes all the queries out of query_info_df.
    q2q_filtered_df = q2q_df \
        .join(query_info_df, 'query') \
        .join(query_info_df.select(F.col('query').alias('query2'), F.col('content_id').alias('content_id2'),
                                   F.col('ratio').alias('ratio2')), 'query2') \
        .filter(~((F.col('content_id') == F.col('content_id2')) & (F.col('ratio') < RATIO_THRESHOLD) &
                  (F.col('ratio2') < RATIO_THRESHOLD))) \
        .select('query', 'query2', 'weight', 'source')
    query_content_rank_df.unpersist()
    query_info_df.unpersist()
    return q2q_filtered_df




def load_cms_metadata(spark, movie_table, tv_show_table, tv_season_table, episode_table, tournament_table,
                      sports_season_table, match_table, clip_table, channel_table):
    movie_df = load_hive_table(spark, movie_table) \
        .select(F.col('avscontentid').alias('content_id'), 'actors', 'anchors', 'directors',
                'cpdisplayname', 'description', F.lit(None).alias('name'), 'searchkeywords', 'shortsummary',
                F.lit(None).alias('shorttitle'), 'title', F.lit('movie').alias('source'))
    tv_show_df = load_hive_table(spark, tv_show_table) \
        .select(F.col('avscontentid').alias('content_id'), 'actors', F.lit(None).alias('anchors'), 'directors',
                F.lit(None).alias('cpdisplayname'), 'description', F.lit(None).alias('name'), 'searchkeywords',
                F.lit(None).alias('shortsummary'), 'shorttitle', 'title', F.lit('tv_show').alias('source'))
    tv_season_df = load_hive_table(spark, tv_season_table) \
        .select(F.col('avscontentid').alias('content_id'), F.lit(None).alias('actors'), F.lit(None).alias('anchors'),
                F.lit(None).alias('directors'), F.lit(None).alias('cpdisplayname'), F.lit(None).alias('description'),
                F.lit(None).alias('name'), 'searchkeywords', F.lit(None).alias('shortsummary'),
                F.lit(None).alias('shorttitle'), 'title', F.lit('tv_season').alias('source'))
    episode_df = load_hive_table(spark, episode_table) \
        .select(F.col('avscontentid').alias('content_id'), 'actors', 'anchors', 'directors',
                'cpdisplayname', 'description', F.lit(None).alias('name'), 'searchkeywords', 'shortsummary',
                F.lit(None).alias('shorttitle'), 'title', F.lit('episode').alias('source'))
    tournament_df = load_hive_table(spark, tournament_table) \
        .select(F.col('avscontentid').alias('content_id'), F.lit(None).alias('actors'), F.lit(None).alias('anchors'),
                F.lit(None).alias('directors'), F.lit(None).alias('cpdisplayname'), F.lit(None).alias('description'),
                'name', F.lit(None).alias('searchkeywords'), F.lit(None).alias('shortsummary'),
                F.lit(None).alias('shorttitle'), F.lit(None).alias('title'), F.lit('tournament').alias('source'))
    sports_season_df = load_hive_table(spark, sports_season_table) \
        .select(F.col('avscontentid').alias('content_id'), F.lit(None).alias('actors'), F.lit(None).alias('anchors'),
                F.lit(None).alias('directors'), F.lit(None).alias('cpdisplayname'), F.lit(None).alias('description'),
                'name', F.lit(None).alias('searchkeywords'), F.lit(None).alias('shortsummary'),
                F.lit(None).alias('shorttitle'), F.lit(None).alias('title'), F.lit('sports_season').alias('source'))
    match_df = load_hive_table(spark, match_table) \
        .select(F.col('avscontentid').alias('content_id'), F.lit(None).alias('actors'), F.lit(None).alias('anchors'),
                F.lit(None).alias('directors'), 'cpdisplayname', 'description', F.lit(None).alias('name'),
                'searchkeywords', 'shortsummary', F.lit(None).alias('shorttitle'), 'title',
                F.lit('match').alias('source'))
    clip_df = load_hive_table(spark, clip_table) \
        .select(F.col('avscontentid').alias('content_id'), 'actors', 'anchors', 'directors', 'cpdisplayname',
                'description', F.lit(None).alias('name'), 'searchkeywords', 'shortsummary',
                F.col('secondarytitle').alias('shorttitle'), 'title', F.lit('clip').alias('source'))
    channel_df = load_hive_table(spark, channel_table) \
        .select(F.col('avscontentid').alias('content_id'), F.lit(None).alias('actors'), F.lit(None).alias('anchors'),
                F.lit(None).alias('directors'), F.lit(None).alias('cpdisplayname'), 'description', 'name',
                'searchkeywords', F.lit(None).alias('shortsummary'), F.lit(None).alias('shorttitle'), 'title',
                F.lit('channel').alias('source'))
    return movie_df.union(tv_show_df).union(tv_season_df).union(episode_df).union(tournament_df).union(
        sports_season_df).union(match_df).union(clip_df).union(channel_df)



def clean_meta_corpus(meta_corpus_df):
    return meta_corpus_df.filter(~(meta_corpus_df.source.isin(REMOVE_META_COL))) \
        .groupBy('text').agg(F.sum('count').alias('count')) \
        .filter(F.length('text') > 2).filter( F.length('text') < 50)



def rewrite_query_v2(query_meta_df, query_count_df, content2entity_df):
    query_content_rank_df = query_meta_df \
        .groupBy('query').count() \
        .filter(F.col('count') >= 2) \
        .select('query') \
        .join(query_meta_df, 'query') \
        .withColumn('rank', F.row_number().over(W.partitionBy('query').orderBy(F.desc('count')))) \
        .persist(storageLevel)
    query_content1_count_df = query_content_rank_df \
        .filter(F.col('rank') == 1) \
        .select('query', 'content_id', 'count')
    query_content2_count_df = query_content_rank_df \
        .filter(F.col('rank') == 2) \
        .select('query', F.col('content_id').alias('content_id2'), F.col('count').alias('count2'))
    query_info_df = query_content1_count_df \
        .join(query_content2_count_df, 'query') \
        .select('query', 'content_id', F.expr('count2 / count').alias('ratio'),F.col('count').alias('sub_count')) \
        .persist(storageLevel)
    filtered_query = query_info_df.filter(F.col('ratio')<0.5).join(query_count_df,'query')\
        .withColumn('prop', F.expr('sub_count/count')).filter(F.col('prop')>0.1).orderBy('content_id')
    rewrited_query_df = filtered_query.join(content2entity_df, 'content_id').select('query', 'better_query')
    query_content_rank_df.unpersist()
    query_info_df.unpersist()
    return rewrited_query_df

def gen_len_coef(spark):
    x = list(range(3, 51))
    y = [0.05] + [float(math.log((i - 1) / 2) ** 2) for i in range(4, 51)]
    temp = [[a, b] for (a, b) in zip(x, y)]
    len_coef_df = spark.createDataFrame(temp, schema=['length', 'coef'])
    return len_coef_df

def get_content2words(query_content_count):
    query_score_df = query_content_count.withColumn('length', F.length('query')).join(len_coef_df, 'length').withColumn(
        'score', F.expr('count*coef'))
    w = W.partitionBy('content_id')
    content2words_df = query_score_df.withColumn('maxscore', F.max('score').over(w)) \
        .where(F.col('score') == F.col('maxscore')) \
        .drop('maxscore').select(F.col('query').alias('better_query'), 'content_id')
    return content2words_df

from path import *
from utils import *
import os
# spark = SparkSession.builder.getOrCreate()
spark.stop()
spark = hive_spark('statistics')

date = (datetime.date.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")  # yesterday
days = -90

