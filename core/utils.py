import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
import math
from udf import *
from common_utils import *
from constant import *


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

def get_content2words(query_content_count, len_coef_df):
    query_score_df = query_content_count.withColumn('length', F.length('query')).join(len_coef_df, 'length').withColumn(
        'score', F.expr('count*coef'))
    w = W.partitionBy('content_id')
    content2words_df = query_score_df.withColumn('maxscore', F.max('score').over(w)) \
        .where(F.col('score') == F.col('maxscore')) \
        .drop('maxscore').select(F.col('query').alias('better_query'), 'content_id')
    return content2words_df


