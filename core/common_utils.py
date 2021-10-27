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

