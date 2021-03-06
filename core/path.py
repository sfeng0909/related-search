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