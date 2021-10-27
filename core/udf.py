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



