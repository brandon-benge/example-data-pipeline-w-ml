from __future__ import annotations

import os

from pyspark.sql import Column
from pyspark.sql import functions as F


TOKENIZATION_SALT = os.getenv("TOKENIZATION_SALT", "local-demo-tokenization-salt")


def mask_name(column: Column) -> Column:
    return F.when(column.isNull(), F.lit(None)).otherwise(F.concat(F.substring(column, 1, 1), F.lit("***")))


def mask_email(column: Column) -> Column:
    local_part = F.substring_index(column, "@", 1)
    domain = F.substring_index(column, "@", -1)
    return F.when(column.isNull(), F.lit(None)).otherwise(F.concat(F.substring(local_part, 1, 1), F.lit("***@"), domain))


def mask_phone(column: Column) -> Column:
    return F.when(column.isNull(), F.lit(None)).otherwise(F.concat(F.lit("***-***-"), F.substring(column, -4, 4)))


def mask_zip_code(column: Column) -> Column:
    return F.when(column.isNull(), F.lit(None)).otherwise(F.concat(F.substring(column, 1, 2), F.lit("***")))


def tokenized_column(column: Column) -> Column:
    return F.sha2(F.concat_ws("||", F.lit(TOKENIZATION_SALT), column), 256)
