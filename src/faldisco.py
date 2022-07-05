#!/usr/bin/env python3
# pyre-strict

# Copyright (c) Meta Platforms, Inc. and affiliates.

# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
import os
import sys
from typing import List, Set
import faldisco_globals as fg

from sqlalchemy import MetaData, create_engine, BigInteger, Float, SmallInteger
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy.engine import Engine
from sqlalchemy.sql.type_api import TypeEngine

from faldisco_utils import FaldiscoUtils

logger = logging.getLogger(__name__)

SUPPORTED_TYPES: Set[TypeEngine] = {
    String,
    BigInteger,
    Float,
    Integer,
    SmallInteger,
}

IGNORED_NAMES: Set[str] = {"ds", "shard"}
DB_URL: str = "mysql+pymysql://root:root@localhost/mysql"


def main() -> None:
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    args = sys.argv[1:]
    if not (len(args) == 3 or len(args) == 4):
        print_usage_and_exit()

    ref = args[0]
    target = args[1]
    if not ("." in ref and "." in target):
        print_usage_and_exit()

    ref_schema_name = ref.split(".")[0]
    ref_table_name = ref.split(".")[1]
    target_schema_name = target.split(".")[0]
    target_table_name = target.split(".")[1]

    ref_join_keys: List[str] = [k.strip() for k in args[2].split(",")]
    target_join_keys = ref_join_keys if len(args) == 3 else [k.strip() for k in args[3].split(",")]
    logger.info(f"The tables are {ref_schema_name}.{ref_table_name} and {target_schema_name}.{target_table_name}")
    engine: Engine = create_engine(DB_URL)
    logger.info(f"{engine} {type(engine)}")
    metadata_obj: MetaData = MetaData(bind=DB_URL)
    metadata_obj.reflect()
    logger.info(metadata_obj.is_bound())
    ref_table: Table = metadata_obj.tables[ref_table_name]
    target_table: Table = metadata_obj.tables[target_table_name]
    if ref_table is None or target_table is None:
        print(f"Either the source {ref} or target {target} tables don't exist")
        sys.exit(-1)

    logger.info(f"Usable columns {ref_table.c}")
    logger.info(f"Usable target columns {target_table.c}")
    logger.info(
        f"Ref join keys {ref_join_keys} Target_join_keys {target_join_keys}"
    )
    try:
        os.mkdir(fg.FALDISCO_OUTPUT_FOLDER)
    except FileExistsError:
        pass

    FaldiscoUtils.find_alignment(
        engine=engine,
        ref_schema_name=ref_schema_name,
        ref_table_name=ref_table_name,
        ref_table_fields=ref_table.c,
        ref_join_keys=ref_join_keys,
        target_schema_name=target_schema_name,
        target_table_name=target_table_name,
        target_table_fields=target_table.c,
        target_join_keys=[],
    )


def print_usage_and_exit() -> None:
    print(
        "Usage: python faldisco.py <ref ns.ref table> <target ns.target table> <ref_join_keys> ["
        "target_join_keys] "
    )
    sys.exit(-1)


def supported(col: Column) -> bool:
    return (
            col.type in SUPPORTED_TYPES
            and col.name not in IGNORED_NAMES
            and "json" not in col.name
    )


if __name__ == "__main__":
    main()
