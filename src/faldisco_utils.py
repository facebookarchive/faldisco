#!/usr/bin/env python3
# pyre-strict


# Copyright (c) Meta Platforms, Inc. and affiliates.

# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
from typing import List

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy.sql import ColumnCollection

import faldisco_globals as fg
from field_alignment import (
    Field_Alignment,
)

logger = logging.getLogger(__name__)


class FaldiscoUtils:
    def __init__(self):
        return

    # combine reference table and target table on alignment key
    # rename all ref fields r__ field name
    # rename all target fields t__ field name
    # to avoid name collisions
    @staticmethod
    def gen_sql(
            ref_schema_name: str,
            ref_table_name: str,
            ref_table_fields: ColumnCollection,
            ref_join_key: str,
            target_schema_name: str,
            target_table_name: str,
            target_table_fields: ColumnCollection,
            target_join_key: str,
            ds_date: str,
    ) -> str:
        sql_statement = f"select r.{ref_join_key} as r__{ref_join_key} "
        for c in ref_table_fields.keys():
            sql_statement = sql_statement + f", r.{c} as r__{c}"
        for c in target_table_fields.keys():
            sql_statement = sql_statement + f", t.{c} as t__{c}"
        sql_statement = (
                sql_statement
                + f" from {ref_schema_name}.{ref_table_name} r join "
                + f"{target_schema_name}.{target_table_name}: t on r.{ref_join_key} = t.{target_join_key}"
                + f" where r.ds = '{ds_date}' and t.ds = '{ds_date}' "
                + f" LIMIT = {fg.SAMPLE_SIZE}"
        )
        return sql_statement

    @staticmethod
    def find_alignment(
            engine: Engine,
            ref_schema_name: str,
            ref_table_name: str,
            ref_table_fields: ColumnCollection,
            ref_join_keys: List[str],
            target_schema_name: str,
            target_table_name: str,
            target_table_fields: ColumnCollection,
            target_join_keys: List[str],
    ):
        fa = Field_Alignment(
            ref_schema_name,
            ref_table_name,
            target_schema_name,
            target_table_name,
            ref_join_keys,
            ref_table_fields.keys(),
            target_table_fields.keys(),
        )

        query = fa.gen_sql()
        logger.setLevel(logging.INFO)
        logger.info("FALDISCO__DEBUG: query={query}")
        with engine.connect() as connection:
            qresults_df = pd.read_sql(sql=query, con=connection)
        logger.info(f"FALDISCO__DEBUG: {qresults_df} result size {qresults_df.shape}")
        fa.df = qresults_df
        num_alignments = fa.find_field_alignment()
        logger.info("FALDISCO__DEBUG: Number of alignments=" + str(num_alignments))

        # write out profiles
        profiles_df = fa.profiles_to_df(fg.FIELD_PROFILES_TABLE_FIELDS)
        # load results
        profiles_df.to_csv(path_or_buf=f"{fg.FALDISCO_OUTPUT_FOLDER}{fg.FALDISCO_OUTPUT_FOLDER}{ref_table_name}_to_{target_table_name}_profiles")
        results_df = fa.results_df
        for _index, row in results_df.iterrows():
            t = f"t__{row['target_field_name']}"
            r = f"r__{row['reference_field_name']}"
            if (
                    (r in fg.TRACE_FIELDS_ANY)
                    or (t in fg.TRACE_FIELDS_ANY)
                    or (r in fg.TRACE_FIELDS_ALL and t in fg.TRACE_FIELDS_ALL)
            ):
                logger.info(
                    f"FALDISCO__DEBUG: RESULTS: {row['reference_field_name']}, {row['target_field_name']}, alignment type={row['alignment_type']}, strength={row['alignment_strength']}"
                )
        # load results
        results_df.to_csv(path_or_buf=f"{fg.FALDISCO_OUTPUT_FOLDER}{fg.FALDISCO_OUTPUT_FOLDER}{ref_table_name}_to_{target_table_name}_field_alignments")
        alignment_values_df = fa.alignment_values_df
        for _index, row in alignment_values_df.iterrows():
            t = f"t__{row['target_field_name']}"
            r = f"r__{row['reference_field_name']}"
            if (
                    (r in fg.TRACE_FIELDS_ANY)
                    or (t in fg.TRACE_FIELDS_ANY)
                    or (r in fg.TRACE_FIELDS_ALL and t in fg.TRACE_FIELDS_ALL)
            ):
                logger.info(
                    f"FALDISCO__DEBUG: RESULTS: {row['reference_field_name']}={row['reference_field_value']}, {row['target_field_name']}={row['target_field_value']}, {row['alignment_type']}, alignment={row['alignment_count']}, misalignment={row['misalignment_count']}"
                )

        if len(alignment_values_df) == 0:
            logger.info("FALDISCO__DEBUG: no value alignments found")
        else:
            # load results
            alignment_values_df.to_csv(path_or_buf=f"{fg.FALDISCO_OUTPUT_FOLDER}{fg.FALDISCO_OUTPUT_FOLDER}{ref_table_name}_to_{target_table_name}_value_alignments")

