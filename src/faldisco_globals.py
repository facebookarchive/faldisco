#!/usr/bin/env python3
# pyre-strict


# Copyright (c) Meta Platforms, Inc. and affiliates.

# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# sample size
SAMPLE_SIZE = 2000
KEY_MIN_VALUE_COUNT = 1
KEY_MAX_VALUE_COUNT = 1

# 1-selectivity threshold for constant fields
CONSTANT_VALUE_THRESHOLD = 0.99
SPARSE_VALUE_THRESHOLD = 0.95
# selectivity threshold for unique fields
UNIQUE_SELECTIVIY_THRESHOLD = 0.8

FIELD_EXACT_MATCH_THRESHOLD = (
    0.4  # what percentage of values should align for us to keep processing
)
FIELD_ROW_ALIGNMENT_THRESHOLD = (
    0.4  # what percentage of rows should align for us to keep processing
)

FIELD_VALUE_ALIGNMENT_THRESHOLD = (
    0.6  # what percentage of values should align for us to keep processing
)

FIELD_SPARSE_NON_MFV_ALIGNMENT_THRESHOLD = 0.9  # what percentage of non most frequent values are in the same rows in
# ref and target

FIELD_ALIGNMENT_TABLE_FIELDS = [
    "reference_table_namespace",
    "reference_table_name",
    "reference_field_name",
    "target_table_namespace",
    "target_table_name",
    "target_field_name",
    "alignment_type",
    "alignment_strength",
]
FIELD_VALUE_ALIGNMENT_TABLE_FIELDS = [
    "reference_table_namespace",
    "reference_table_name",
    "reference_field_name",
    "target_table_namespace",
    "target_table_name",
    "target_field_name",
    "reference_field_value",
    "target_field_value",
    "alignment_type",
    "alignment_count",
    "misalignment_count",
]
FIELD_VALUE_ALIGNMENT_TABLE_FIELD_TYPES = {
    "reference_table_namespace": str,
    "reference_table_name": str,
    "reference_field_name": str,
    "target_table_namespace": str,
    "target_table_name": str,
    "target_field_name": str,
    "reference_field_value": str,
    "target_field_value": str,
    "alignment_type": str,
    "alignment_count": int,
    "misalignment_count": int,
}
ALIGNMENT_TYPE_EXACT_MATCH = "exact match"
ALIGNMENT_TYPE_ALIGNMENT = "alignment"
ALIGNMENT_TYPE_SPARSE_EXACT_MATCH = "sparse exact match"
ALIGNMENT_TYPE_SPARSE_ALIGNMENT = "sparse alignment"
ALIGNMENT_TYPE_SPARSE_NON_MFV_ALIGNMENT = "sparse non-mvf alignment"

ALIGNMENT_SELECTIVITY_RATIO_THRESHOLD = 0.0

FIELD_PROFILES_TABLE_FIELDS = [
    "table_namespace",
    "table_name",
    "field_name",
    "cardinality",
    "selectivity",
    "min_value",
    "max_value",
    "min_len",
    "max_len",
    "mfv_count",
    "num_rows",
    "is_unique",
    "is_sparse",
    "is_constant"
]
FALDISCO_SAVE_PROFILES = True
FALDISCO_SAVE_ALIGNMENT_VALUES = True

TRACE_FIELDS_ANY = []
TRACE_FIELDS_ALL = []

TRACE_RECORDS_FOR_FIELDS_ANY = []
TRACE_RECORDS_FOR_FIELDS_ALL = []

FALDISCO_SPECIAL_VALUE_PREFIX = "FALDISCO_"
FALDISCO_SPECIAL_VALUE_PREFIX_LEN = len(FALDISCO_SPECIAL_VALUE_PREFIX)
FALDISCO_NAN = FALDISCO_SPECIAL_VALUE_PREFIX + "NAN"
FALDISCO_NULL = FALDISCO_SPECIAL_VALUE_PREFIX + "NULL"
FALDISCO_EMPTY = FALDISCO_SPECIAL_VALUE_PREFIX + "EMPTY"
FALDISCO_OUTPUT_FOLDER = "../out/"




def is_special_value(val: str) -> bool:
    return val[0:FALDISCO_SPECIAL_VALUE_PREFIX_LEN] == FALDISCO_SPECIAL_VALUE_PREFIX


def make_orig_field_name(f: str) -> str:
    # strip prefix - either r__ or t__
    return f[3:]
