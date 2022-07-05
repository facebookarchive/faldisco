#!/usr/bin/env python3
# pyre-strict


# Copyright (c) Meta Platforms, Inc. and affiliates.

# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging

import faldisco_globals as fg

logger = logging.getLogger(__name__)


class Field_Profiles:
    cardinality: int
    selectivity: float
    mfv_count: int
    num_rows: int
    min_len: int
    max_len: int
    min_val: str
    max_val: str
    mfv: str

    def __init__(
            self,
            num_rows: int,
            cardinality: int,
            selectivity: float,
            mfv_count: int,
            min_len: int,
            max_len: int,
            min_val: str,
            max_val: str,
            mfv: str,
    ):
        self.num_rows = num_rows
        self.cardinality = cardinality
        self.selectivity = selectivity
        self.mfv_count = mfv_count
        self.num_rows = num_rows
        self.min_len = min_len
        self.max_len = max_len
        self.min_val = min_val
        self.max_val = max_val
        self.mfv = mfv

    def set_num_rows(self, num_rows: int):
        self.num_rows = num_rows

    def set_field_cardinality(self, cardinality):
        self.cardinality = cardinality

    def set_field_selectivity(self, selectivity):
        self.selectivity = selectivity

    def set_field_mfv_count(self, mfv_count):
        self.mfv_count = mfv_count

    def set_field_min_len(self, min_len):
        self.min_len = min_len

    def set_field_max_len(self, max_len):
        self.max_len = max_len

    def set_field_min_val(self, min_val):
        self.min_val = min_val

    def set_field_max_val(self, max_val):
        self.max_val = max_val

    def set_mfv(self, mfv):
        self.mfv = mfv

    def get_field_cardinality(self):
        return self.cardinality

    def get_field_selectivity(self):
        return self.selectivity

    def get_field_mfv_count(self):
        return self.mfv_count

    def get_num_rows(self):
        return self.num_rows

    def get_field_min_len(self) -> int:
        return self.min_len

    def get_field_max_len(self) -> int:
        return self.max_len

    def get_field_min_val(self) -> str:
        return self.min_val

    def get_field_max_val(self) -> str:
        return self.max_val

    def get_field_mfv(self) -> str:
        return self.mfv

    def is_constant_field(self):
        # if the field only has one value - easy, it is constant
        if self.get_field_cardinality() <= 1:
            return True
        # if the field has one value that takes up most rows > CONSTANT_VALUE_THRESHOLD, it is constant
        elif (
                self.get_field_mfv_count() / self.get_num_rows()
                > fg.CONSTANT_VALUE_THRESHOLD
        ):
            return True
        else:
            return False

    def is_sparse_field(self):
        # if the field only has one value - easy, it is constant
        if self.is_constant_field():
            return False
        # if the field has one value that takes up most rows > SPARSE_VALUE_THRESHOLD, it is constant
        elif (
                self.get_field_mfv_count() / self.get_num_rows() > fg.SPARSE_VALUE_THRESHOLD
        ):
            return True
        else:
            return False

    def is_unique_field(self):
        # if the field has selectivity of 1 - it is unique
        if self.get_field_selectivity() > fg.UNIQUE_SELECTIVIY_THRESHOLD:
            return True
        # if we take out the most frequent value and the field becomes unique, consider it unique
        elif (self.get_num_rows() - self.get_field_mfv_count()) > 0 and (
                self.get_field_cardinality() - 1
        ) / (
                self.get_num_rows() - self.get_field_mfv_count()
        ) > fg.UNIQUE_SELECTIVIY_THRESHOLD:
            return True
        return False

    def __str__(self) -> str:
        return ("field_profile:" +
                f"num_rows:{self.num_rows}, cardinality: {self.cardinality}, selectivity:{self.selectivity}, min_len:{self.min_len}" +
                f"max_len:{self.max_len}, min_val:{self.min_val}, max_val:{self.max_val}")
