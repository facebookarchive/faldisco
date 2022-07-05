#!/usr/bin/env python3
# pyre-strict


# Copyright (c) Meta Platforms, Inc. and affiliates.

# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging

logger = logging.getLogger(__name__)


class Field_Combinations:
    combinations: {}
    ref_fields: {str}  # list of ref_fields with counts of target fields
    target_fields: {str}  # list of target fields with counts of ref fields
    name: str

    def __init__(self, name: str):
        self.name = name
        self.combinations = {}
        self.ref_fields = {}
        self.target_fields = {}
        return

    def increment_combination(
            self, ref_field_name: str, target_field_name: str, val: float
    ):
        tfs = self.get_target_fields(ref_field_name)
        if target_field_name not in tfs.keys():
            # new combination - increment counts
            self.increment_ref_field_count(ref_field_name)
            self.increment_target_field_count(target_field_name)
            tfs[target_field_name] = val
        else:
            tfs[target_field_name] += val

    def get_combination(self, ref_field_name: str, target_field_name: str):
        tfs = self.get_target_fields(ref_field_name)
        if target_field_name not in tfs.keys():
            tfs[target_field_name] = 0.0
        return tfs[target_field_name]

    def adjust_field_count(self, field_counts: {}, field_name: str, adjustment: int):
        if field_name not in field_counts.keys():
            field_counts[field_name] = 0
        field_counts[field_name] += adjustment

    def increment_ref_field_count(self, ref_field_name: str):
        self.adjust_field_count(self.ref_fields, ref_field_name, 1)

    def decrement_ref_field_count(self, ref_field_name: str):
        self.adjust_field_count(self.ref_fields, ref_field_name, -1)

    def increment_target_field_count(self, target_field_name: str):
        self.adjust_field_count(self.target_fields, target_field_name, 1)

    def decrement_target_field_count(self, target_field_name: str):
        self.adjust_field_count(self.target_fields, target_field_name, -1)

    def set_combination(
            self, ref_field_name: str, target_field_name: str, value: float
    ):
        tfs = self.get_target_fields(ref_field_name)
        if target_field_name not in tfs.keys():
            # new combination - increment counts
            self.increment_ref_field_count(ref_field_name)
            self.increment_target_field_count(target_field_name)
        tfs[target_field_name] = value

    def remove_combination(self, ref_field_name: str, target_field_name: str):
        if ref_field_name in self.combinations.keys():
            tfs = self.get_target_fields(ref_field_name)
            if target_field_name in tfs.keys():
                tfs.pop(target_field_name)
                self.decrement_target_field_count(target_field_name)
                self.decrement_ref_field_count(ref_field_name)

    def check_combination(self, ref_field_name: str, target_field_name: str) -> bool:
        if ref_field_name in self.combinations.keys():
            if target_field_name in self.combinations[ref_field_name].keys():
                return True
        return False

    def get_target_field_names(self, ref_field_name: str) -> {}:
        target_fields = self.get_target_fields(ref_field_name)
        return target_fields.keys()

    def get_target_fields(self, ref_field_name: str) -> {}:
        if ref_field_name not in self.combinations.keys():
            self.combinations[ref_field_name] = {}
        return self.combinations[ref_field_name]

    def get_ref_field_names(self) -> {}:
        return self.combinations.keys()

    def add_combination(self, ref_field_name: str, target_field_name: str):
        self.set_combination(ref_field_name, target_field_name, 0.0)

    def num_combinations(self) -> int:
        cc = 0
        for r in self.combinations:
            # add up number of target fields for each reference field
            cc += self.ref_fields[r]
        return cc

    def log_combinations(self):
        ref_field_names = self.get_ref_field_names()
        for r in ref_field_names:
            target_field_names = self.get_target_field_names(r)
            for t in target_field_names:
                val = self.get_combination(r, t)
                logger.info(f"FALDISCO__DEBUG: logging {self.name}: {r}, {t} = {val}")
