#!/usr/bin/env python3
# pyre-strict


# Copyright (c) Meta Platforms, Inc. and affiliates.

# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging

from pandas import DataFrame

import faldisco_globals as fg
from field_profiles import (
    Field_Profiles,
)
from value_matches import Value_Matches

logger = logging.getLogger(__name__)


class Faldisco_Results:
    results_df: DataFrame
    potential_matches: {}
    field_profiles: {Field_Profiles}
    ref_table_namespace: str
    ref_table_name: str
    target_table_namespace: str
    target_table_name: str
    value_matches: Value_Matches
    value_matches_df: DataFrame
    value_matches_row_num: int

    def __init__(
            self,
            field_profiles: {Field_Profiles},
            ref_table_namespace: str,
            ref_table_name: str,
            target_table_namespace: str,
            target_table_name: str,
            value_matches: Value_Matches,
            value_matches_df: DataFrame,
    ):
        self.potential_matches = {}
        self.field_profiles = field_profiles
        self.results_df = DataFrame(columns=fg.FIELD_ALIGNMENT_TABLE_FIELDS)
        self.target_table_namespace = target_table_namespace
        self.target_table_name = target_table_name
        self.ref_table_namespace = ref_table_namespace
        self.ref_table_name = ref_table_name
        self.value_matches = value_matches
        self.value_matches_df = value_matches_df
        self.value_matches_row_num = 0
        return

    def dedup_results(self) -> DataFrame:
        row_num = 0
        for t in self.potential_matches.keys():
            fp = self.field_profiles[t]
            if fp.is_sparse_field():
                row_num = self.dedup_sparse_field(row_num, t)
            else:
                row_num = self.dedup_field(row_num, t)
        return self.results_df

    def dedup_field(self, row_num: int, target_field_name: str):
        # matches are a list of alignment type and alignment strength
        max_exact_match_strength = 0
        max_alignment_strength = 0
        min_selectivity = 1
        max_strength_selectivity_ratio = 0
        top_exact_matches = []
        top_alignments = []
        other_alignments = []
        fp: Field_Profiles
        matches = self.get_matches(target_field_name)
        for r in matches.keys():
            al = self.get_alignments(target_field_name, r)
            for alignment_type in al.keys():
                alignment_strength = self.get_alignment_strength(
                    target_field_name, r, alignment_type
                )
                if alignment_type == fg.ALIGNMENT_TYPE_EXACT_MATCH:
                    if alignment_strength > max_exact_match_strength:
                        max_exact_match_strength = alignment_strength
                        top_exact_matches = [r]
                    elif alignment_strength == max_exact_match_strength:
                        top_exact_matches.append(r)
                elif alignment_strength > max_exact_match_strength:
                    # we have alignments stronger than max exact matches
                    selectivity = self.get_selectivity(r)
                    strength_selectivity_ratio = alignment_strength / selectivity
                    if strength_selectivity_ratio >= max_strength_selectivity_ratio:
                        # if we have better strength selectivity ratio, add the alignment to the list - it may also be
                        # added to top_alignments, but we will filter that later. On the other hand, if we do not add
                        # it now, it may be bumped from top_alignments and we will lose it
                        max_strength_selectivity_ratio = strength_selectivity_ratio
                        other_alignments.append(r)
                    if alignment_strength > max_alignment_strength or (
                            alignment_strength == max_alignment_strength
                            and selectivity < min_selectivity
                    ):
                        # we have a new champion
                        max_alignment_strength = alignment_strength
                        top_alignments = [r]
                        min_selectivity = selectivity
                    elif (
                            alignment_strength == max_alignment_strength
                            and min_selectivity == selectivity
                    ):
                        top_alignments.append(r)

        row_num = self.add_field_results(
            row_num,
            target_field_name,
            max_exact_match_strength,
            max_alignment_strength,
            min_selectivity,
            max_strength_selectivity_ratio,
            top_exact_matches,
            top_alignments,
            other_alignments,
        )
        return row_num

    def dedup_sparse_field(self, row_num: int, target_field_name: str):
        # matches are a list of alignment type and alignment strength
        max_exact_match_strength = 0
        max_alignment_strength = 0
        max_sparse_non_mfv_alignment_strength = 0
        min_selectivity = 1
        max_strength_selectivity_ratio = 0
        top_exact_matches = []
        top_alignments = []
        other_alignments = []
        top_non_mfv_alignments = []
        fp: Field_Profiles
        matches = self.get_matches(target_field_name)
        for r in matches.keys():
            al = self.get_alignments(target_field_name, r)
            for alignment_type in al.keys():
                alignment_strength = self.get_alignment_strength(
                    target_field_name, r, alignment_type
                )
                if alignment_type == fg.ALIGNMENT_TYPE_SPARSE_EXACT_MATCH:
                    if alignment_strength > max_exact_match_strength:
                        max_exact_match_strength = alignment_strength
                        top_exact_matches = [r]
                    elif alignment_strength == max_exact_match_strength:
                        top_exact_matches.append(r)
                elif (
                        alignment_type == fg.ALIGNMENT_TYPE_SPARSE_ALIGNMENT
                        and alignment_strength > max_exact_match_strength
                ):
                    # we have alignments stronger than max exact matches
                    if alignment_strength > max_alignment_strength:
                        # we have a new champion
                        max_alignment_strength = alignment_strength
                        top_alignments = [r]
                    elif alignment_strength > max_sparse_non_mfv_alignment_strength:
                        top_alignments.append(r)
                elif (
                        alignment_type == fg.ALIGNMENT_TYPE_SPARSE_NON_MFV_ALIGNMENT
                        and alignment_strength > max_alignment_strength
                ):
                    # we have non-mfv alignments stronger than max alignment
                    if alignment_strength > max_sparse_non_mfv_alignment_strength:
                        # we have a new champion
                        max_sparse_non_mfv_alignment_strength = alignment_strength
                        top_non_mfv_alignments = [r]
                    elif alignment_strength > max_sparse_non_mfv_alignment_strength:
                        top_non_mfv_alignments.append(r)

        row_num = self.add_sparse_field_results(
            row_num,
            target_field_name,
            max_exact_match_strength,
            max_alignment_strength,
            min_selectivity,
            max_strength_selectivity_ratio,
            max_sparse_non_mfv_alignment_strength,
            top_exact_matches,
            top_alignments,
            other_alignments,
            top_non_mfv_alignments,
        )
        return row_num

    def add_sparse_field_results(
            self,
            row_num: int,
            target_field_name: str,
            max_exact_match_strength: float,
            max_alignment_strength: float,
            min_selectivity: float,
            max_strength_selectivity_ratio: float,
            max_sparse_non_mfv_alignment_strength: float,
            top_exact_matches: [str],
            top_alignments: [str],
            other_alignments: [str],
            top_non_mfv_alignments: [str],
    ) -> int:

        # first see if we should add non mfv alignment to the results
        if (
                max_sparse_non_mfv_alignment_strength > max_exact_match_strength
                and max_sparse_non_mfv_alignment_strength > max_alignment_strength
                and max_sparse_non_mfv_alignment_strength > 0
        ):
            row_num = self.add_matches_to_result(
                row_num,
                target_field_name,
                top_non_mfv_alignments,
                fg.ALIGNMENT_TYPE_SPARSE_NON_MFV_ALIGNMENT,
                max_sparse_non_mfv_alignment_strength,
            )
        if max_exact_match_strength >= 0:
            # add top exact matches to the results
            row_num = self.add_matches_to_result(
                row_num,
                target_field_name,
                top_exact_matches,
                fg.ALIGNMENT_TYPE_SPARSE_EXACT_MATCH,
                max_exact_match_strength,
            )
        if max_exact_match_strength < max_alignment_strength:
            # let's add alignments
            # first do the max strength ones with minimum selectivity
            row_num = self.add_matches_to_result(
                row_num,
                target_field_name,
                top_alignments,
                fg.ALIGNMENT_TYPE_SPARSE_ALIGNMENT,
                max_alignment_strength,
            )
            # now it gets tricky, we want to find other alignments where ratio of alignment strength and selectivity vs.
            # the match with highest alignment strength and minimum selectivity is > ALIGNMENT_SELECTIVITY_RATIO_THRESHOLD
            # filter out other_alignments that are either in exact_matches or alignments
            other_alignments = Faldisco_Results.filter_out_dups(
                other_alignments, top_alignments + top_exact_matches
            )
            row_num = self.process_other_alignments(
                row_num,
                other_alignments,
                max_alignment_strength,
                min_selectivity,
                max_exact_match_strength,
                max_strength_selectivity_ratio,
                target_field_name,
                fg.ALIGNMENT_TYPE_SPARSE_ALIGNMENT,
            )

        return row_num

    def add_field_results(
            self,
            row_num: int,
            target_field_name: str,
            max_exact_match_strength: float,
            max_alignment_strength: float,
            min_selectivity: float,
            max_strength_selectivity_ratio: float,
            top_exact_matches: [str],
            top_alignments: [str],
            other_alignments: [str],
    ) -> int:

        if max_exact_match_strength >= 0:
            # add top exact matches to the results
            row_num = self.add_matches_to_result(
                row_num,
                target_field_name,
                top_exact_matches,
                fg.ALIGNMENT_TYPE_EXACT_MATCH,
                max_exact_match_strength,
            )
        if max_exact_match_strength < max_alignment_strength:
            # let's add alignments
            # first do the max strength ones with minimum selectivity
            row_num = self.add_matches_to_result(
                row_num,
                target_field_name,
                top_alignments,
                fg.ALIGNMENT_TYPE_ALIGNMENT,
                max_alignment_strength,
            )
            # now it gets tricky, we want to find other alignments where ratio of alignment strength and selectivity vs.
            # the match with highest alignment strength and minimum selectivity is > ALIGNMENT_SELECTIVITY_RATIO_THRESHOLD
            # filter out other_alignments that are either in exact_matches or alignments
            other_alignments = Faldisco_Results.filter_out_dups(
                other_alignments, top_alignments + top_exact_matches
            )
            row_num = self.process_other_alignments(
                row_num,
                other_alignments,
                max_alignment_strength,
                min_selectivity,
                max_exact_match_strength,
                max_strength_selectivity_ratio,
                target_field_name,
                fg.ALIGNMENT_TYPE_ALIGNMENT,
            )

        return row_num

    @staticmethod
    def filter_out_dups(list1: [str], list2: [str]) -> [str]:
        # remove any items in list2 from list1
        deduped_list = []
        for i in list1:
            if i not in list2 + deduped_list:
                deduped_list.append(i)
        return deduped_list

    def get_selectivity(self, f: str) -> float:
        fp: Field_Profiles
        if self.field_profiles is not None and f in self.field_profiles.keys():
            fp = self.field_profiles[f]
            return fp.get_field_selectivity()
        else:
            return -1

    def add_match_to_result(
            self,
            row_num: int,
            ref_field_name: str,
            target_field_name: str,
            alignment_type: str,
            alignment_strength: float,
    ):
        orig_ref_field_name = fg.make_orig_field_name(ref_field_name)
        orig_target_field_name = fg.make_orig_field_name(target_field_name)

        self.results_df.loc[row_num] = [
            self.ref_table_namespace,
            self.ref_table_name,
            orig_ref_field_name,
            self.target_table_namespace,
            self.target_table_name,
            orig_target_field_name,
            alignment_type,
            alignment_strength,
        ]
        row_num += 1
        if (
                f"r__{orig_ref_field_name}" in fg.TRACE_RECORDS_FOR_FIELDS_ANY
                or f"t__{orig_target_field_name}" in fg.TRACE_RECORDS_FOR_FIELDS_ANY
                or (
                f"r__{orig_ref_field_name}" in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                and f"t__{orig_target_field_name}" in fg.TRACE_RECORDS_FOR_FIELDS_ALL
        )
        ):
            logger.info(
                f"FALDISCO__DEBUG: Adding {alignment_type} at row {row_num} between {ref_field_name} and {target_field_name} = {alignment_strength} to results"
            )

        # if an alignment, add value matches
        if alignment_type == fg.ALIGNMENT_TYPE_ALIGNMENT:
            self.value_matches_row_num = self.value_matches.add_alignment_values_to_df(
                self.ref_table_namespace,
                self.ref_table_name,
                ref_field_name,
                self.target_table_namespace,
                self.target_table_name,
                target_field_name,
                alignment_type,
                self.value_matches_df,
                self.value_matches_row_num,
            )
        elif alignment_type == fg.ALIGNMENT_TYPE_SPARSE_ALIGNMENT:
            ref_mfv = self.field_profiles.get_field_mfv(ref_field_name)
            target_mfv = self.field_profiles.get_field_mfv(target_field_name)
            self.value_matches_row_num = (
                self.value_matches.add_sparse_alignment_values_to_df(
                    self.ref_table_namespace,
                    self.ref_table_name,
                    ref_field_name,
                    self.target_table_namespace,
                    self.target_table_name,
                    target_field_name,
                    ref_mfv,
                    target_mfv,
                    alignment_type,
                    self.value_matches_df,
                    self.value_matches_row_num,
                )
            )

        return row_num

    def add_matches_to_result(
            self,
            row_num: int,
            target_field_name: str,
            matches: [str],
            alignment_type: str,
            alignment_strength: float,
    ):
        for r in matches:
            row_num = self.add_match_to_result(
                row_num,
                r,
                target_field_name,
                alignment_type,
                alignment_strength,
            )
        return row_num

    def process_other_alignments(
            self,
            row_num: int,
            other_alignments: [],
            max_alignment_strength: float,
            min_selectivity: float,
            max_exact_match_strength: float,
            max_strength_selectivity_ratio: float,
            target_field_name: str,
            alignment_type: str,
    ):
        # now it gets tricky, we want to find other alignments where ratio of alignment strength and selectivity vs.
        # the match with highest alignment strength and minimum selectivity is > ALIGNMENT_SELECTIVITY_RATIO_THRESHOLD
        threshold = (
                max_strength_selectivity_ratio - fg.ALIGNMENT_SELECTIVITY_RATIO_THRESHOLD
        )
        for r in other_alignments:
            alignment_strength = self.get_alignment_strength(
                target_field_name, r, alignment_type
            )
            if alignment_strength > max_exact_match_strength:
                # worth exploring
                selectivity = self.get_selectivity(r)
                strength_selectivity_ratio = alignment_strength / selectivity
                if strength_selectivity_ratio >= threshold:
                    self.add_match_to_result(
                        row_num,
                        r,
                        target_field_name,
                        alignment_type,
                        alignment_strength,
                    )
                    row_num += 1
        return row_num

    def add_match(
            self,
            ref_field_name: str,
            target_field_name: str,
            alignment_type: str,
            alignment_strength: float,
    ):
        tf = self.potential_matches
        if target_field_name not in tf.keys():
            tf[target_field_name] = {}
        rf = tf[target_field_name]
        if ref_field_name not in rf.keys():
            rf[ref_field_name] = {}
        al = rf[ref_field_name]
        al[alignment_type] = alignment_strength
        if (
                ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY
                or target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY
                or (
                ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                and target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
        )
        ):
            logger.info(
                f"FALDISCO__DEBUG: Add_Match: Adding {alignment_type}={alignment_strength} between {ref_field_name} and {target_field_name}"
            )

    def get_matches(self, target_field_name: str):
        # if target_field_name not in self.potential_matches.keys():
        #     self.potential_matches[target_field_name] = {}
        return self.potential_matches[target_field_name]

    def get_alignments(self, target_field_name: str, ref_field_name: str):
        tm = self.get_matches(target_field_name)
        if ref_field_name not in tm.keys():
            tm[ref_field_name] = {}
        return tm[ref_field_name]

    def get_alignment_strength(
            self, target_field_name: str, ref_field_name: str, alignment_type: str
    ):
        al = self.get_alignments(target_field_name, ref_field_name)
        if alignment_type not in al.keys():
            return 0
        else:
            return al[alignment_type]

    def get_results_df(self):
        return self.results_df
