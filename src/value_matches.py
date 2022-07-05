#!/usr/bin/env python3
# pyre-strict


# Copyright (c) Meta Platforms, Inc. and affiliates.

# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging

from pandas import DataFrame

import faldisco_globals as fg
from field_profiles import Field_Profiles

logger = logging.getLogger(__name__)
ALIGNMENT_VALUE_ROW_MATCH_THRESHOLD = 0.8
ALIGNMENT_VALUE_ROW_MATCH_THRESHOLD = 0.3


class Value_Matches:
    value_matches = {}

    def __init__(self, ref_field_names: [str], target_field_names: [str]):
        # initialize the ref_field, target_field levels
        for r in ref_field_names:
            self.value_matches[r] = {}
            for t in target_field_names:
                self.value_matches[r][t] = {}

    def get_ref_values(self, ref_field_name: str, target_field_name: str):
        target_fields = self.get_target_fields(ref_field_name)
        if target_field_name not in target_fields.keys():
            target_fields[target_field_name] = {}
        return target_fields[target_field_name]

    def get_target_fields(self, ref_field_name: str):
        vm = self.value_matches
        if ref_field_name not in vm.keys():
            vm[ref_field_name] = {}
        return vm[ref_field_name]

    def get_target_values(
            self, ref_field_name: str, target_field_name: str, ref_value: str
    ):
        ref_values = self.get_ref_values(ref_field_name, target_field_name)
        if ref_value not in ref_values.keys():
            ref_values[ref_value] = {}
        return ref_values[ref_value]

    def get_alignment(
            self,
            ref_field_name: str,
            target_field_name: str,
            ref_value: str,
            target_value: str,
    ):
        target_values = self.get_target_values(
            ref_field_name, target_field_name, ref_value
        )
        if target_value not in target_values.keys():
            target_values[target_value] = 0
        return target_values[target_value]

    # increment count for this combination of field names and values
    def add_value(
            self, ref_field_name, target_field_name, ref_field_value, target_field_value
    ):
        alignment = self.get_alignment(
            ref_field_name, target_field_name, ref_field_value, target_field_value
        )
        target_values = self.get_target_values(
            ref_field_name, target_field_name, ref_field_value
        )
        target_values[target_field_value] = alignment + 1

    def calc_sparse_field_combination_alignment(
            self,
            ref_field_name: str,
            target_field_name: str,
            profiles: {Field_Profiles},
            check_for_exact_matches: bool,
    ):
        aligned_rows = 0
        matching_rows = 0
        total_rows = 0
        total_values = 0
        matching_values = 0
        # first, find mfv for reference and target fields
        ref_fp = profiles[ref_field_name]
        ref_mfv = ref_fp.get_field_mfv()
        target_fp = profiles[target_field_name]
        target_mfv = target_fp.get_field_mfv()
        is_unique = ref_fp.is_unique_field() or target_fp.is_unique_field()

        ref_values = self.get_ref_values(ref_field_name, target_field_name)

        if target_field_name in fg.TRACE_FIELDS_ANY or (
                target_field_name in fg.TRACE_FIELDS_ALL
                and ref_field_name in fg.TRACE_FIELDS_ALL
        ):
            logger.info(
                f"FALDISCO__DEBUG: CALC_SPARSE_ALIGNMENT between {ref_field_name} and {target_field_name}: "
                + f"ref_mfv={ref_mfv}, target_mfv={target_mfv}, is_unique={str(is_unique)}"
            )
        for rval in ref_values.keys():
            max_count = 0
            target_values = self.get_target_values(
                ref_field_name, target_field_name, rval
            )
            tvals = len(target_values)
            trows = 0
            mismatches = 0
            total_values += len(target_values)
            for tval in target_values.keys():
                this_count = self.get_alignment(
                    ref_field_name, target_field_name, rval, tval
                )
                if rval == ref_mfv or tval == target_mfv:
                    # skip the mfv to mfv matches - they are meaningless for sparse fields
                    # mfv to non-mfv matches are counted as mismatches
                    if rval != ref_mfv or tval != target_mfv:
                        mismatches += this_count
                        total_rows += this_count
                        trows += this_count
                else:
                    if check_for_exact_matches and rval == tval:
                        matching_rows += this_count
                    if (not is_unique) and this_count > max_count:
                        max_count = this_count
                    total_rows += this_count
                    trows += this_count
                    tvals += 1
                    if target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY or (
                            target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                            and ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                    ):
                        logger.info(
                            f"FALDISCO__DEBUG: CALC_ALIGNMENT {ref_field_name}={rval} {target_field_name}={tval} match={str(this_count)}, max_count={str(max_count)}, "
                            + f"trows={(trows)}, total_rows={(total_rows)}"
                        )
            # if the number of target values corresponding to this ref value < 1/threshold, we have a match
            if (not is_unique) and (
                    tvals == 1
                    or (
                            max_count > trows * ALIGNMENT_VALUE_ROW_MATCH_THRESHOLD
                            and tvals / ALIGNMENT_VALUE_ROW_MATCH_THRESHOLD
                    )
            ):
                matching_values += 1
            # total_rows = total_rows + this_count
            aligned_rows = aligned_rows + max_count
            if target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY or (
                    target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                    and ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
            ):
                logger.info(
                    f"FALDISCO__DEBUG: CALC_SPARSE_ALIGNMENT {ref_field_name}={rval} {target_field_name}={tval} aligned_rows={str(aligned_rows)} total_rows={str(total_rows)} mismatches={str(mismatches)}"
                )
        if total_rows > 0 and total_values > 0:
            return (
                aligned_rows / total_rows,
                matching_rows / total_rows,
                matching_values / total_values,
                (total_rows - mismatches) / total_rows,
            )
        else:
            return (0, 0, 0, 0)

    def calc_field_combination_alignment(
            self,
            ref_field_name: str,
            target_field_name: str,
            profiles: {Field_Profiles},
            check_for_exact_matches: bool,
    ):
        aligned_rows = 0
        matching_rows = 0
        total_rows = 0
        non_unique_rows = 0
        total_values = 0
        matching_values = 0
        target_fp = profiles[target_field_name]
        # if we have more than 2 values, filter out MFV matches
        target_mfv = ""
        if target_fp.get_field_cardinality() > 2:
            target_mfv = target_fp.get_field_mfv()

        ref_values = self.get_ref_values(ref_field_name, target_field_name)
        for rval in ref_values.keys():
            max_count = 0
            max_tval = None
            target_values = self.get_target_values(
                ref_field_name, target_field_name, rval
            )
            tvals = len(target_values)
            trows = 0
            total_values += len(target_values)
            for tval in target_values.keys():
                this_count = self.get_alignment(
                    ref_field_name, target_field_name, rval, tval
                )
                if this_count > max_count:
                    max_count = this_count
                    max_tval = tval
                if check_for_exact_matches:
                    trace = "matches"
                    if rval == tval:
                        matching_rows += this_count
                    else:
                        trace = "mismatches"

                    if (
                            ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY
                            or target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY
                            or (
                            ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                            and target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                    )
                    ):
                        logger.info(
                            f"FALDISCO__DEBUG: CALC_ALIGNMENT: found {this_count} exact {trace} between {ref_field_name}={rval} and {target_field_name}={tval}. this count={this_count}, matching_rows = {matching_rows}"
                        )
                trows += this_count
                total_rows += this_count
                tvals += 1
                if (
                        ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY
                        or target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY
                        or (
                        ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                        and target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                )
                ):
                    logger.info(
                        f"FALDISCO__DEBUG: CALC_ALIGNMENT found alignment between {ref_field_name}={rval} {target_field_name}={max_tval} for {max_count} aligned rows"
                    )
            # if the number of target values corresponding to this ref value < 1/threshold,
            # the target value is not a sigle row and target value is not target field mfv,
            # we have a match
            if (
                    tvals == 1
                    or max_tval == target_mfv
                    or (
                    max_count > trows * ALIGNMENT_VALUE_ROW_MATCH_THRESHOLD
                    and tvals / ALIGNMENT_VALUE_ROW_MATCH_THRESHOLD
            )
            ):
                matching_values += 1
            # if we do not have a unique value, adjust aligned row count
            if trows > 1:
                non_unique_rows += trows
                aligned_rows += max_count
                if (
                        ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY
                        or target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY
                        or (
                        ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                        and target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                )
                ):
                    logger.info(
                        f"FALDISCO__DEBUG: CALC_ALIGNMENT {ref_field_name}={rval} {target_field_name}={tval} aligned_rows={aligned_rows} non_unique_rows={non_unique_rows}, total_rows={total_rows}"
                    )
        return (
            aligned_rows / non_unique_rows,
            matching_rows / total_rows,
            matching_values / total_values,
        )

    def add_alignment_values_to_df(
            self,
            ref_table_namespace: str,
            ref_table_name: str,
            ref_field_name: str,
            target_table_namespace: str,
            target_table_name: str,
            target_field_name: str,
            alignment_type: str,
            df: DataFrame,
            row_num: int,
    ) -> int:
        orig_ref_field_name = fg.make_orig_field_name(ref_field_name)
        orig_target_field_name = fg.make_orig_field_name(target_field_name)
        ref_values = self.get_ref_values(ref_field_name, target_field_name)
        for rval in ref_values.keys():
            max_count = 0
            max_tval = None
            target_values = self.get_target_values(
                ref_field_name, target_field_name, rval
            )
            trows = 0
            tvals = 0
            for tval in target_values.keys():
                this_count = self.get_alignment(
                    ref_field_name, target_field_name, rval, tval
                )
                if this_count > max_count:
                    max_count = this_count
                    max_tval = tval
                trows += this_count
                tvals += 1

            if (
                    (ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY)
                    or (target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY)
                    or (
                    ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                    and target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
            )
            ):
                logger.info(
                    f"FALDISCO__DEBUG: adding value alignment[{row_num}]: {ref_field_name}={rval}, {target_field_name}={max_tval}, {alignment_type}, alignment={max_count}, misalignment={trows - max_count}"
                )
            # add rval and max_tval to results
            df.loc[row_num] = [
                ref_table_namespace,
                ref_table_name,
                orig_ref_field_name,
                target_table_namespace,
                target_table_name,
                orig_target_field_name,
                str(rval),
                str(max_tval),
                alignment_type,
                max_count,
                trows - max_count,
            ]
            row_num += 1
        return row_num


def add_sparse_alignment_values_to_df(
        self,
        ref_table_namespace: str,
        ref_table_name: str,
        ref_field_name: str,
        target_table_namespace: str,
        target_table_name: str,
        target_field_name: str,
        ref_mfv: str,
        target_mfv: str,
        alignment_type: str,
        df: DataFrame,
        row_num: int,
) -> int:
    orig_ref_field_name = fg.make_orig_field_name(ref_field_name)
    orig_target_field_name = fg.make_orig_field_name(target_field_name)
    ref_values = self.get_ref_values(ref_field_name, target_field_name)
    for rval in ref_values.keys():
        if rval != ref_mfv:
            max_count = 0
            max_tval = None
            target_values = self.get_target_values(
                ref_field_name, target_field_name, rval
            )
            trows = 0
            tvals = 0
            for tval in target_values.keys():
                this_count = self.get_alignment(
                    ref_field_name, target_field_name, rval, tval
                )
                if this_count > max_count:
                    max_count = this_count
                    max_tval = tval
                trows += this_count
                tvals += 1
            if max_tval != target_mfv:
                # add rval and max_tval to results
                if (
                        (ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY)
                        or (target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY)
                        or (
                        ref_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                        and target_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                )
                ):
                    logger.info(
                        f"FALDISCO__DEBUG: adding sparse value alignment[{row_num}]: {ref_field_name}={rval}, {target_field_name}={max_tval}, {alignment_type}, alignment={max_count}, misalignment={trows - max_count}"
                    )
                df.loc[row_num] = [
                    ref_table_namespace,
                    ref_table_name,
                    orig_ref_field_name,
                    target_table_namespace,
                    target_table_name,
                    orig_target_field_name,
                    str(rval),
                    str(max_tval),
                    alignment_type,
                    max_count,
                    trows - max_count,
                ]
                row_num += 1
    return row_num
