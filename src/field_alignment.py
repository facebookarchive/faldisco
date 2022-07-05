#!/usr/bin/env python3
# pyre-strict


# Copyright (c) Meta Platforms, Inc. and affiliates.

# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging
from typing import Dict

import pandas as pd
from pandas import DataFrame

import faldisco_globals as fg
from faldisco_results import Faldisco_Results
from field_combinations import Field_Combinations
from field_profiles import Field_Profiles
from value_matches import Value_Matches

logger = logging.getLogger(__name__)


class Field_Alignment:
    ref_table_namespace: str
    ref_table_name: str
    target_table_namespace: str
    target_table_name: str
    # contains valid ref, target field combinations and their alignment percentage - 0 means no alignment and no
    # processing
    alignment_combinations: Field_Combinations
    exact_match_combinations: Field_Combinations
    sparse_alignment_combinations: Field_Combinations
    alignment_exact_match_combinations: Field_Combinations

    ref_field_names = [str]  # list of reference fields
    target_field_names = [str]  # list of target fields
    join_field_names = [str]  # list of join fields
    orig_ref_field_names = [str]  # list of reference fields
    orig_target_field_names = [str]  # list of target fields
    orig_join_field_names = [str]  # list of join fields
    df: DataFrame  # data frame with the join
    deduped_df: DataFrame  # data frame without any duplicates
    num_rows = 0
    field_profiles : Dict[str, Field_Profiles] = {}

    # final list of aligned field combinations
    results_df: DataFrame
    # final list of matches organized by target_field_name, ref_field_name: [alignment_type, alignment_strength]
    results: Faldisco_Results

    value_matches: Value_Matches
    sparse_value_matches: Value_Matches

    def __init__(
            self,
            ref_table_namespace: str,
            ref_table_name: str,
            target_table_namespace: str,
            target_table_name: str,
            join_field_names: [str],
            ref_field_names: [str],
            target_field_names: [str],
    ):
        self.target_table_namespace = target_table_namespace
        self.target_table_name = target_table_name
        self.ref_table_namespace = ref_table_namespace
        self.ref_table_name = ref_table_name
        self.orig_ref_field_names = ref_field_names
        self.orig_target_field_names = target_field_names
        self.orig_join_field_names = join_field_names
        self.ref_field_names = [f"r__{c}" for c in self.orig_ref_field_names]
        self.target_field_names = [f"t__{c}" for c in self.orig_target_field_names]
        self.join_field_names = [f"r_j__{c}" for c in self.orig_join_field_names]
        self.alignment_combinations = Field_Combinations("alignments")
        self.exact_match_combinations = Field_Combinations("exact matches")
        self.sparse_alignment_combinations = Field_Combinations("sparse alignments")
        self.alignment_exact_match_combinations = Field_Combinations(
            "alignment exact matches"
        )
        self.results_df = DataFrame(columns=fg.FIELD_ALIGNMENT_TABLE_FIELDS)
        self.alignment_values_df = DataFrame(
            columns=fg.FIELD_VALUE_ALIGNMENT_TABLE_FIELDS
        )
        self.alignment_values_df.astype(fg.FIELD_VALUE_ALIGNMENT_TABLE_FIELD_TYPES)
        self.results = None

    @staticmethod
    def check_min_int(current: int, new: int):
        if current == -1 or current > new:
            return new
        else:
            return current

    @staticmethod
    def check_max_int(current: int, new: int):
        if current == -1 or current < new:
            return new
        return current

    @staticmethod
    def check_min_str(current: str, new: str):
        if current is None or current > new:
            return new
        else:
            return current

    @staticmethod
    def check_max_str(current: str, new: str):
        if current is None or current < new:
            return new
        else:
            return current

    def profile_field(self, df: DataFrame, field_name: str):
        logger.setLevel(logging.DEBUG)
        # field_df = df.field_name
        sorted_df = df.sort_values(by=[field_name])
        prev = None
        # most frequent value count
        mfv_count = 0
        unique_count = 0
        current_count = 0
        mfv = None
        min_val = None
        max_val = None
        min_len = -1
        max_len = -1
        val = None
        val_len = -1
        num_rows = self.num_rows

        for _index, r in sorted_df.iterrows():
            val = str(r[field_name])
            if prev is None or prev != val:
                unique_count += 1
                if prev is not None and current_count > mfv_count:
                    # we have a new value, process previous value
                    mfv_count = current_count
                    mfv = prev
                prev = val
                # if value is not a FALDISCO fill in for NULL or empty string, check the length
                if not fg.is_special_value(val):
                    val_len = len(str(val))
                    min_len = Field_Alignment.check_min_int(min_len, val_len)
                    max_len = Field_Alignment.check_max_int(max_len, val_len)
                    min_val = Field_Alignment.check_min_str(min_val, val)
                    max_val = Field_Alignment.check_max_str(max_val, val)
                current_count = 1
            else:
                # we have a duplicate value
                current_count += 1
        # process the last value
        if not fg.is_special_value(val):
            val_len = len(str(val))
            min_len = Field_Alignment.check_min_int(min_len, val_len)
            max_len = Field_Alignment.check_max_int(max_len, val_len)
            min_val = Field_Alignment.check_min_str(min_val, val)
            max_val = Field_Alignment.check_max_str(max_val, val)
        if current_count > mfv_count:
            # we have a new value, process previous value
            mfv_count = current_count
            mfv = prev
        # ok - here's what we have:
        # - unique_count has number of unique values in the field - if it is 1, we have a constant column
        # - mfv_count has the number of rows of most frequent value
        # self.field_profiles.set_field_mfv(field_name, str(mfv))
        selectivity = unique_count / num_rows
        fp = Field_Profiles(
            # num_rows: int,
            num_rows,
            # cardinality: int
            unique_count,
            # selectivity: float,
            selectivity,
            # mfv_count: int,
            mfv_count,
            # min_len: int,
            min_len,
            # max_len: int,
            max_len,
            # min_val: str,
            min_val,
            # max_val: str,
            max_val,
            # mfv
            mfv,
        )
        if field_name in fg.TRACE_FIELDS_ANY:
            logger.info(
                f"FALDISCO__DEBUG: profiling field: {field_name}: mfv={mfv}, mfv_count={mfv_count}, "
                + f"cardinality = {unique_count}, selectivity={selectivity}, "
                + f"min_len={min_len}, max_len={max_len}, min_val={min_val}, "
                + f"max_val={max_val}, is_unique={fp.is_unique_field()}, "
                + f"is_constant={fp.is_constant_field()}, is_sparse={fp.is_sparse_field()}"
            )
        return fp

    def profile_fields(self, df: DataFrame, field_names: {}):
        for c in field_names:
            self.field_profiles[c] = self.profile_field(df, c)

    def can_fields_have_exact_match(
            self, ref_field_name: str, target_field_name: str
    ) -> bool:
        rp = self.field_profiles[ref_field_name]
        tp = self.field_profiles[target_field_name]
        rmax_len = rp.get_field_max_len()
        rmin_len = rp.get_field_min_len()
        tmax_len = tp.get_field_max_len()
        tmin_len = tp.get_field_min_len()
        rmax = rp.get_field_max_val()
        rmin = rp.get_field_min_val()
        tmax = tp.get_field_max_val()
        tmin = tp.get_field_min_val()

        # first check if lengths overlap
        # then check if values can overlap - TODO: currently comparing as strings, may not work for all data types
        check = (
                rmin_len <= tmax_len
                and tmin_len <= rmax_len
                and rmax is not None
                and rmin is not None
                and tmax is not None
                and tmin is not None
                and rmin <= tmax
                and tmin <= rmax
        )
        return check

    def classify_field(
            self,
            field_name: str,
            alignment_ref_field_names: [],
            unique_ref_field_names: [],
            sparse_ref_field_names: [],
    ):
        fp = self.field_profiles[field_name]
        if not fp.is_constant_field():
            if fp.is_sparse_field():
                # Important: unique sparse fields should be treated as sparse, not as unique
                sparse_ref_field_names.append(field_name)
                if (
                        field_name in fg.TRACE_FIELDS_ANY
                        or field_name in fg.TRACE_FIELDS_ALL
                ):
                    logger.info(
                        f"FALDISCO__DEBUG: sparse field #{len(sparse_ref_field_names)}={field_name}"
                    )
            elif fp.is_unique_field():
                unique_ref_field_names.append(field_name)
                if (
                        field_name in fg.TRACE_FIELDS_ANY
                        or field_name in fg.TRACE_FIELDS_ALL
                ):
                    logger.info(
                        f"FALDISCO__DEBUG: unique field#{len(unique_ref_field_names)}= {field_name}"
                    )
            else:
                alignment_ref_field_names.append(field_name)
                if (
                        field_name in fg.TRACE_FIELDS_ANY
                        or field_name in fg.TRACE_FIELDS_ALL
                ):
                    logger.info(
                        f"FALDISCO__DEBUG: alignment field#{len(alignment_ref_field_names)}= {field_name}"
                    )

        else:
            if field_name in fg.TRACE_FIELDS_ANY or field_name in fg.TRACE_FIELDS_ALL:
                logger.info(f"FALDISCO__DEBUG: constant field {field_name}")

    def make_combinations(
            self,
            ref_field_names: [str],
            target_field_names: [str],
            ac: Field_Combinations,
            axc: Field_Combinations,
    ):
        num_alignment_combinations = 0
        num_exact_match_combinations = 0
        for r in ref_field_names:
            for t in target_field_names:
                # these fields can be used in alignment discovery and exact match discovery
                if ac is not None:
                    ac.add_combination(r, t)
                    num_alignment_combinations += 1
                if self.can_fields_have_exact_match(r, t):
                    axc.add_combination(r, t)
                    # xc.log_combinations()
                    num_exact_match_combinations += 1
        return num_alignment_combinations, num_exact_match_combinations

    def create_combinations(self, df: DataFrame):
        # go through all the ref and target fields and look for constant and unique fields
        logger.setLevel(logging.DEBUG)
        self.profile_fields(df, self.ref_field_names)
        self.profile_fields(df, self.target_field_names)
        # now that we have profiles, create three lists:
        # combos of potential alignments
        # combos of potential exact matches
        # combos of potential sparse field matches
        #
        # to do that, we are going to remove constant fields and break ref and target fields into three lists:
        # unique fields - these can only be used for exact matches
        unique_ref_field_names = []
        unique_target_field_names = []
        # sparse fields - these can only be used for sparse field matches
        sparse_ref_field_names = []
        sparse_target_field_names = []
        # remaining fields can be used for alignments and potential exact matches
        alignment_ref_field_names = []
        alignment_target_field_names = []

        for c in self.ref_field_names:
            self.classify_field(
                c,
                alignment_ref_field_names,
                unique_ref_field_names,
                sparse_ref_field_names,
            )

        for c in self.target_field_names:
            self.classify_field(
                c,
                alignment_target_field_names,
                unique_target_field_names,
                sparse_target_field_names,
            )

        # now that we have groups, make combinations
        num_alignment_combinations = 0
        num_exact_match_combinations = 0
        num_sparse_alignment_combinations = 0
        num_alignment_exact_match_combinations = 0
        xc = self.exact_match_combinations
        ac = self.alignment_combinations
        axc = self.alignment_exact_match_combinations
        (
            num_alignment_combinations,
            num_alignment_exact_match_combinations,
        ) = self.make_combinations(
            alignment_ref_field_names, alignment_target_field_names, ac, axc
        )

        self.value_matches = Value_Matches(
            alignment_ref_field_names, alignment_target_field_names
        )

        (_ignore, num_exact_match_combinations,) = self.make_combinations(
            unique_ref_field_names, unique_target_field_names, None, xc
        )

        sac = self.sparse_alignment_combinations
        (num_sparse_alignment_combinations, _ignore,) = self.make_combinations(
            sparse_ref_field_names, sparse_target_field_names, sac, axc
        )

        self.sparse_value_matches = Value_Matches(
            sparse_ref_field_names, sparse_target_field_names
        )
        logger.info(
            f"FALDISCO__DEBUG: Combos: alignment: {num_alignment_combinations}; "
            + f"exact:{num_exact_match_combinations}; sparse:{num_sparse_alignment_combinations}"
        )

    def process_row_alignments(self, row):
        vm = self.value_matches
        for r in self.alignment_combinations.get_ref_field_names():
            for t in self.alignment_combinations.get_target_field_names(r):
                # add value for this combination
                rval = row[r]
                tval = row[t]
                if rval != rval:
                    rval = fg.FALDISCO_NAN
                    if r in fg.TRACE_RECORDS_FOR_FIELDS_ANY or (
                            r in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                            and t in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                    ):
                        k = self.join_field_names[0]
                        kval = row[k]
                        logger.info(
                            f"FALDISCO__DEBUG: process row: alignment: unexpected value {r}={rval} at {k}={kval}"
                        )

                if tval != tval:
                    tval = fg.FALDISCO_NAN
                    if t in fg.TRACE_RECORDS_FOR_FIELDS_ANY or (
                            r in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                            and t in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                    ):
                        k = self.join_field_names[0]
                        kval = row[k]
                        logger.info(
                            f"FALDISCO__DEBUG: process row: alignment: unexpected value {t}={tval} at {k}={kval}"
                        )
                vm.add_value(r, t, rval, tval)

    @staticmethod
    def record_level_trace_for_field(
            field_name: str,
            other_field_name: str,
            msg: str,
    ):
        if field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY or (
                field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                and other_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
        ):
            logger.info(msg)

    @staticmethod
    def record_level_trace_for_combination_of_fields(
            field_name: str,
            other_field_name: str,
            msg: str,
    ):
        if (
                field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY
                or other_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ANY
                or (
                field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
                and other_field_name in fg.TRACE_RECORDS_FOR_FIELDS_ALL
        )
        ):
            logger.info(msg)

    def process_row_exact_matches(self, row):
        xc = self.exact_match_combinations

        for r in self.exact_match_combinations.get_ref_field_names():
            for t in self.exact_match_combinations.get_target_field_names(r):
                rval = row[r]
                tval = row[t]
                if rval == tval:
                    # found a match
                    xc.increment_combination(r, t, 1)
                    Field_Alignment.record_level_trace_for_combination_of_fields(
                        r,
                        t,
                        f"FALDISCO__DEBUG: found exact match between: {r} and {t} on {row[r]} num_matches={xc.get_combination(r, t)} ({self.exact_match_combinations.get_combination(r, t)})",
                    )
                elif rval != rval and tval != tval:
                    k = self.join_field_names[0]
                    kval = row[k]

                    xc.increment_combination(r, t, 1)
                    Field_Alignment.record_level_trace_for_combination_of_fields(
                        r,
                        t,
                        f"FALDISCO__DEBUG: found exact match between unexpected (nan) values {r} and {t} on {row[r]} at key={kval}.  num_matches={xc.get_combination(r, t)} ({self.exact_match_combinations.get_combination(r, t)})",
                    )
                elif rval != rval or tval != tval:
                    # no match, but log unexpected value
                    k = self.join_field_names[0]
                    kval = row[k]
                    if rval != rval:
                        Field_Alignment.record_level_trace_for_field(
                            r,
                            t,
                            f"FALDISCO__DEBUG: process_row: exact matches: unexpected value {r}={rval} at {k}={kval}",
                        )
                    else:
                        Field_Alignment.record_level_trace_for_field(
                            t,
                            r,
                            f"FALDISCO__DEBUG: process_row: exact matches: unexpected value {t}={tval} at {k}={kval}",
                        )

    def process_row_sparse_alignments(self, row):
        svm = self.sparse_value_matches
        for r in self.sparse_alignment_combinations.get_ref_field_names():
            for t in self.sparse_alignment_combinations.get_target_field_names(r):
                # add value for this combination
                svm.add_value(r, t, str(row[r]), str(row[t]))

    def process_row(self, row):
        self.process_row_alignments(row)
        self.process_row_exact_matches(row)
        self.process_row_sparse_alignments(row)

    def process_rows(self, df):
        # initialize field_alignment matrix
        for _index, row in df.iterrows():
            self.process_row(row)
        return len(df)

    def update_alignments(self):
        # row_num = len(results.index)
        vm = self.value_matches
        ac = self.alignment_combinations
        xac = self.alignment_exact_match_combinations
        remove_combinations = {}
        # ac.log_combinations()
        for r in ac.get_ref_field_names():
            for t in ac.get_target_field_names(r):
                check_for_exact_matches = xac.check_combination(r, t)
                # calculate alignment
                (
                    alignment,
                    exact_match_strength,
                    value_match_strength,
                ) = vm.calc_field_combination_alignment(
                    r, t, self.field_profiles, check_for_exact_matches
                )
                # first, process exact matches
                if exact_match_strength >= fg.FIELD_EXACT_MATCH_THRESHOLD:
                    self.results.add_match(
                        r,
                        t,
                        fg.ALIGNMENT_TYPE_EXACT_MATCH,
                        exact_match_strength,
                    )
                    xac.set_combination(r, t, exact_match_strength)
                else:
                    # no exact match
                    xac.remove_combination(r, t)

                # now process alignment, but only if it is stronger than exact_match_strength
                if (
                        exact_match_strength <= alignment
                        and alignment > fg.FIELD_ROW_ALIGNMENT_THRESHOLD
                        and value_match_strength > fg.FIELD_VALUE_ALIGNMENT_THRESHOLD
                ):
                    self.results.add_match(
                        r,
                        t,
                        fg.ALIGNMENT_TYPE_ALIGNMENT,
                        alignment,
                    )
                    ac.set_combination(r, t, alignment)
                else:
                    # stop tracking - there is no alignment
                    remove_combinations[r] = t
        # remove the combinations that are not exact matches
        for r in remove_combinations.keys():
            ac.remove_combination(r, remove_combinations[r])
        return

    def update_sparse_alignments(self):
        # row_num = len(results.index)
        svm = self.sparse_value_matches
        sac = self.sparse_alignment_combinations
        xac = self.alignment_exact_match_combinations
        remove_combinations = {}
        # sac.log_combinations()
        for r in sac.get_ref_field_names():
            for t in sac.get_target_field_names(r):
                check_for_exact_matches = xac.check_combination(r, t)
                # calculate alignment
                (
                    alignment,
                    exact_match_strength,
                    value_match_strength,
                    non_mfv_row_alignments,
                ) = svm.calc_sparse_field_combination_alignment(
                    r, t, self.field_profiles, check_for_exact_matches
                )
                Field_Alignment.record_level_trace_for_combination_of_fields(
                    r,
                    t,
                    f"FALDISCO__DEBUG: calc sparse exact matches between {r} and {t} returned: alignment = "
                    + f"{alignment}, exact_match_strength = {exact_match_strength}, "
                    + f"value_match_strength={value_match_strength}, non_mfv_row_alignments={non_mfv_row_alignments}",
                )
                # # first, process exact matches
                if exact_match_strength >= fg.FIELD_EXACT_MATCH_THRESHOLD:
                    self.results.add_match(
                        r,
                        t,
                        fg.ALIGNMENT_TYPE_SPARSE_EXACT_MATCH,
                        exact_match_strength,
                    )
                    xac.set_combination(r, t, exact_match_strength)
                else:
                    # no exact match
                    xac.remove_combination(r, t)

                # now process alignment, but only if it is stronger than exact_match_strength
                if (
                        exact_match_strength <= alignment
                        and alignment > fg.FIELD_ROW_ALIGNMENT_THRESHOLD
                        and value_match_strength > fg.FIELD_VALUE_ALIGNMENT_THRESHOLD
                ):
                    self.results.add_match(
                        r,
                        t,
                        fg.ALIGNMENT_TYPE_SPARSE_ALIGNMENT,
                        alignment,
                    )
                    sac.set_combination(r, t, alignment)
                elif (
                        non_mfv_row_alignments > fg.FIELD_SPARSE_NON_MFV_ALIGNMENT_THRESHOLD
                ):
                    # we do not have alignment, but looks like the shape matches - every time there is a non-mfv
                    # value in ref field, there is one in target field
                    self.results.add_match(
                        r,
                        t,
                        fg.ALIGNMENT_TYPE_SPARSE_NON_MFV_ALIGNMENT,
                        non_mfv_row_alignments,
                    )
                else:
                    # stop tracking - there is no alignment
                    remove_combinations[r] = t
        # remove the combinations that are not exact matches
        for r in remove_combinations.keys():
            sac.remove_combination(r, remove_combinations[r])
        return

    def update_exact_matches(self):
        match_strength: float
        num_rows = self.num_rows
        xc = self.exact_match_combinations
        remove_combinations = {}
        # xc.log_combinations()
        for r in xc.get_ref_field_names():
            for t in xc.get_target_field_names(r):
                # check if match is > threshold
                num_matches = xc.get_combination(r, t)
                match_strength = num_matches / num_rows
                if match_strength >= fg.FIELD_EXACT_MATCH_THRESHOLD:
                    self.results.add_match(
                        r, t, fg.ALIGNMENT_TYPE_EXACT_MATCH, match_strength
                    )
                else:
                    # stop tracking - there is no alignment
                    remove_combinations[r] = t
        # remove the combinations that are not exact matches
        for r in remove_combinations.keys():
            xc.remove_combination(r, remove_combinations[r])

    def find_field_alignment(self):
        # first prepare the data frame for processing
        # all remove duplicate rows
        # self.deduped_df = self.df.drop_duplicates(
        #     subset=self.join_field_names, keep=False
        # )
        self.deduped_df = self.df
        self.num_rows = len(self.deduped_df)
        logger.setLevel(logging.INFO)
        if self.num_rows == 0:
            logger.info("All rows are duplicates")
            return
        logger.setLevel(logging.DEBUG)
        logger.info(
            f"FALDISCO__DEBUG: Removed Duplicates. Remaining # rows: {self.num_rows}"
        )

        # see what field combinations we can create
        self.create_combinations(self.deduped_df)

        # check if there are any combinations left to check
        num_combinations = (
                self.alignment_combinations.num_combinations()
                + self.exact_match_combinations.num_combinations()
                + self.sparse_alignment_combinations.num_combinations()
        )

        logger.info(
            f"FALDISCO__DEBUG: Created combinations. total # combinations: {num_combinations}"
        )
        if num_combinations == 0:
            # nothing to evaluate
            return 0

        # Ok - we have good rows and good combinations, process the rows
        self.process_rows(self.deduped_df)

        # self.exact_match_combinations.log_combinations()
        # check field alignments and create a data frame with results (field_alignments_df)
        self.results = Faldisco_Results(
            self.field_profiles,
            self.ref_table_namespace,
            self.ref_table_name,
            self.target_table_namespace,
            self.target_table_name,
            self.value_matches,
            self.alignment_values_df,
        )

        self.update_alignments()
        self.update_exact_matches()
        self.update_sparse_alignments()
        self.results_df = self.results.dedup_results()
        num_result_rows = len(self.results_df)
        logger.info(f"FALDISCO__DEBUG: Processed results: {num_result_rows}")
        return num_result_rows

    # combine reference table and target table on alignment key
    # rename all ref fields r__ field name
    # rename all target fields t__ field name
    # to avoid name collissions
    def gen_sql(self) -> str:
        ojk = self.orig_join_field_names[0]
        rjk = self.join_field_names[0]
        sql_statement = f"""
            with key_counts as (select r.{ojk}, count(*) as numrows 
            from {self.ref_table_namespace}.{self.ref_table_name} r join 
            {self.target_table_namespace}.{self.target_table_name} t on 
            r.{ojk} = t.{ojk}  
            group by r.{ojk}) select r.{ojk} as {rjk} 
            """

        for c in self.orig_ref_field_names:
            sql_statement = (
                    sql_statement
                    + f""", case when (r.{c} is null) then 'FALDISCO_NULL' 
                        when cast(r.{c} as char) = '' then 'FALDISCO_EMPTY' 
                        else cast(r.{c} as char) end 
                        as r__{c}
                        """
            )
        for c in self.orig_target_field_names:
            sql_statement = (
                    sql_statement
                    + f""", case when (t.{c} is null) then 'FALDISCO_NULL' 
                       when cast(t.{c} as char) = '' then  'FALDISCO_EMPTY' 
                       else cast(t.{c} as char) end 
                       as t__{c}
                  """
            )
            # sql_statement = sql_statement + f", t.{c} as t__{c}"
        sql_statement = (
                sql_statement
                + f" from {self.ref_table_namespace}.{self.ref_table_name} r join "
                + f"{self.target_table_namespace}.{self.target_table_name} t on "
                + f"r.{ojk} = t.{ojk}"
                + f" join key_counts k on r.{ojk} = k.{ojk}"
                + f" where  k.numrows >= {fg.KEY_MIN_VALUE_COUNT} and k.numrows <= {fg.KEY_MAX_VALUE_COUNT}"
                + f" LIMIT {fg.SAMPLE_SIZE}"
        )
        return sql_statement

    def profiles_to_df(
            self,
            profiling_table_fields: [str],
    ) -> pd.DataFrame:
        df = DataFrame(columns=profiling_table_fields)
        row_num = 0
        for r in self.ref_field_names:
            self.add_profile_field_to_df(
                df, row_num, self.ref_table_namespace, self.ref_table_name, r
            )
            row_num += 1

        # now the target table
        for t in self.target_field_names:
            self.add_profile_field_to_df(
                df, row_num, self.target_table_namespace, self.target_table_name, t
            )
            row_num += 1
        return df

    def add_profile_field_to_df(
            self,
            df: DataFrame,
            row_num: int,
            table_namespace: str,
            table_name: str,
            field_name: str
    ) -> None:
        fp = self.field_profiles[field_name]
        original_field_name = fg.make_orig_field_name(field_name)
        if fp.is_unique_field():
            is_unique = "y"
        else:
            is_unique = "n"
        if fp.is_sparse_field():
            is_sparse = "y"
        else:
            is_sparse = "n"
        if fp.is_constant_field():
            is_constant = "y"
        else:
            is_constant = "n"

        df.loc[row_num] = [
            # "table_namespace",
            table_namespace,
            # "table_name",
            table_name,
            # "field_name",
            original_field_name,
            # "cardinality",
            fp.get_field_cardinality(),
            # "selectivity",
            fp.get_field_selectivity(),
            # "min",
            fp.get_field_min_val(),
            # "max",
            fp.get_field_max_val(),
            # "min_len",
            fp.get_field_min_len(),
            # "max_len",
            fp.get_field_max_len(),
            # "mfv_count",
            fp.get_field_mfv_count(),
            # "num_rows",
            fp.get_num_rows(),
            # "is_unique",
            is_unique,
            # "is_sparse",
            is_sparse,
            # "is_constant",
            is_constant,
        ]
