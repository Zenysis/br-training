#!/usr/bin/env python
''' Generates a file mapping occupation code to title. Currently, just merges the
    pre 2002 codes with the CBO 2002 codes.
'''

import sys

import numpy as np
import pandas as pd
from pylib.base.flags import Flags

from log import LOG

CODE_COLUMN = 'CODIGO'
TITLE_COLUMN = 'TITULO'

FAMILY_COLUMN = 'family'
SUBGROUP_COLUMN = 'subgroup'
PRINCIPAL_SUBGROUP_COLUMN = 'principal_subgroup'
GROUP_COLUMN = 'group'


# Expand the code range column into rows. The input codes must be 3 digits and
# either of the form ###-### or ###.
def expand_3_digit_ranges(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_index(TITLE_COLUMN)
    df['start_code'] = df[CODE_COLUMN].str[:3].astype(int)
    df['end_code'] = df[CODE_COLUMN].str[-3:].astype(int)

    enumerated_codes = [
        np.arange(row['start_code'], row['end_code'] + 1) for _, row in df.iterrows()
    ]
    df = (
        pd.DataFrame.from_records(data=enumerated_codes, index=df.index)
        .stack()
        .reset_index(1, drop=True)
        .reset_index()
        .rename(columns={0: CODE_COLUMN})
    )
    df[CODE_COLUMN] = df[CODE_COLUMN].astype(int).astype(str).str.pad(3, fillchar='0')
    return df


# Merge in the provided dataframe to add new category column
def add_category(
    df: pd.DataFrame, merged_df: pd.DataFrame, code_length: int, new_col_name: str
) -> pd.DataFrame:
    len_df = len(df)
    df['temp'] = df[CODE_COLUMN].str[:code_length]

    df = (
        df.merge(
            merged_df,
            left_on='temp',
            right_on=CODE_COLUMN,
            suffixes=('', '_merged'),
        )
        .drop(columns=['temp', f'{CODE_COLUMN}_merged'])
        .rename(columns={f'{TITLE_COLUMN}_merged': new_col_name})
    )
    assert len_df == len(df)
    return df


def main():
    Flags.PARSER.add_argument(
        '--short_title',
        type=str,
        required=True,
        help='CSV lookup between code and title for 3 digit pre 2002 codes',
    )
    Flags.PARSER.add_argument(
        '--short_subgroup',
        type=str,
        required=True,
        help='CSV lookup between code and subgroup for 3 digit pre 2002 codes',
    )
    Flags.PARSER.add_argument(
        '--short_group',
        type=str,
        required=True,
        help='CSV lookup between code and group for 3 digit pre 2002 codes',
    )
    Flags.PARSER.add_argument(
        '--cbo94_to_cbo2002',
        type=str,
        required=True,
        help='CSV lookup between CBO 1994 5 digit codes to CBO 2002 6 digit codes',
    )
    Flags.PARSER.add_argument(
        '--cbo_title',
        type=str,
        required=True,
        help='CSV lookup between code and title for 6 digit CBO 2002 codes',
    )
    Flags.PARSER.add_argument(
        '--cbo_family',
        type=str,
        required=True,
        help='CSV lookup between code and family for 6 digit CBO 2002 codes',
    )
    Flags.PARSER.add_argument(
        '--cbo_subgroup',
        type=str,
        required=True,
        help='CSV lookup between code and subgroup for 6 digit CBO 2002 codes',
    )
    Flags.PARSER.add_argument(
        '--cbo_principal_subgroup',
        type=str,
        required=True,
        help='CSV lookup between code and principal subgroup for 6 digit CBO 2002 codes',
    )
    Flags.PARSER.add_argument(
        '--cbo_group',
        type=str,
        required=True,
        help='CSV lookup between code and group for 6 digit CBO 2002 codes',
    )
    Flags.PARSER.add_argument(
        '--output',
        type=str,
        required=True,
        help='File path for the output occupation lookup',
    )
    Flags.InitArgs()

    # Short codes
    LOG.info('Started processing pre 2002 3 digit codes')
    short_titles = pd.read_csv(Flags.ARGS.short_title, dtype=str)
    short_subgroups = pd.read_csv(Flags.ARGS.short_subgroup, dtype=str)
    short_groups = pd.read_csv(Flags.ARGS.short_group, dtype=str)
    short_groups[TITLE_COLUMN] = short_groups[TITLE_COLUMN].str.split('-').str[1]

    short_subgroups = expand_3_digit_ranges(short_subgroups)
    short_groups = expand_3_digit_ranges(short_groups)

    # Here the "subgroup" input becomes the "principal subgroup" column to
    # align with the CBO 2002 categories.
    short_titles = add_category(
        short_titles, short_subgroups, 3, PRINCIPAL_SUBGROUP_COLUMN
    )
    short_titles = add_category(short_titles, short_groups, 3, GROUP_COLUMN)
    LOG.info('Finished processing pre 2002 3 digit codes: %s found', len(short_titles))

    # CBO 2002 codes
    LOG.info('Started processing CBO 6 digit codes')
    cbo_titles = pd.read_csv(Flags.ARGS.cbo_title, dtype=str)
    cbo_families = pd.read_csv(Flags.ARGS.cbo_family, dtype=str)
    cbo_sub_groups = pd.read_csv(Flags.ARGS.cbo_subgroup, dtype=str)
    cbo_sub_groups[TITLE_COLUMN] = cbo_sub_groups[TITLE_COLUMN].str.title()
    cbo_principal_sub_groups = pd.read_csv(Flags.ARGS.cbo_principal_subgroup, dtype=str)
    cbo_principal_sub_groups[TITLE_COLUMN] = cbo_principal_sub_groups[
        TITLE_COLUMN
    ].str.title()
    cbo_group = pd.read_csv(Flags.ARGS.cbo_group, dtype=str)
    cbo_group[TITLE_COLUMN] = cbo_group[TITLE_COLUMN].str.title()

    cbo_titles = add_category(cbo_titles, cbo_families, 4, FAMILY_COLUMN)
    cbo_titles = add_category(cbo_titles, cbo_sub_groups, 3, SUBGROUP_COLUMN)
    cbo_titles = add_category(
        cbo_titles, cbo_principal_sub_groups, 2, PRINCIPAL_SUBGROUP_COLUMN
    )
    cbo_titles = add_category(cbo_titles, cbo_group, 1, GROUP_COLUMN)
    LOG.info('Finished processing CBO 6 digit codes: %s found', len(cbo_titles))

    # Code conversion table
    LOG.info('Started processing conversion table')
    cbo_94 = pd.read_csv(Flags.ARGS.cbo94_to_cbo2002, dtype=str, sep=';')
    cbo_94 = (
        cbo_94.merge(
            cbo_titles,
            left_on='CBO2002',
            right_on=CODE_COLUMN,
        )
        .drop(columns=['CBO2002', CODE_COLUMN])
        .rename(columns={'CBO94': CODE_COLUMN})
    )
    LOG.info('Finished processing conversion table: %s found', len(cbo_94))

    LOG.info('Outputting merged file')
    pd.concat([short_titles, cbo_94, cbo_titles]).to_csv(Flags.ARGS.output, index=False)


if __name__ == '__main__':
    sys.exit(main())
