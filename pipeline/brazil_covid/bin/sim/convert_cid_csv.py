#!/usr/bin/env python
''' Converts the raw CID 9 and 10 codes into a csv lookup from code to cause of death
    title as well as each category that code falls into. Although the input data needs
    to be converted, it is highly structured, which is used to build the output. CID 9
    vs 10 are generally structured the same although they have some key differences.

    The output columns (title, parent, and categories have a `cause_of_death_` prefix):
      - CID ID: These are the CID codes found in the SIM data.
      - title: The display name of this cause of death.
      - parent: The direct parent category for the cause of death. Note the parent code
            is sometimes used in the data, so there should be a row for each parent
            category (with an empty title).
      - category 1, 2, 3, and 4: Category 1 is the top level category with the other ones
            falling underneath it. Some causes of death have more categories than others,
            but all parents and codes fall into at least one category. For rows with less
            than 4 categories, the remaining ones are empty.
    The output title, parent, and categories columns should all include the code or range
    of codes in the text.

    CID 9:
    The codes are 4 digit numbers, ex. 0010.
    There are parent categories with a 3 digit number and then 'X', ex. 001X.
    There is only one level of categories with 3 digit ranges, ex. 001-139.

    Example:
    For the following input rows:
        cause_of_death_title                cid_id
        Doenças infecciosas e parasitárias  001-139
        Cólera                              001X
        Devida a Vibrio cholerae            0010
        Devida a Vibrio cholerae el tor     0011
        Não especificada                    0019
    The output would be:
        cid_id  title                                   parent          category1                                   category2   category3   category4
        001X                                            001X: Cólera    001-139: Doenças infecciosas e parasitárias
        0010    0010: Devida a Vibrio cholerae          001X: Cólera    001-139: Doenças infecciosas e parasitárias
        0011    0011: Devida a Vibrio cholerae el tor   001X: Cólera    001-139: Doenças infecciosas e parasitárias
        0019    0019: Cólera: Não especificada          001X: Cólera    001-139: Doenças infecciosas e parasitárias

    CID 10:
    The codes are a letter and then 3 digit number, ex. A000.
    There are parent categories with a letter and then 2 digit number, ex. A00.
    There are 1 to 4 levels of categories with 1 letter and 2 digit ranges, ex. A00-B99.

    The data is read in as a single column that must be split into code and title columns.

    Example:
    For the following input rows:
        cid
        (A00-B99) Algumas doenças infecciosas e parasitárias
        (A00-A09) Doenças infecciosas intestinais
        (A00) Cólera
        (A00.0) Cólera devida a Vibrio cholerae 01 biótipo cholerae
        (A00.1) Cólera devida a Vibrio cholerae 01 biótipo El Tor
        (A00.9) Cólera não especificada
    The output would be:
        cid_id  title                                                       parent          category1                                               category2                                   category3   category4
        A00                                                                 A00: Cólera     A00-B99: Algumas doenças infecciosas e parasitárias     A00-A09: Doenças infecciosas intestinais
        A000    A00.0: Cólera devida a Vibrio cholerae 01 biótipo cholerae  A00: Cólera     A00-B99: Algumas doenças infecciosas e parasitárias     A00-A09: Doenças infecciosas intestinais
        A001    A00.1: Cólera devida a Vibrio cholerae 01 biótipo El Tor    A00: Cólera     A00-B99: Algumas doenças infecciosas e parasitárias     A00-A09: Doenças infecciosas intestinais
        A009    A00.9: Cólera não especificada                              A00: Cólera     A00-B99: Algumas doenças infecciosas e parasitárias     A00-A09: Doenças infecciosas intestinais
'''

from collections import defaultdict
import sys

import pandas as pd
from pylib.base.flags import Flags

from log import LOG

# pylint: disable=unsupported-assignment-operation,unsubscriptable-object,no-member

ALPHABET_LOCATION = {letter: i for i, letter in enumerate('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}
ALPHABET_LOOKUP = {i: letter for i, letter in enumerate('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}

CATEGORY_LOOKUP = {}

ID_COL = 'cid_id'
CATEGORY_1_COL = 'cause_of_death_category1'
CATEGORY_2_COL = 'cause_of_death_category2'
CATEGORY_3_COL = 'cause_of_death_category3'
CATEGORY_4_COL = 'cause_of_death_category4'
PARENT_COL = 'cause_of_death_parent'
TITLE_COL = 'cause_of_death_title'


def less_than(start_CID, end_CID, inclusive=True):
    if ALPHABET_LOCATION[start_CID.letter] < ALPHABET_LOCATION[end_CID.letter]:
        return True
    elif ALPHABET_LOCATION[start_CID.letter] == ALPHABET_LOCATION[end_CID.letter]:
        if start_CID.number < end_CID.number:
            return True
        elif start_CID.number == end_CID.number:
            return inclusive
        else:
            return False
    else:
        return False


def format_cid(cid):
    return cid[0], int(cid[1:])


class CID:
    def __init__(self, letter, number):
        self.letter = letter
        self.number = number

    def to_id(self):
        number = str(self.number)
        return f'{self.letter}{number.zfill(2)}'

    def add_one(self):
        if self.number < 99:
            return CID(self.letter, self.number + 1)
        else:
            return CID(ALPHABET_LOOKUP[ALPHABET_LOCATION[self.letter] + 1], 0)


# Separate CID id and title into different columns
def separate_title_id(dataframe):
    output = []
    for row in dataframe['cid']:
        cid, title = row.split(')', 1)
        cid = cid.replace('(', '')
        output.append({ID_COL: cid, TITLE_COL: title.strip()})
    return pd.DataFrame(output)


# For the CID 9 categories with a 3 digit numerical range and title, add an
# entry to CATEGORY_LOOKUP for each code in the range. Ex. "001-139,Doenças
# infecciosas e parasitárias" would give keys 001, 002, ..., 139 with values
# "{key}: Doenças infecciosas e parasitárias".
def build_cid9_category_lookup(category):
    floor, ceil = [int(val) for val in category[ID_COL].split('-')]
    for val in range(floor, ceil + 1):
        CATEGORY_LOOKUP[
            str(val).zfill(3)
        ] = f'{category[ID_COL]}: {category[TITLE_COL]}'


# Given a function to determine if a row is a parent from the CID ID column,
# convert those rows to be parents. This function relies on the parent row
# being first to then forward fill the values.
def build_parent_column(df, is_parent):
    # Create parent column and set titles to be empty for parent rows
    df.loc[is_parent(df[ID_COL]), PARENT_COL] = df[TITLE_COL]
    df[PARENT_COL] = df[PARENT_COL].ffill()
    df.loc[is_parent(df[ID_COL]), TITLE_COL] = pd.NA

    # Some titles occur multiple times, so deduplicate them by adding the
    # parent like "parent: title". Don't do this if the duplicate titles
    # have the same parent as it won't help.
    index = df[TITLE_COL].duplicated(keep=False) & ~df[
        [TITLE_COL, PARENT_COL]
    ].duplicated(keep=False)
    df.loc[index, TITLE_COL] = df[PARENT_COL] + ': ' + df[TITLE_COL]

    # Add CID ID to title and parent columns.
    # NOTE(abby): This is essentially redoing the creation of the parent column.
    # It's done this way so the CID ID isn't included when deduplicating the
    # titles.
    df[PARENT_COL] = (
        df.loc[is_parent(df[ID_COL]), ID_COL]
        + ': '
        + df.loc[is_parent(df[ID_COL]), PARENT_COL]
    )
    df[PARENT_COL] = df[PARENT_COL].ffill()
    df.loc[~df[TITLE_COL].isna(), TITLE_COL] = df[ID_COL] + ': ' + df[TITLE_COL]

    return df


# For the CID 10 categories with a letter and 2 digit numerical range, return a
# list of all codes in the range. Ex. A00-B99 would give A00, A01, ..., B99.
def list_cid10_codes_in_range(range_str):
    start_str, end_str = range_str.split('-')
    start = CID(*format_cid(start_str))
    end = CID(*format_cid(end_str))
    codes = []
    while less_than(start, end, inclusive=True):
        codes.append(start.to_id())
        if ALPHABET_LOCATION[start.letter] == 25 and start.number == 99:
            break
        start = start.add_one()
    return codes


def main():
    Flags.PARSER.add_argument(
        '--input_cid9',
        type=str,
        required=True,
        help='Raw csv lookup of cause of death (CID 9) to id.',
    )
    Flags.PARSER.add_argument(
        '--input_cid10',
        type=str,
        required=True,
        help='Raw csv lookup of cause of death (CID 10) to id.',
    )
    Flags.PARSER.add_argument(
        '--cause_of_death_lookup',
        type=str,
        required=True,
        help='File path for the cause of death lookup',
    )
    Flags.InitArgs()

    LOG.info('Reading in CID 9 mapping')
    cid9_df = pd.read_csv(Flags.ARGS.input_cid9)
    LOG.info(cid9_df.head(10))

    # Build cid 9 category lookup
    cid9_df[cid9_df[ID_COL].str.contains('-')].apply(build_cid9_category_lookup, axis=1)

    # Clean up titles and create the category 1 column
    cid9_df = cid9_df[~cid9_df[ID_COL].str.contains('-')]
    cid9_df[TITLE_COL] = cid9_df[TITLE_COL].str.strip()
    cid9_df[CATEGORY_1_COL] = cid9_df[ID_COL].str[:3].replace(CATEGORY_LOOKUP)

    # Create parent column
    cid9_df = build_parent_column(cid9_df, lambda x: x.str.contains('X'))

    LOG.info(cid9_df.head(10))
    LOG.info('Finished processing CID 9 codes')

    LOG.info('Reading in CID 10 mapping')
    cid10_df = separate_title_id(pd.read_csv(Flags.ARGS.input_cid10))
    LOG.info(cid10_df.head(10))

    # Build a lookup from category code range to its text name
    cid_categories = cid10_df[cid10_df[ID_COL].str.contains('-')]
    name_lookup = {
        row[ID_COL]: f'{row[ID_COL]}: {row[TITLE_COL]}'
        for _, row in cid_categories.iterrows()
    }
    # Build a lookup from code to all categories it falls into
    code_to_names = defaultdict(list)
    for range_str in cid_categories[ID_COL]:
        for code in list_cid10_codes_in_range(range_str):
            code_to_names[code].append(name_lookup[range_str])
    code_categories_df = pd.DataFrame.from_dict(
        code_to_names,
        orient='index',
        columns=[CATEGORY_1_COL, CATEGORY_2_COL, CATEGORY_3_COL, CATEGORY_4_COL],
    ).reset_index()

    # Build parent column and clean codes
    cid10_df = cid10_df[~cid10_df[ID_COL].str.contains('-')]
    cid10_df = build_parent_column(
        cid10_df, lambda x: ~x.str.contains('.', regex=False)
    )
    cid10_df[ID_COL] = cid10_df[ID_COL].str.replace('.', '', regex=False)

    # Add in the categories generated above
    cid10_df['short_cid_id'] = cid10_df[ID_COL].str[:3]
    df_len = len(cid10_df)
    cid10_df = cid10_df.merge(
        code_categories_df, left_on='short_cid_id', right_on='index'
    ).drop(columns=['index', 'short_cid_id'])
    assert df_len == len(
        cid10_df
    ), 'Rows dropped when merging in categories, all rows should have at least one category'
    LOG.info(cid10_df.head(10))
    LOG.info('Finished processing CID 10 mapping')

    LOG.info('Combining CID 9 and 10 and outputting')
    lookup_df = pd.concat([cid9_df, cid10_df])
    lookup_df.to_csv(Flags.ARGS.cause_of_death_lookup, index=False)


if __name__ == '__main__':
    sys.exit(main())
