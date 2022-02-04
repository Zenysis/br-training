#!/usr/bin/env python
import sys

import pandas as pd
import json
from pylib.base.flags import Flags
from slugify import slugify

from log import LOG

ALPHABET_LOCATION = {letter: i for i, letter in enumerate('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}
ALPHABET_LOOKUP = {i: letter for i, letter in enumerate('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}

CID_USED = set()


def create_ids(cid_str):
    start_str, end_str = cid_str.split('-')
    start_l, start_n, start_sn = format_cid(start_str)
    end_l, end_n, end_sn = format_cid(end_str, end=True)
    start = CID(start_l, start_n, start_sn)
    end = CID(end_l, end_n, end_sn)
    indicators = []
    while (
        less_than(start, end, inclusive=True) and ALPHABET_LOCATION[start.letter] < 25
    ):
        current_id = start.to_id()
        if current_id not in CID_USED:
            CID_USED.add(current_id)
            indicators.append(current_id)
        start = start.add_one()
    return indicators


def less_than(start_CID, end_CID, inclusive=True):
    if ALPHABET_LOCATION[start_CID.letter] < ALPHABET_LOCATION[end_CID.letter]:
        return True
    elif ALPHABET_LOCATION[start_CID.letter] == ALPHABET_LOCATION[end_CID.letter]:
        if start_CID.number < end_CID.number:
            return True
        elif start_CID.number == end_CID.number:
            if start_CID.subnumber < end_CID.subnumber:
                return True
            elif start_CID.subnumber == end_CID.subnumber:
                return inclusive
            else:
                return False
        else:
            return False
    else:
        return False


def format_cid(cid, end=False):
    letter = cid[0]
    if '.' in cid[1:]:
        number, subnumber = cid[1:].split('.')
    else:
        number = cid[1:]
        subnumber = 0 if not end else 9
    return letter, int(number), int(subnumber)


class CID:
    def __init__(self, letter, number, subnumber):
        self.letter = letter
        self.number = number
        self.subnumber = subnumber

    def to_id(self):
        number = str(self.number)
        if len(number) < 2:
            number = f'0{number}'
        return f'{self.letter.lower()}{number}{self.subnumber}'

    def add_one(self):
        if self.subnumber < 9:
            return CID(self.letter, self.number, self.subnumber + 1)
        elif self.number < 99:
            return CID(self.letter, self.number + 1, 0)
        else:
            return CID(ALPHABET_LOOKUP[ALPHABET_LOCATION[self.letter] + 1], 0, 0)


def separate_name_id(dataframe):
    output = []
    for row in dataframe['cid']:
        cid, name = row.split(')', 1)
        cid = cid.replace('(', '')
        output.append({'cid': cid, 'name': name})
    return pd.DataFrame(output)


def create_group_mapping(row):
    group_mapping = {
        'name': row['name'],
        'id': row['cid'].lower().replace('(', '').replace('.', ''),
        'slug': slugify(row['name'], separator='_'),
    }
    indicator_ids = create_ids(row['cid'])
    group_mapping['indicators'] = indicator_ids
    return group_mapping


def main():
    Flags.PARSER.add_argument(
        '--input_file',
        type=str,
        required=True,
        help='Raw csv lookup of cause of death to id.',
    )
    Flags.PARSER.add_argument(
        '--output_indicator_groups',
        type=str,
        required=True,
        help='File path for the cause of death indicators',
    )
    Flags.PARSER.add_argument(
        '--cause_of_death_lookup',
        type=str,
        required=True,
        help='File path for the cause of death lookup',
    )
    Flags.InitArgs()
    cause_of_death_df = separate_name_id(pd.read_csv(Flags.ARGS.input_file))
    LOG.info(cause_of_death_df.head(10))

    groups = []
    for i, row in cause_of_death_df.iterrows():
        if '-' in row['cid']:
            format_group = create_group_mapping(row)
            if len(format_group) > 0:
                groups.append(format_group)

    cause_of_death_by_death_location_indicators = [
        {'id': f"sim_occurrence_cause_of_death_{g['slug']}", 'text': g['name'].strip()}
        for g in groups
    ]
    cause_of_death_by_death_location_group = {
        'groupId': 'sim_occurrence_cause_of_death',
        'groupText': 'Causa da Morte (by Death Location)',
        'indicators': cause_of_death_by_death_location_indicators,
    }
    cause_of_death_by_residence_location_indicators = [
        {'id': f"sim_residence_cause_of_death_{g['slug']}", 'text': g['name'].strip()}
        for g in groups
    ]
    cause_of_death_by_residence_location_group = {
        'groupId': 'sim_residence_cause_of_death',
        'groupText': 'Causa da Morte (by Residence Location)',
        'indicators': cause_of_death_by_residence_location_indicators,
    }

    json_data = json.dumps(
        [
            cause_of_death_by_death_location_group,
            cause_of_death_by_residence_location_group,
        ],
        indent=4,
    )
    with open(Flags.ARGS.output_indicator_groups, 'w') as output_json:
        output_json.write(json_data)

    lookup = [
        {'cid_id': cid_id, 'field_id': g['slug']}
        for g in groups
        for cid_id in g['indicators']
    ]
    pd.DataFrame(lookup).to_csv(Flags.ARGS.cause_of_death_lookup, index=False)


if __name__ == '__main__':
    sys.exit(main())
