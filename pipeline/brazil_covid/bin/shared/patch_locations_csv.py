#!/usr/bin/env python
# Update the pre-matching locations.csv file for certain sources to find the correct
# canonical hierarchy based on the Municipality Code that is stored in the Municipality
# column for that source.
from collections import defaultdict
import csv
import sys

from pylib.base.flags import Flags

from config.br_covid.datatypes import DimensionFactoryType
from log import LOG
from util.file.file_config import FileConfig, FilePattern, validate_file_configs


# This is a bit of a hack because it does not enforce the dictionary doesn't change, it
# relies on the user not editing it. But this needed a hashable dictionary and it doesn't
# change.
class HashableDict(dict):
    def __key(self):
        return tuple((k, self[k]) for k in sorted(self))

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        # pylint: disable=protected-access
        return self.__key() == other.__key()


def process_source(unmatched_filename, matched_filename):
    # Map the "clean" field to the original "raw" fields that are needed when the
    # "fill dimension data" step tries to merge the matching results with the original
    # output rows. Due to fixes for missing municipality codes, multiple codes can map
    # to the same canonical municipality so use a set here.
    original_dimension_map = defaultdict(set)
    with open(unmatched_filename) as unmatched_file:
        reader = csv.DictReader(unmatched_file)
        for row in reader:
            row_id_fields = []
            result = {}
            for dimension in DimensionFactoryType.hierarchical_dimensions:
                clean_key = f'{DimensionFactoryType.clean_prefix}{dimension}'
                result[clean_key] = row[f'{DimensionFactoryType.raw_prefix}{dimension}']
                row_id_fields.append(row[clean_key])

            row_id = '__'.join(row_id_fields)
            original_dimension_map[row_id].add(HashableDict(result))

    # Read all the canonical matching rows and build what the new output row should be.
    output_rows = []
    header = []
    with open(matched_filename) as matched_file:
        reader = csv.DictReader(matched_file)
        header = reader.fieldnames
        for row in reader:
            row_id = '__'.join(
                row[f'{DimensionFactoryType.clean_prefix}{dimension}']
                for dimension in DimensionFactoryType.hierarchical_dimensions
            )
            # Create a new row per cleaned value this canonical maps to
            for result in original_dimension_map[row_id]:
                output_rows.append({**row, **result})

    # Replace the canonical matching file *in place* with our hacked update.
    with open(matched_filename, 'w') as matched_file:
        writer = csv.DictWriter(matched_file, fieldnames=header)
        writer.writeheader()
        writer.writerows(output_rows)


def main():
    Flags.PARSER.add_argument(
        '--unmatched_basedir_pattern',
        type=str,
        required=True,
        help='Pattern to use to find unmatched locations.csv dimension files for '
        'individual sources',
    )
    Flags.PARSER.add_argument(
        '--matched_basedir_pattern',
        type=str,
        required=True,
        help='Pattern to use to find mapped_locations.csv dimension files for '
        'individual sources',
    )
    Flags.PARSER.add_argument(
        '--sources',
        nargs='+',
        type=str,
        required=True,
        help='List of sources to process',
    )
    Flags.InitArgs()

    # NOTE(stephen): The original mapped_locations.csv file will be overwritten *in
    # place* for the sources provided.
    unmatched_pattern = FilePattern(Flags.ARGS.unmatched_basedir_pattern)
    matched_pattern = FilePattern(Flags.ARGS.matched_basedir_pattern)
    file_configs = FileConfig.build_multiple_from_pattern(
        unmatched_pattern, matched_pattern, Flags.ARGS.sources
    )
    validate_file_configs(file_configs)

    for file_config in file_configs:
        LOG.info('Processing %s', file_config.name)
        process_source(file_config.input_file, file_config.output_file)
    LOG.info('Finished processing sources')

    return 0


if __name__ == '__main__':
    sys.exit(main())
