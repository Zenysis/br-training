#!/usr/bin/env python
# Update the pre-matching locations.csv file for certain sources to find the correct
# canonical hierarchy based on the Municipality Code that is stored in the Municipality
# column for that source.
import csv
import sys

from pylib.base.flags import Flags

from config.br_covid.datatypes import Dimension, DimensionFactoryType
from log import LOG
from util.file.file_config import FileConfig, FilePattern, validate_file_configs

MUNICIPALITY_COLUMN = f'{DimensionFactoryType.clean_prefix}{Dimension.MUNICIPALITY}'


def build_municipality_code_lookup(filename):
    '''Build a mapping from municipality code to the location dict that should be used
    for that code.
    '''
    # NOTE(stephen): BR has two municipality codes that we need to test for.
    short_code_mapping = {}
    long_code_mapping = {}
    with open(filename) as input_file:
        reader = csv.DictReader(input_file)
        for row in reader:
            # NOTE(stephen): Storing the clean-prefixed dimension name since that's what
            # the input source files will use.
            locations = {
                f'{DimensionFactoryType.clean_prefix}{dimension}': row[dimension]
                for dimension in DimensionFactoryType.hierarchical_dimensions
            }
            short_code_mapping[row['MunicipalityCodeShort']] = locations
            long_code_mapping[row['MunicipalityCodeLong']] = locations
    return (short_code_mapping, long_code_mapping)


def process_source(filename, short_code_mapping, long_code_mapping):
    output_rows = []
    header = []
    with open(filename) as input_file:
        reader = csv.DictReader(input_file)
        header = reader.fieldnames
        for row in reader:
            municipality_code = row[MUNICIPALITY_COLUMN]
            location = short_code_mapping.get(
                municipality_code
            ) or long_code_mapping.get(municipality_code)

            if not location:
                LOG.warning(
                    'Cannot find location data for municipality code: %s',
                    municipality_code,
                )
                location = {}
            output_rows.append({**row, **location})

    with open(filename, 'w') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=header)
        writer.writeheader()
        writer.writerows(output_rows)


def main():
    Flags.PARSER.add_argument(
        '--input_basedir_pattern',
        type=str,
        required=True,
        help='Pattern to use to find dimension files for individual sources to '
        'match.',
    )
    Flags.PARSER.add_argument(
        '--municipality_code_mapping_file',
        type=str,
        required=True,
        help='File with municpality codes mapping to full location hiearchy',
    )
    Flags.PARSER.add_argument(
        '--sources',
        nargs='+',
        type=str,
        required=True,
        help='List of sources to process',
    )
    Flags.InitArgs()

    # NOTE(stephen): The original locations.csv file will be overwritten *in place* for
    # the sources provided.
    input_pattern = FilePattern(Flags.ARGS.input_basedir_pattern)
    (short_code_mapping, long_code_mapping) = build_municipality_code_lookup(
        Flags.ARGS.municipality_code_mapping_file
    )

    for source in Flags.ARGS.sources:
        LOG.info('Processing %s', source)
        process_source(
            input_pattern.build(source), short_code_mapping, long_code_mapping
        )
    LOG.info('Finished processing sources')

    return 0


if __name__ == '__main__':
    sys.exit(main())
