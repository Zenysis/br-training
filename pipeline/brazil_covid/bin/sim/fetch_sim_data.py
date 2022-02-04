#!/usr/bin/env python
import os
import sys

from pylib.base.flags import Flags
import requests

from log import LOG
from util.file.compression.lz4 import LZ4Writer


# URLs from https://opendatasus.saude.gov.br/dataset/sim-1979-2019
def get_url(year: int) -> str:
    return f'https://diaad.s3.sa-east-1.amazonaws.com/sim/Mortalidade_Geral_{year}.csv'


def main():
    Flags.PARSER.add_argument(
        '--output_folder',
        type=str,
        required=True,
        help='Output folder for each year of SIM data',
    )
    Flags.PARSER.add_argument(
        '--start_year',
        type=int,
        required=True,
        help='First year there is data to fetch',
    )
    Flags.PARSER.add_argument(
        '--end_year',
        type=int,
        required=True,
        help='Last year there is data to fetch (inclusive)',
    )
    Flags.InitArgs()

    current_year = Flags.ARGS.start_year
    end_year = Flags.ARGS.end_year
    output_folder = Flags.ARGS.output_folder

    while current_year <= end_year:
        # Fetch the url
        url = get_url(current_year)
        LOG.info('Fetching data from %s at url: %s', current_year, url)
        response = requests.get(url)
        response.raise_for_status()

        # Write the data to a file
        output_file_name = os.path.join(output_folder, f'sim_{current_year}.csv.lz4')
        with LZ4Writer(output_file_name) as writer:
            writer.write(response.text)

        current_year += 1

    LOG.info('Successfully fetched all years')

    return 0


if __name__ == '__main__':
    sys.exit(main())
