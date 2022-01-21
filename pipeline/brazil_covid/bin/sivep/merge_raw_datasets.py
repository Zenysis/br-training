#!/usr/bin/env python
import sys

import pandas as pd

from pylib.base.flags import Flags

from log import LOG
from util.file.compression.pigz import PigzWriter


def main():
    Flags.PARSER.add_argument(
        '--sivep_2019', type=str, required=True, help='SIVEP 2019 CSV'
    )
    Flags.PARSER.add_argument(
        '--sivep_2020', type=str, required=True, help='SIVEP 2020 CSV'
    )
    Flags.PARSER.add_argument(
        '--sivep_2021', type=str, required=True, help='SIVEP 2021 CSV'
    )
    Flags.PARSER.add_argument(
        '--output_file', type=str, required=True, help='Merged output CSV to write.'
    )
    Flags.InitArgs()

    # Read the CSV files with all columns as strings. This allows us to perform a more
    # straightforward and safe transformation since Pandas won't try to infer datatypes.
    LOG.info('Reading 2019 dataset')

    # NOTE(stephen): The latin-1 encoding is used only for the 2019 dataset.
    df_2019 = pd.read_csv(
        Flags.ARGS.sivep_2019,
        sep=';',
        dtype=str,
        keep_default_na=False,
        encoding='latin-1',
    )
    LOG.info('Number of 2019 rows read: %s', len(df_2019))

    # Reading 2020 dataset
    LOG.info('Reading 2020 dataset')
    df_2020 = pd.read_csv(
        Flags.ARGS.sivep_2020, sep=';', dtype=str, keep_default_na=False
    )
    LOG.info('Number of 2020 rows read: %s', len(df_2020))

    # Reading 2021 dataset
    LOG.info('Reading 2021 dataset')
    df_2021 = pd.read_csv(
        Flags.ARGS.sivep_2021, sep=';', dtype=str, keep_default_na=False
    )
    LOG.info('Number of 2021 rows read: %s', len(df_2021))

    LOG.info('Building merged dataframe')
    output_columns = []
    for column in df_2021.columns:
        # Preference the 2021 column order over the 2020 column order.
        output_columns.append(column)

        # Add any missing columns to the 2020 dataframe with a default empty value.
        if column not in df_2020.columns:
            df_2020[column] = ''

        # Add any missing columns to the 2019 dataframe with a default empty value.
        if column not in df_2019.columns:
            df_2019[column] = ''

    for column in df_2020.columns:
        if column not in df_2021.columns:
            # Preference the 2020 column order over the 2019 column order.
            output_columns.append(column)

            # Ensure the 2021 dataframe also has an empty column added if it exists only
            # in the 2020 dataset.
            df_2021[column] = ''

            # Add any missing columns to the 2019 dataframe with a default empty value.
            if column not in df_2019.columns:
                df_2019[column] = ''

    # Add in any columns from 2019 that are not in the 2020 dataframe.
    for column in df_2019.columns:
        if column not in df_2021.columns and column not in df_2020.columns:
            output_columns.append(column)

            # Ensure the 2021 dataframe also has an empty column added if it exists only
            # in the 2019 dataset.
            df_2021[column] = ''

            # Ensure the 2020 dataframe also has an empty column added if it exists only
            # in the 2019 dataset.
            df_2020[column] = ''

    LOG.info('Output columns: %s', output_columns)
    merged_dataframe = df_2021[output_columns].append(
        df_2020[output_columns].append(df_2019[output_columns])
    )

    LOG.info('Writing merged CSV. Number of rows: %s', len(merged_dataframe))
    with PigzWriter(Flags.ARGS.output_file) as output_file:
        merged_dataframe.to_csv(output_file, sep=';', index=False)

    LOG.info('Successfully wrote merged CSV')
    return 0


if __name__ == '__main__':
    sys.exit(main())
