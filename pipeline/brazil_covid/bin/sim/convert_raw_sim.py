#!/usr/bin/env python
import os
import sys

import pandas as pd

from pylib.base.flags import Flags

from log import LOG
from pipeline.brazil_covid.bin.sim.convert_cid_csv import (
    ID_COL as CAUSE_OF_DEATH_CODE,
    CATEGORY_1_COL as CAUSE_OF_DEATH_FIELD,
)
from pipeline.brazil_covid.bin.sim.sim_mappings import (
    COLUMN_MAPPING_VALUES,
    FIELD_COLUMNS,
    INPUT_AGE_COLUMN,
    INPUT_CAUSE_OF_DEATH_COLUMN,
    INPUT_DATE_COLUMN_BEFORE_1996,
    INPUT_DATE_COLUMN_AFTER_1996,
    INPUT_MUNICIPALITY_OCCURRENCE_COLUMN,
    INPUT_NUMERIC_COLUMNS_TO_FIELDS,
    INPUT_REQUIRED_COLUMNS,
    LOCATION_COLUMNS_RENAME,
    RENAME_DIMENSIONS,
)
from util.file.compression.lz4 import LZ4Reader, LZ4Writer
from util.file.file_config import FilePattern

# pylint: disable=no-member

# Only read in 1/5 gb of compressed files at a time.
FILE_SIZE_THRESHOLD = 0.2 * 1024 * 1024 * 1024

OUTPUT_DEATHS_FIELD = '*field_obitos'


# The first digit of the age value is the unit. Convert this to a more human readable form.
def convert_age(age: str) -> str:
    if age == '':
        return ''

    first_digit = age[0]
    age_number = int(age[1:])

    if first_digit == '0':
        return f'{age_number} segundos'
    if first_digit == '1':
        return f'{age_number} minutos'
    if first_digit == '2':
        return f'{age_number} horas'
    if first_digit == '3':
        return f'{age_number} meses'
    if first_digit == '4':
        return f'{age_number} anos'
    if first_digit == '5':
        return f'{age_number + 100} anos'

    return ''


def convert_numerical_columns(
    df: pd.DataFrame, column_name: str, replace_missing_vals: bool, convert_to_int: bool
) -> None:
    if replace_missing_vals:
        # Convert 99 to empty string (missing value)
        df.loc[df[column_name] == '99', column_name] = ''

    if convert_to_int:
        # Convert rows to integers
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce').astype(
            'Int64'
        )


def process_dataframe(
    df: pd.DataFrame, cause_of_death_df: pd.DataFrame, output_file_name: str
) -> None:
    LOG.info('Number of rows in input: %s', len(df))

    # Add any columns that might be missing, but expected
    df = df.reindex(
        df.columns.union(INPUT_REQUIRED_COLUMNS, sort=False), axis=1, fill_value=''
    )

    LOG.info('Beginning date parsing')
    # After 1996, the date column is `DTOBITO` and typically the format is ddmmyyyy.
    df['date'] = pd.to_datetime(
        df.loc[~df[INPUT_DATE_COLUMN_AFTER_1996].isna(), INPUT_DATE_COLUMN_AFTER_1996],
        format='%d%m%Y',
        errors='coerce',
    )
    # If it wasn't in that format, then it was in ?mmyyyy, so assign those to the first of
    # the month.
    row_index = (~df[INPUT_DATE_COLUMN_AFTER_1996].isna()) & df['date'].isna()
    df.loc[row_index, 'date'] = pd.to_datetime(
        df.loc[row_index, INPUT_DATE_COLUMN_AFTER_1996].str[-6:],
        format='%m%Y',
        errors='coerce',
    )
    # If it wasn't in that format, then it was in ?yyyy, so assign those to the first of
    # the year.
    row_index = (~df[INPUT_DATE_COLUMN_AFTER_1996].isna()) & df['date'].isna()
    df.loc[row_index, 'date'] = pd.to_datetime(
        df.loc[row_index, INPUT_DATE_COLUMN_AFTER_1996].str[-4:],
        format='%Y',
        errors='coerce',
    )

    # Before 1996, the date column is `DATAOBITO` and typically the format is yymmdd.
    row_index = (~df[INPUT_DATE_COLUMN_BEFORE_1996].isna()) & df['date'].isna()
    df.loc[row_index, 'date'] = pd.to_datetime(
        df.loc[row_index, INPUT_DATE_COLUMN_BEFORE_1996],
        format='%y%m%d',
        errors='coerce',
    )
    # If it wasn't in that format, then it was in yymm?, so assign those to the first of
    # the month.
    row_index = (~df[INPUT_DATE_COLUMN_BEFORE_1996].isna()) & df['date'].isna()
    df.loc[row_index, 'date'] = pd.to_datetime(
        df.loc[row_index, INPUT_DATE_COLUMN_BEFORE_1996].str[:4],
        format='%y%m',
        errors='coerce',
    )
    # If it wasn't in that format, then it was in yy?. Take the first two characters
    # for the year and make it the first of the year.
    row_index = (~df[INPUT_DATE_COLUMN_BEFORE_1996].isna()) & df['date'].isna()
    df.loc[row_index, 'date'] = pd.to_datetime(
        df.loc[row_index, INPUT_DATE_COLUMN_BEFORE_1996].str[:2],
        format='%y',
        errors='coerce',
    )

    # No rows should have been dropped, but log and filter just in case.
    LOG.info(
        'Number of rows dropped with invalid dates: %s', len(df[df['date'].isna()])
    )
    df = df[~df['date'].isna()].drop(
        columns=[INPUT_DATE_COLUMN_BEFORE_1996, INPUT_DATE_COLUMN_AFTER_1996]
    )
    len_df = len(df)
    LOG.info('Finished date parsing')

    LOG.info('Starting remapping column values to human readable strings')
    for column_name, mapping_values in COLUMN_MAPPING_VALUES.items():
        df[column_name] = df[column_name].replace(mapping_values)

    # Convert age to human readable
    df[INPUT_AGE_COLUMN] = df[INPUT_AGE_COLUMN].apply(convert_age)

    # Convert numerical columns
    convert_numerical_columns(df, 'IDADEMAE', False, True)
    convert_numerical_columns(df, 'QTDFILVIVO', True, True)
    convert_numerical_columns(df, 'QTDFILMORT', True, True)
    convert_numerical_columns(df, 'SEMAGESTAC', True, False)
    LOG.info('Finished remapping column values')

    LOG.info('Starting processing cause of death')
    LOG.info(df[INPUT_CAUSE_OF_DEATH_COLUMN].unique())

    unmatched_cid_ids = df.loc[
        ~df[INPUT_CAUSE_OF_DEATH_COLUMN].isin(cause_of_death_df[CAUSE_OF_DEATH_CODE]),
        INPUT_CAUSE_OF_DEATH_COLUMN,
    ]
    LOG.info('Unmatched CID IDs: %s', unmatched_cid_ids.unique())
    LOG.info('For those %s rows, using parent CID', len(unmatched_cid_ids))
    df.loc[
        ~df[INPUT_CAUSE_OF_DEATH_COLUMN].isin(cause_of_death_df[CAUSE_OF_DEATH_CODE]),
        INPUT_CAUSE_OF_DEATH_COLUMN,
    ] = df[INPUT_CAUSE_OF_DEATH_COLUMN].str[:3]
    df = df.merge(
        cause_of_death_df,
        left_on=INPUT_CAUSE_OF_DEATH_COLUMN,
        right_on=CAUSE_OF_DEATH_CODE,
    ).drop(columns=[CAUSE_OF_DEATH_CODE, INPUT_CAUSE_OF_DEATH_COLUMN])
    assert len(df) == len_df, 'Rows dropped after merging on cause of death'
    LOG.info('Finished processing cause of death')

    LOG.info('Building numeric field columns')
    # Build a unique numeric field for each dimension value in all the columns that we care
    # about. This will allow the user to query for a specific value without grouping.
    for column, dimension_mapping in FIELD_COLUMNS.items():
        for dimension_value in dimension_mapping.values():
            # If there is no value for this dimension in the dataframe, we don't need to
            # create a field for it.
            if not dimension_value:
                continue

            field_name = f'*field_{column} - {dimension_value}'  # 'ESCMAE2010 - Sem escolaridade'
            df_row_filter = (
                df[column] == dimension_value
            )  # df['ESCMAE2010'] == 'Sem escolaridade'

            # Initialize the field to na since all rows that do not match the dimension value
            # should not have a field value.
            df[field_name] = pd.NA

            # Set all rows that match the filter to 1.
            df.loc[df_row_filter, field_name] = 1

    # Build fields for numeric columns
    for column in INPUT_NUMERIC_COLUMNS_TO_FIELDS:
        field_name = f'*field_{column}'
        df[field_name] = df[column]

    # Build fields for cause of death indicators. As there are many causes of death,
    # only convert the top level cause of death categories to be fields.
    for dimension_value in df[CAUSE_OF_DEATH_FIELD].unique():
        field_name = f'*field_{CAUSE_OF_DEATH_FIELD} - {dimension_value}'
        df_row_filter = df[CAUSE_OF_DEATH_FIELD] == dimension_value
        df[field_name] = pd.NA
        df.loc[df_row_filter, field_name] = 1

    # Add a field for all deaths
    df[OUTPUT_DEATHS_FIELD] = 1
    LOG.info('Finished building numeric field columns')

    LOG.info('Renaming columns')
    df = df.rename(columns=RENAME_DIMENSIONS)
    LOG.info('Finished renaming columns')

    LOG.info('Writing the output CSV')
    with LZ4Writer(output_file_name) as output_file:
        df.to_csv(output_file, index=False)
    LOG.info('Finished writing output CSV')


def main():
    Flags.PARSER.add_argument(
        '--input_folder',
        type=str,
        required=True,
        help='Input folder with years of raw SIM files to convert',
    )
    Flags.PARSER.add_argument(
        '--output_file_pattern', type=str, required=True, help='Converted file location'
    )
    Flags.PARSER.add_argument(
        '--cause_of_death_codes_csv',
        type=str,
        required=True,
        help='File path for cause of death codes to fields',
    )
    Flags.InitArgs()

    output_file_pattern = FilePattern(Flags.ARGS.output_file_pattern)

    LOG.info('Starting cause of death load')
    cause_of_death_df = pd.read_csv(Flags.ARGS.cause_of_death_codes_csv)
    LOG.info(cause_of_death_df.head(10))

    LOG.info('Reading in input files into dataframe')
    df = pd.DataFrame()
    total_file_size_counter = 0
    count = 0
    for input_file_name in os.listdir(Flags.ARGS.input_folder):
        if input_file_name.endswith('.csv.lz4'):
            file_name = os.path.join(Flags.ARGS.input_folder, input_file_name)
            file_size = os.stat(file_name).st_size
            if total_file_size_counter + file_size > FILE_SIZE_THRESHOLD:
                LOG.info('Processing previous data and creating new dataframe')
                # Process the current dataframe
                process_dataframe(
                    df, cause_of_death_df, output_file_pattern.build(str(count))
                )
                # Create a new one
                total_file_size_counter = 0
                count += 1
                df = pd.DataFrame()

            LOG.info('Processing file %s', input_file_name)
            total_file_size_counter += file_size
            with LZ4Reader(file_name) as input_file:
                input_df = pd.read_csv(
                    input_file,
                    sep=';',
                    dtype=str,
                    keep_default_na=False,
                    usecols=lambda col: col in INPUT_REQUIRED_COLUMNS
                    or col in LOCATION_COLUMNS_RENAME.keys(),
                )

                # Pre 1996, the municipality columns had different names
                if INPUT_MUNICIPALITY_OCCURRENCE_COLUMN not in input_df.columns:
                    input_df = input_df.rename(columns=LOCATION_COLUMNS_RENAME)

                df = df.append(input_df, ignore_index=True)
    LOG.info('Finished reading input files')

    LOG.info('Processing final dataframe')
    process_dataframe(df, cause_of_death_df, output_file_pattern.build(str(count)))

    return 0


if __name__ == '__main__':
    sys.exit(main())
