#!/usr/bin/env python
import math
import os
import sys
from typing import Dict, List, Tuple

import pandas as pd

from pylib.base.flags import Flags

from config.br_covid.datatypes import Dimension
from log import LOG
from pipeline.brazil_covid.bin.sim.convert_cid_csv import ID_COL as CAUSE_OF_DEATH_CODE
from pipeline.brazil_covid.bin.sim.convert_occupation_codes import (
    CODE_COLUMN as OCCUPATION_CODE_COLUMN,
)
from pipeline.brazil_covid.bin.sim.sim_mappings import (
    CAUSE_OF_DEATH_COLUMNS_TO_RENAME,
    CAUSE_OF_DEATH_FIELDS,
    COLUMN_MAPPING_VALUES,
    FIELD_COLUMNS,
    INPUT_AGE_COLUMN,
    INPUT_DATE_COLUMN_AFTER_1996,
    INPUT_DATE_COLUMN_BEFORE_1996,
    INPUT_MOTHER_OCCUPATION_COLUMN,
    INPUT_NUMERIC_COLUMNS_TO_PLACEHOLDERS,
    INPUT_OCCUPATION_COLUMN,
    INPUT_REQUIRED_COLUMNS,
    MAX_NUMBER_OF_CODES_PER_CAUSE_OF_DEATH_COL,
    MOTHERS_OCCUPATION_COLUMNS_RENAME,
    OCCUPATION_COLUMNS_RENAME,
    PRE_1996_COLUMNS_RENAME,
    RENAME_DIMENSIONS,
)
from util.file.compression.lz4 import LZ4Reader, LZ4Writer
from util.file.file_config import FilePattern

# pylint: disable=no-member

# Only read in 5 million lines at a time.
LINE_THRESHOLD = 5000000

OUTPUT_DEATHS_FIELD = '*field_obitos'


# The first digit of the age value is the unit. Convert this to a more human readable form.
# Also return the age group, which is created to align with other integrations.
def convert_age(age: str) -> Tuple[str, str]:
    # NOTE(abby): There is a very small number of invalid values, just handle them here.
    if age == '1 d':
        return '24 horas', '0-9'
    if age == '0':
        return '0 segundos', '0-9'

    if age != '':
        first_digit = age[0]
        age_number = int(age[1:])

        if first_digit == '0':
            return f'{age_number} segundos', '0-9'
        if first_digit == '1':
            return f'{age_number} minutos', '0-9'
        if first_digit == '2':
            return f'{age_number} horas', '0-9'
        if first_digit == '3':
            return f'{age_number} meses', '0-9'
        if first_digit == '4':
            if age_number >= 80:
                return f'{age_number} anos', '80+'
            age_group = math.floor(age_number / 10) * 10
            return f'{age_number} anos', f'{age_group}-{age_group+9}'
        if first_digit == '5':
            return f'{age_number + 100} anos', '80+'

    return '', 's/informação'


def convert_numerical_columns(
    df: pd.DataFrame, column_name: str, replace_placeholder_values: List[str]
) -> None:
    # Convert missing value placeholders to empty strings
    LOG.info(
        'Removing %s placeholder values from column %s',
        len(df[df[column_name].isin(replace_placeholder_values)]),
        column_name,
    )
    for placeholder_value in replace_placeholder_values:
        df.loc[df[column_name] == placeholder_value, column_name] = ''

    # Convert rows to integers
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce').astype('Int64')


# TODO(abby): Can this be faster?
def process_cause_of_death(
    df: pd.DataFrame,
    cause_of_death_df: pd.DataFrame,
    column_name: str,
    columns_rename: Dict[str, str],
) -> pd.DataFrame:
    LOG.info('Processing cause of death column %s', column_name)
    # There may be multiple causes of death in each column, split them out into new columns.
    split_columns_rename = {
        i: f'{column_name}{i}'
        for i in range(MAX_NUMBER_OF_CODES_PER_CAUSE_OF_DEATH_COL[column_name])
    }

    # Post 1996, the codes are prefixed with a '*', remove only the first one
    # so they can be used to split the codes into different columns. Strip out
    # the period to convert codes like 'A00.0' -> 'A000' to match the formatting
    # of the lookup file. Uppercase the text to match lookup file.
    df[column_name] = (
        df[column_name]
        .str.replace('*', '', regex=False, n=1)
        .str.replace('.', '', regex=False)
        .str.upper()
    )
    df = df.merge(
        df[column_name]
        .str.split('*', expand=True)
        .apply(lambda col: col.str.strip())
        .rename(columns=split_columns_rename),
        left_index=True,
        right_index=True,
    ).drop(columns=[column_name])

    known_codes = set(cause_of_death_df.index)
    for i, column in split_columns_rename.items():
        # If no rows have the maximum number of codes for any input column, then
        # some of these columns may not be present in the dataframe.
        if column not in df.columns:
            LOG.info('Skipping column %s', column)
            continue

        # For unmatched causes of death, try using only the first 3 digits to match
        # the parent.
        df.loc[~df[column].isin(known_codes), column] = df[column].str[:3]
        remaining_unmatched_count = (
            ~df[column].isna() & (df[column] != '') & ~df[column].isin(known_codes)
        ).sum()
        if remaining_unmatched_count > 0:
            LOG.info(
                'Remaining rows with unmatched CID IDs: %s', remaining_unmatched_count
            )

        df = (
            df.merge(
                cause_of_death_df,
                left_on=column,
                right_index=True,
                how='left',
            ).drop(columns=[column])
            # Rename the merged columns to be unique since the input column is split into
            # multiple columns as a multi value dimension.
            .rename(
                columns={key: f'{value}_{i}' for key, value in columns_rename.items()}
            )
        )

    return df


def clean_and_replace_occupation_column(
    df: pd.DataFrame,
    occupation_df: pd.DataFrame,
    input_column: str,
    column_list: List[str],
) -> pd.DataFrame:
    # Some rows have punctuation in the code, remove them.
    df.loc[
        ~df[input_column].str.isnumeric() & (df[input_column] != ''), input_column
    ] = ''
    # Convert the unknown 5 digit codes to 3 digit ones.
    known_codes = set(occupation_df.index)
    unmatched_rows = (
        (df[input_column] != '')
        & ~df[input_column].isin(known_codes)
        & (df[input_column].str.len() == 5)
    )
    df.loc[unmatched_rows, input_column] = df.loc[unmatched_rows, input_column].str[:3]

    # Use the occupation lookup file to convert from occupation code to title.
    df = df.merge(
        occupation_df,
        left_on=input_column,
        right_index=True,
        how='left',
    )
    # For rows that have an occupation but it can't be matched, log the values then
    # set the value to "unknown occupation".
    title_column = column_list[0]
    missing_rows_index = df[title_column].isna() & (df[input_column] != '')
    LOG.info(
        '%s %s rows had codes that could not be matched. Unmatched codes: \n %s',
        missing_rows_index.sum(),
        input_column,
        df[missing_rows_index][input_column].unique(),
    )
    for column in column_list:
        df.loc[missing_rows_index, column] = 'Ocupação desconhecida'
    df = df.drop(columns=[input_column])

    return df


def process_dataframe(
    df: pd.DataFrame,
    cause_of_death_df: pd.DataFrame,
    occupation_df: pd.DataFrame,
    output_file_name: str,
) -> None:
    LOG.info('Number of rows in input: %s', len(df))

    # Add any columns that might be missing, but expected
    df = df.reindex(
        df.columns.union(INPUT_REQUIRED_COLUMNS, sort=False), axis=1, fill_value=''
    )

    LOG.info('Beginning date parsing')
    # After 1996, the date column is `DTOBITO` and typically the format is ddmmyyyy.
    df['date'] = pd.to_datetime(
        df.loc[df[INPUT_DATE_COLUMN_AFTER_1996] != '', INPUT_DATE_COLUMN_AFTER_1996],
        format='%d%m%Y',
        errors='coerce',
    )
    # If it wasn't in that format, then it was in ?mmyyyy, so assign those to the first of
    # the month.
    row_index = (df[INPUT_DATE_COLUMN_AFTER_1996] != '') & df['date'].isna()
    df.loc[row_index, 'date'] = pd.to_datetime(
        df.loc[row_index, INPUT_DATE_COLUMN_AFTER_1996].str[-6:],
        format='%m%Y',
        errors='coerce',
    )
    # If it wasn't in that format, then it was in ?yyyy, so assign those to the first of
    # the year.
    row_index = (df[INPUT_DATE_COLUMN_AFTER_1996] != '') & df['date'].isna()
    df.loc[row_index, 'date'] = pd.to_datetime(
        df.loc[row_index, INPUT_DATE_COLUMN_AFTER_1996].str[-4:],
        format='%Y',
        errors='coerce',
    )

    # Before 1996, the date column is `DATAOBITO` and typically the format is yymmdd.
    row_index = (df[INPUT_DATE_COLUMN_BEFORE_1996] != '') & df['date'].isna()
    df.loc[row_index, 'date'] = pd.to_datetime(
        df.loc[row_index, INPUT_DATE_COLUMN_BEFORE_1996],
        format='%y%m%d',
        errors='coerce',
    )
    # If it wasn't in that format, then it was in yymm?, so assign those to the first of
    # the month.
    row_index = (df[INPUT_DATE_COLUMN_BEFORE_1996] != '') & df['date'].isna()
    df.loc[row_index, 'date'] = pd.to_datetime(
        df.loc[row_index, INPUT_DATE_COLUMN_BEFORE_1996].str[:4],
        format='%y%m',
        errors='coerce',
    )
    # If it wasn't in that format, then it was in yy?. Take the first two characters
    # for the year and make it the first of the year.
    row_index = (df[INPUT_DATE_COLUMN_BEFORE_1996] != '') & df['date'].isna()
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
    LOG.info('Finished date parsing')

    LOG.info('Starting remapping column values to human readable strings')
    for column_name, mapping_values in COLUMN_MAPPING_VALUES.items():
        df[column_name] = df[column_name].replace(mapping_values)

    # Convert age to be human readable and populate the age group dimension
    df[INPUT_AGE_COLUMN], df[Dimension.AGE_GROUP] = zip(
        *df[INPUT_AGE_COLUMN].map(convert_age)
    )

    # Convert numerical columns
    for column_name, placeholders in INPUT_NUMERIC_COLUMNS_TO_PLACEHOLDERS.items():
        convert_numerical_columns(df, column_name, placeholders)
    LOG.info('Finished remapping column values')

    LOG.info('Starting processing cause of death')
    for column_name, columns_rename in CAUSE_OF_DEATH_COLUMNS_TO_RENAME.items():
        df = process_cause_of_death(df, cause_of_death_df, column_name, columns_rename)
    LOG.info('Finished processing cause of death')

    LOG.info('Starting processing occupation columns')
    df = clean_and_replace_occupation_column(
        df,
        occupation_df.rename(columns=OCCUPATION_COLUMNS_RENAME),
        INPUT_OCCUPATION_COLUMN,
        list(OCCUPATION_COLUMNS_RENAME.values()),
    )
    df = clean_and_replace_occupation_column(
        df,
        occupation_df.rename(columns=MOTHERS_OCCUPATION_COLUMNS_RENAME),
        INPUT_MOTHER_OCCUPATION_COLUMN,
        list(MOTHERS_OCCUPATION_COLUMNS_RENAME.values()),
    )
    LOG.info('Finished processing occupation columns')

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
    for column in INPUT_NUMERIC_COLUMNS_TO_PLACEHOLDERS.keys():
        field_name = f'*field_{column}'
        df[field_name] = df[column]

    # Add a field for all deaths
    df[OUTPUT_DEATHS_FIELD] = 1
    LOG.info('Finished building numeric field columns')

    LOG.info('Building cause of death field columns')
    # Build fields for cause of death indicators. As there are many causes of death,
    # only convert the top level cause of death categories (Category 1) to be fields.
    # However, there can be multiple causes of death (it's a multi value dimension),
    # so use all Category 1 columns. Using `pivot_table` here to improve performance.
    cause_of_death_fields_df = pd.DataFrame()
    for field in CAUSE_OF_DEATH_FIELDS:
        # If no rows have the maximum number of codes for the primary cause of death,
        # then some of these columns may not be present in the dataframe.
        if field not in df.columns:
            LOG.info('Skipping using column %s', field)
            continue

        pivoted_fields = df.pivot_table(index=df.index, columns=field, aggfunc='size')
        cause_of_death_fields_df = cause_of_death_fields_df.add(
            pivoted_fields, fill_value=0
        )

    cause_of_death_fields_df = cause_of_death_fields_df.clip(upper=1).rename(
        columns=lambda col: f'*field_cause_of_death_category1 - {col}'
    )
    df = df.merge(
        cause_of_death_fields_df,
        left_index=True,
        right_index=True,
    )
    LOG.info('Finished cause of death field columns')

    LOG.info('Renaming columns')
    df = df.rename(columns=RENAME_DIMENSIONS)
    LOG.info('Finished renaming columns')

    LOG.info('Writing the output CSV')
    # Since these are large files, use high compression
    with LZ4Writer(output_file_name, level=9) as output_file:
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
    Flags.PARSER.add_argument(
        '--occupation_codes_csv',
        type=str,
        required=True,
        help='File path for occupation codes lookup',
    )
    Flags.InitArgs()

    output_file_pattern = FilePattern(Flags.ARGS.output_file_pattern)

    LOG.info('Starting cause of death load')
    cause_of_death_df = pd.read_csv(
        Flags.ARGS.cause_of_death_codes_csv, dtype=str, keep_default_na=False
    )
    cause_of_death_df.set_index(CAUSE_OF_DEATH_CODE, inplace=True)
    LOG.info(cause_of_death_df.head(10))

    LOG.info('Starting occupation load')
    occupation_df = pd.read_csv(
        Flags.ARGS.occupation_codes_csv, dtype=str, keep_default_na=False
    )
    occupation_df.set_index(OCCUPATION_CODE_COLUMN, inplace=True)
    LOG.info(occupation_df.head(10))

    LOG.info('Reading in input files into dataframe')
    df = pd.DataFrame()
    total_file_line_counter = 0
    count = 0
    for input_file_name in os.listdir(Flags.ARGS.input_folder):
        # There are other files in the feed durectory, filter to just data files.
        if input_file_name.endswith('.csv.lz4'):
            file_name = os.path.join(Flags.ARGS.input_folder, input_file_name)
            # NOTE(abby): The fetal deaths files are delimited by a comma and the
            # others use a semicolon.
            sep = ',' if 'DOFET' in file_name else ';'
            with LZ4Reader(file_name) as input_file:
                input_df = pd.read_csv(
                    input_file,
                    sep=sep,
                    dtype=str,
                    keep_default_na=False,
                    usecols=lambda col: col in INPUT_REQUIRED_COLUMNS
                    or col in PRE_1996_COLUMNS_RENAME.keys(),
                )
            assert len(input_df) > 0, f'Input file {input_file_name} has no rows'

            # Pre 1996, some columns had different names
            if INPUT_DATE_COLUMN_BEFORE_1996 in input_df.columns:
                input_df = input_df.rename(columns=PRE_1996_COLUMNS_RENAME)

            # Check the number of lines after the file has already been read in
            # so the file doesn't need to be read in twice.
            if total_file_line_counter + len(input_df) > LINE_THRESHOLD:
                LOG.info('Processing previous data and creating new dataframe')
                # Process the current dataframe
                process_dataframe(
                    df,
                    cause_of_death_df,
                    occupation_df,
                    output_file_pattern.build(str(count)),
                )
                # Create a new data frame
                total_file_line_counter = 0
                count += 1
                df = pd.DataFrame()
                LOG.info('Reading in input files into dataframe')

            # Add fillna here to account for earlier years where the missing
            # columns would be added as na.
            df = df.append(input_df, ignore_index=True).fillna('')
            LOG.info('Processing file %s', input_file_name)
            total_file_line_counter += len(input_df)
    LOG.info('Finished reading input files')

    if len(df) > 0:
        LOG.info('Processing final dataframe')
        process_dataframe(
            df, cause_of_death_df, occupation_df, output_file_pattern.build(str(count))
        )

    return 0


if __name__ == '__main__':
    sys.exit(main())
