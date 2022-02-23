#!/usr/bin/env python
import os
import sys

import pandas as pd

from pylib.base.flags import Flags

from config.br_covid.datatypes import Dimension
from log import LOG
from util.file.compression.lz4 import LZ4Reader, LZ4Writer
from util.file.file_config import FilePattern

# Only read in 1/2 gb of compressed files at a time.
FILE_SIZE_THRESHOLD = 0.5 * 1024 * 1024 * 1024

ESC2010_COLUMN_MAPPING = {
    '0': 'Sem escolaridade',
    '1': 'Fundamental I (1ª a 4ª série)',
    '2': 'Fundamental II (5ª a 8ª série)',
    '3': 'Médio (antigo 2º Grau)',
    '4': 'Superior incompleto',
    '5': 'Superior completo',
    '9': 'Ignorado',
    # NOTE(stephen): We want the empty column value to be empty after transformation.
    '': '',
}

YES_NO_RESPONSE_MAPPING = {
    '1': 'Sim',
    '2': 'Não',
    '9': 'Ignorado',
    '': '',
}

# Mapping from column name to a dictionary containing values that should be replaced in
# that column.
COLUMN_MAPPING_VALUES = {
    'SEXO': {
        '1': 'Masculino',
        '2': 'Feminino',
        '0': 'Ignorado'
    },
    'RACACOR': {
        '1': 'Branca',
        '2': 'Preta',
        '3': 'Amarela',
        '4': 'Parda',
        '5': 'Indígena',
        '': 'Ignorado',
    },
    'ESC2010': ESC2010_COLUMN_MAPPING,
    'ESCMAE2010': ESC2010_COLUMN_MAPPING,
    'GRAVIDEZ': {
        '1': 'Única',
        '2': 'Dupla',
        '3': 'Tripla e mais',
        '9': 'Ignorada',
        # NOTE(stephen): Empty string likely means the person was not pregnant.
        '': '',
    },
    'LOCOCOR': {
        '1': 'Hospital',
        '2': 'Outros estabelecimentos de saúde',
        '3': 'Domicílio',
        '4': 'Via pública',
        '5': 'Outros',
        '6': 'Aldeia indígena',
        '9': 'Ignorado',
    },
    'PARTO': {
        '1': 'Vaginal',
        '2': 'Cesáreo',
        '9': 'Ignorado',
        '': '',
    },
    'OBITOPARTO': {
        '1': 'Antes',
        '2': 'Durante',
        '3': 'Depois',
        '9': 'Ignorado',
        '': '',
    },
    'TPMORTEOCO': {
        '1': 'Na gravidez',
        '2': 'No parto',
        '3': 'No abortamento',
        '4': 'Até 42 dias após o término do parto',
        '5': 'De 43 dias a 1 ano após o término da gestação',
        '8': 'Não ocorreu nestes períodos',
        '9': 'Ignorado',
        '': '',
    },
    'ASSISTMED': YES_NO_RESPONSE_MAPPING,
    'NECROPSIA': YES_NO_RESPONSE_MAPPING,
    'ACIDTRAB': YES_NO_RESPONSE_MAPPING,
    'OBITOGRAV': YES_NO_RESPONSE_MAPPING,
    'ATESTANTE': {
        '1': 'Assistente',
        '2': 'Substituto',
        '3': 'IML',
        '4': 'SVO',
        '5': 'Outro',
        '': '',
    },
    'CIRCOBITO': {
        '1': 'Acidente',
        '2': 'Suicídio',
        '3': 'Homicídio',
        '4': 'Outros',
        '9': 'Ignorado',
        '': '',  # Not a violent death.
    },
    'FONTE': {
        '1': 'Ocorrência policial',
        '2': 'Hospital',
        '3': 'Família',
        '4': 'Outra',
        '9': 'Ignorado',
        '': '',
    },
    'OBITOPUERP': {
        '1': 'Sim, até 42 dias após o parto',
        '2': 'Sim, de 43 dias a 1 ano',
        '3': 'Não',
        '9': 'Ignorado',
        '': '',
    },
    'FONTEINV': {
        '1': 'Comitê de Morte Materna e/ou Infantil',
        '2': 'Visita domiciliar / Entrevista família',
        '3': 'Estabelecimento de Saúde / Prontuário',
        '4': 'Relacionado com outros bancos de dados',
        '5': 'SVO',
        '6': 'IML',
        '7': 'Outra fonte',
        '8': 'Múltiplas fontes',
        '9': 'Ignorado',
        '': '',
    },
    'TIPOBITO': {
        '1': 'Fetal',
        '2': 'Não Fetal',
        '': '',
    },
}

# Most columns in COLUMN_MAPPING_VALUES should be fields. Exclude some columns that
# should not be fields.
FIELD_COLUMNS = {
    key: value
    for key, value in COLUMN_MAPPING_VALUES.items()
    if key not in {'SEXO', 'RACACOR', 'ESC2010'}
}

# These columns have numerical values and require no mapping
INPUT_NUMERIC_COLUMNS_TO_FIELDS = {
    'IDADEMAE',
    'QTDFILVIVO',
    'QTDFILMORT',
    'SEMAGESTAC',
}

# Mapping from residence location to the output name for those locations.
RESIDENCE_LOCATION_COLUMN_RENAME = {
    'RegionName': 'RegionNameRes',
    'StateName': 'StateNameRes',
    'HealthRegionName': 'HealthRegionNameRes',
    'MunicipalityName': 'MunicipalityNameRes'
}

# Mapping from death occurrence location to the output name for those locations.
OCCURENCE_LOCATION_COLUMN_RENAME = {
    'RegionName': 'RegionNameOcor',
    'StateName': 'StateNameOcor',
    'HealthRegionName': 'HealthRegionNameOcor',
    'MunicipalityName': 'MunicipalityNameOcor'
}

# Mapping from death certification location to the output name for those locations.
CERTIFICATION_LOCATION_COLUMN_RENAME = {
    'RegionName': 'RegionNameCert',
    'StateName': 'StateNameCert',
    'HealthRegionName': 'HealthRegionNameCert',
    'MunicipalityName': 'MunicipalityNameCert'
}

RENAME_DIMENSIONS = {
    'ACIDTRAB': Dimension.IS_WORK_RELATED,
    'ASSISTMED': Dimension.MEDICAL_CARE,
    'CIRCOBITO': Dimension.VIOLENT_DEATH_TYPE,
    'ESC2010': Dimension.SCHOOLING,
    'ESCMAE2010': Dimension.MOTHERS_SCHOOLING,
    'FONTE': Dimension.INFO_SOURCE,
    'GRAVIDEZ': Dimension.PREGNANCY_TYPE,
    'IDADE': Dimension.AGE,
    'IDADEMAE': Dimension.MOTHERS_AGE,
    'LOCOCOR': Dimension.PLACE_OF_DEATH,
    'NECROPSIA': Dimension.IS_AUTOPSY,
    'OBITOPARTO': Dimension.MOMENT_OF_CHILDBIRTH,
    'PARTO': Dimension.PREGNANCY_KIND,
    'QTDFILMORT': Dimension.NUMBER_DECEASED_CHILDREN,
    'QTDFILVIVO': Dimension.NUMBER_LIVING_CHILDREN,
    'RACACOR': Dimension.RACE,
    'SEMAGESTAC': Dimension.GESTATION_WEEKS,
    'SEXO': Dimension.GENDER,
    'TPMORTEOCO': Dimension.GESTATIONAL_PHASE,
}

INPUT_DATE_COLUMN_BEFORE_1996 = 'DATAOBITO'
INPUT_DATE_COLUMN_AFTER_1996 = 'DTOBITO'
INPUT_MUNICIPALITY_RESIDENCE_COLUMN = 'CODMUNRES'
INPUT_MUNICIPALITY_OCCURRENCE_COLUMN = 'CODMUNOCOR'
INPUT_MUNICIPALITY_CERTIFICATION_COLUMN = 'COMUNSVOIM'
INPUT_CAUSE_OF_DEATH_COLUMN = 'CAUSABAS'
INPUT_AGE_COLUMN = 'IDADE'

INPUT_REQUIRED_COLUMNS = {
    INPUT_DATE_COLUMN_BEFORE_1996,
    INPUT_DATE_COLUMN_AFTER_1996,
    INPUT_MUNICIPALITY_RESIDENCE_COLUMN,
    INPUT_MUNICIPALITY_OCCURRENCE_COLUMN,
    INPUT_MUNICIPALITY_CERTIFICATION_COLUMN,
    INPUT_CAUSE_OF_DEATH_COLUMN,
    INPUT_AGE_COLUMN,
    *COLUMN_MAPPING_VALUES.keys(),
    *INPUT_NUMERIC_COLUMNS_TO_FIELDS,
}

OUTPUT_CAUSE_OF_DEATH_FIELD = 'cause_of_death'
INPUT_CAUSE_OF_DEATH_FIELD = 'field_id'


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
    df: pd.DataFrame, column_name: str, convert_to_int: bool
) -> None:
    # Convert 99 to empty string (missing value)
    df.loc[df[column_name] == '99', column_name] = ''

    if convert_to_int:
        # Convert non-empty rows to integers
        df.loc[df[column_name] != '', column_name] = pd.to_numeric(
            df.loc[df[column_name] != '', column_name],
            downcast='integer',
            errors='coerce',
        )

# Merge municipality information into primary dataframe for the given location.
def match_municipality_codes(
    df: pd.DataFrame,
    municipality_df: pd.DataFrame,
    location_column: str,
    rename_lookup: dict,
):
    df = (
        df.merge(
            municipality_df,
            left_on=location_column,
            right_on='MunicipalityCodeShort',
            how='left',
        )
        .rename(columns=rename_lookup)
        .drop(columns=['MunicipalityCodeShort', 'MunicipalityCodeLong'])
    )
    # Some rows use the long municipality code
    first_geo_dimension = list(rename_lookup.values())[0]
    df.loc[df[first_geo_dimension].isna(), rename_lookup.values()] = (
        df.loc[df[first_geo_dimension].isna(), :]
        .merge(
            municipality_df,
            left_on=location_column,
            right_on='MunicipalityCodeLong',
            how='left',
        )[rename_lookup.keys()]
        .values
    )

    # For unmatched municipality codes starting with 53, change to
    # 530010 (codes intended for DF)
    df.loc[
        df[first_geo_dimension].isna() & (df[location_column] != '') & df[location_column].str.startswith("53"),
        location_column,
    ] = "530010"

    # For the others, consider the first two digits and include “0000” to
    # complete the 06 digits, thus making the registration with the
    # municipality ignored.
    unmatched_filter = df[first_geo_dimension].isna() & (df[location_column] != '') & ~df[location_column].str.startswith("53")
    df.loc[unmatched_filter,location_column] = df.loc[unmatched_filter,location_column].str[:2] + "0000"

    # merge again
    df.loc[df[first_geo_dimension].isna(), rename_lookup.values()] = (
        df.loc[df[first_geo_dimension].isna(), :]
        .merge(
            municipality_df,
            left_on=location_column,
            right_on='MunicipalityCodeShort',
            how='left',
        )[rename_lookup.keys()]
        .values
    )

    # Log unmatched municipality codes
    unmatched_rows = df.loc[
        df[first_geo_dimension].isna() & (df[location_column] != ''),
        :,
    ]
    if len(unmatched_rows) > 0:
        LOG.info('Unmatched municipality codes:')
        LOG.info(
            unmatched_rows
            .groupby(location_column)
            .agg({'date': ['size', 'min', 'max']})
            .sort_values(('date', 'size'), ascending=False)
        )

    return df


def process_dataframe(
    df: pd.DataFrame,
    municipality_df: pd.DataFrame,
    cause_of_death_df: pd.DataFrame,
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
    LOG.info('Finished date parsing')

    LOG.info('Starting remapping column values to human readable strings')
    for column_name, mapping_values in COLUMN_MAPPING_VALUES.items():
        df[column_name] = df[column_name].replace(mapping_values)

    # Convert age to human readable
    df[INPUT_AGE_COLUMN] = df[INPUT_AGE_COLUMN].apply(convert_age)

    # Convert numerical columns
    convert_numerical_columns(df, 'QTDFILVIVO', True)
    convert_numerical_columns(df, 'QTDFILMORT', True)
    convert_numerical_columns(df, 'SEMAGESTAC', False)
    LOG.info('Finished remapping column values')

    # Merge municipality information into primary dataframe for 3 locations.
    LOG.info('Matching residence municipality code to full locations')
    df = match_municipality_codes(
        df,
        municipality_df,
        INPUT_MUNICIPALITY_RESIDENCE_COLUMN,
        RESIDENCE_LOCATION_COLUMN_RENAME,
    )

    LOG.info('Matching death occurrence municipality code to full locations')
    df = match_municipality_codes(
        df,
        municipality_df,
        INPUT_MUNICIPALITY_OCCURRENCE_COLUMN,
        OCCURENCE_LOCATION_COLUMN_RENAME,
    )

    LOG.info('Matching death certification municipality code to full locations')
    df = match_municipality_codes(
        df,
        municipality_df,
        INPUT_MUNICIPALITY_CERTIFICATION_COLUMN,
        CERTIFICATION_LOCATION_COLUMN_RENAME,
    )

    missing_row_number = len(
        df.loc[
            df['RegionNameRes'].isna()
            | df['RegionNameOcor'].isna()
            | df['RegionNameCert'].isna()
        ]
    )
    LOG.info('Number of rows missing a location %s', missing_row_number)
    LOG.info('Finished matching municipalities')

    LOG.info('Starting processing cause of death')
    df[INPUT_CAUSE_OF_DEATH_COLUMN] = [
        s.lower() for s in df[INPUT_CAUSE_OF_DEATH_COLUMN]
    ]
    LOG.info(df[INPUT_CAUSE_OF_DEATH_COLUMN].unique())

    df = (
        df.merge(
            cause_of_death_df,
            left_on=INPUT_CAUSE_OF_DEATH_COLUMN,
            right_on='cid_id',
            how='left',
        )
        .rename(columns={INPUT_CAUSE_OF_DEATH_FIELD: OUTPUT_CAUSE_OF_DEATH_FIELD})
        .drop(columns=['cid_id', INPUT_CAUSE_OF_DEATH_COLUMN])
    )
    # HACK(abby): Older years use the cause of death codes differently. Temporarily use
    # 'unknown' for those rows so they aren't dropped until this can be addressed.
    LOG.info(
        'Number of rows with unmatched CID IDs: %s',
        df[OUTPUT_CAUSE_OF_DEATH_FIELD].isna().sum(),
    )
    df.loc[
        df[OUTPUT_CAUSE_OF_DEATH_FIELD].isna(), OUTPUT_CAUSE_OF_DEATH_FIELD
    ] = 'ignorado'
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

            # Initialize the field to 0 since all rows that do not match the dimension value
            # should not be counted.
            df[field_name] = 0

            # Set all rows that match the filter to 1.
            df.loc[df_row_filter, field_name] = 1

    # Build fields for numeric columns
    for column in INPUT_NUMERIC_COLUMNS_TO_FIELDS:
        field_name = f'*field_{column}'
        df[field_name] = df[column]

    # Build fields for cause of death indicators
    for dimension_value in df[OUTPUT_CAUSE_OF_DEATH_FIELD].unique():
        field_name = f'*field_{OUTPUT_CAUSE_OF_DEATH_FIELD} - {dimension_value}'
        df_row_filter = df[OUTPUT_CAUSE_OF_DEATH_FIELD] == dimension_value
        df[field_name] = 0
        df.loc[df_row_filter, field_name] = 1
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
        '--municipality_code_mapping_file',
        type=str,
        required=True,
        help='CSV file containing a mapping from municipality code to the full hierarchy'
    )
    Flags.PARSER.add_argument(
        '--output_file_pattern', type=str, required=True, help='Converted file pattern location'
    )
    Flags.PARSER.add_argument(
        '--cause_of_death_codes_csv',
        type=str,
        required=True,
        help='File path for cause of death codes to fields',
    )
    Flags.InitArgs()

    output_file_pattern = FilePattern(Flags.ARGS.output_file_pattern)

    # TODO(stephen): Add in code for states that is missing in CSV. We are seeing
    # that municipality is ignored for some rows (~717) so we need to account for
    # this in the mapping.
    # https://www.ibge.gov.br/explica/codigos-dos-municipios.php
    LOG.info('Reading municipality code mapping file')
    municipality_df = pd.read_csv(Flags.ARGS.municipality_code_mapping_file, dtype=str)
    LOG.info('Finished reading municipalities')

    LOG.info('Starting cause of death load')
    cause_of_death_df = pd.read_csv(Flags.ARGS.cause_of_death_codes_csv)
    shortened_cid_lookup = {}
    for _, row in cause_of_death_df.iterrows():
        shortened_cid_lookup[row['cid_id'][:-1]] = row[INPUT_CAUSE_OF_DEATH_FIELD]
    shortened_df = []
    for key, val in shortened_cid_lookup.items():
        shortened_df.append({'cid_id': key, INPUT_CAUSE_OF_DEATH_FIELD: val})
    shortened_df = pd.DataFrame(shortened_df)
    cause_of_death_df = pd.concat([cause_of_death_df, shortened_df])
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
                    df,
                    municipality_df,
                    cause_of_death_df,
                    output_file_pattern.build(str(count)),
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
                    usecols=lambda col: col in INPUT_REQUIRED_COLUMNS,
                )
                df = df.append(input_df, ignore_index=True)
    LOG.info('Finished reading input files')

    LOG.info('Processing final dataframe')
    process_dataframe(
        df,municipality_df, cause_of_death_df, output_file_pattern.build(str(count))
    )

    return 0


if __name__ == '__main__':
    sys.exit(main())
