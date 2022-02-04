#!/usr/bin/env python
import os
import sys

import pandas as pd

from pylib.base.flags import Flags

from log import LOG
from util.file.compression.lz4 import LZ4Reader, LZ4Writer

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

INPUT_DATE_COLUMN_BEFORE_1996 = 'DATAOBITO'
INPUT_DATE_COLUMN_AFTER_1996 = 'DTOBITO'
INPUT_MUNICIPALITY_RESIDENCE_COLUMN = 'CODMUNRES'
INPUT_MUNICIPALITY_OCCURRENCE_COLUMN = 'CODMUNOCOR'
INPUT_CAUSE_OF_DEATH_COLUMN = 'CAUSABAS'

INPUT_REQUIRED_COLUMNS = {
    INPUT_DATE_COLUMN_BEFORE_1996,
    INPUT_DATE_COLUMN_AFTER_1996,
    INPUT_MUNICIPALITY_RESIDENCE_COLUMN,
    INPUT_MUNICIPALITY_OCCURRENCE_COLUMN,
    INPUT_CAUSE_OF_DEATH_COLUMN,
    *COLUMN_MAPPING_VALUES.keys(),
}

OUTPUT_CAUSE_OF_DEATH_FIELD = 'cause_of_death'
INPUT_CAUSE_OF_DEATH_FIELD = 'field_id'


def main():
    Flags.PARSER.add_argument(
        '--input_folder',
        type=str,
        required=True,
        help='Input folder with years of raw SIM files to convert',
    )
    Flags.PARSER.add_argument(
        '--output_file', type=str, required=True, help='Converted file location'
    )
    Flags.PARSER.add_argument(
        '--cause_of_death_codes_csv',
        type=str,
        required=True,
        help='File path for cause of death codes to fields',
    )
    Flags.InitArgs()

    LOG.info('Reading in input files into dataframe')
    df = pd.DataFrame()
    for input_file_name in os.listdir(Flags.ARGS.input_folder):
        if input_file_name.endswith('.csv.lz4'):
            LOG.info('Processing file %s', input_file_name)
            with LZ4Reader(
                os.path.join(Flags.ARGS.input_folder, input_file_name)
            ) as input_file:
                input_df = pd.read_csv(
                    input_file,
                    sep=';',
                    dtype=str,
                    keep_default_na=False,
                    usecols=lambda col: col in INPUT_REQUIRED_COLUMNS,
                )
                df = df.append(input_df, ignore_index=True)
    LOG.info('Finished reading input files')

    LOG.info('Number of rows in input: %s', len(df))

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
    df = df[~df['date'].isna()]
    input_count = len(df)
    LOG.info('Finished date parsing')

    LOG.info('Starting remapping column values to human readable strings')
    for column_name, mapping_values in COLUMN_MAPPING_VALUES.items():
        df[column_name] = df[column_name].replace(mapping_values)
    LOG.info('Finished remapping column values')

    # TODO(stephen): Add in code for states that is missing in CSV. We are seeing
    # that municipality is ignored for some rows (~717) so we need to account for
    # this in the mapping.
    # https://www.ibge.gov.br/explica/codigos-dos-municipios.php
    LOG.info('Reading municipality code mapping file')
    municipality_df = pd.read_csv(Flags.ARGS.municipality_code_mapping_file, dtype=str)
    LOG.info('Finished reading municipalities')


    # Merge municipality information into primary dataframe for location of residence.
    LOG.info('Matching residence municipality code to full locations')
    df = (df
        .merge(municipality_df, left_on=INPUT_MUNICIPALITY_RESIDENCE_COLUMN, right_on='MunicipalityCodeShort')
        .rename(columns=RESIDENCE_LOCATION_COLUMN_RENAME)
        .drop(columns=['MunicipalityCodeShort', 'MunicipalityCodeLong'])
    )

    # Merge municipality information into primary dataframe for location of death.
    LOG.info('Matching death occurrence municipality code to full locations')
    df = (df
        .merge(municipality_df, left_on=INPUT_MUNICIPALITY_OCCURRENCE_COLUMN, right_on='MunicipalityCodeShort')
        .rename(columns=OCCURENCE_LOCATION_COLUMN_RENAME)
        .drop(columns=['MunicipalityCodeShort', 'MunicipalityCodeLong'])
    )
    after_merge_count = len(df)
    assert input_count == after_merge_count, \
        'Some rows were dropped after municipality code merging! Original rows: %s, New rows: %s' % (input_count, after_merge_count)

    LOG.info('Finished matching municipalities')

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
    df.loc[
        df[OUTPUT_CAUSE_OF_DEATH_FIELD].isna(), OUTPUT_CAUSE_OF_DEATH_FIELD
    ] = 'ignorado'
    after_merge_count = len(df)
    assert input_count == after_merge_count, (
        'Some rows were dropped after cause of death code merging! Original rows: %s, New rows: %s'
        % (input_count, after_merge_count)
    )
    LOG.info('Finished processing cause of death')

    LOG.info('Building numeric field columns')
    # Build a unique numeric field for each dimension value in all the columns that we care
    # about. This will allow the user to query for a specific value without grouping.
    for column, dimension_mapping in COLUMN_MAPPING_VALUES.items():
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
    LOG.info('Finished building numeric field columns')
    for dimension_value in df[OUTPUT_CAUSE_OF_DEATH_FIELD].unique():
        field_name = f'*field_{OUTPUT_CAUSE_OF_DEATH_FIELD} - {dimension_value}'  # 'ESCMAE2010 - Sem escolaridade'
        df_row_filter = (
            df[OUTPUT_CAUSE_OF_DEATH_FIELD] == dimension_value
        )  # df['ESCMAE2010'] == 'Sem escolaridade'
        df[field_name] = 0
        df.loc[df_row_filter, field_name] = 1

    LOG.info('Writing the output CSV')
    with LZ4Writer(Flags.ARGS.output_file) as output_file:
        df.to_csv(output_file, index=False)

    LOG.info('Finished writing output CSV')
    return 0


if __name__ == '__main__':
    sys.exit(main())
