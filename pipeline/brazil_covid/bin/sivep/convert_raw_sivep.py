#!/usr/bin/env python
# This script converts the raw SIVEP dataset into a clean output that can be used by
# process_csv. It follows the rules described in the Brazil Indicator Structure sheet
# (https://docs.google.com/spreadsheets/d/1KXA1koe8zwnqg8sk6mOMLO_6tcuBFitvP5ChiDpoWE0/edit#gid=1523634420)
# and replaces the original dataprep (https://clouddataprep.com/flows/268183).
import datetime
import sys

import numpy as np
import pandas as pd

from pylib.base.flags import Flags

from config.br_covid.datatypes import Dimension
from config.system import STANDARD_DATA_DATE_FORMAT
from log import LOG
from util.file.compression.lz4 import LZ4Writer

INPUT_DATE_FORMAT = '%d/%m/%Y'
TODAY = datetime.date.today()

# Mapping from column to field ID where a value == 1 in the column indicates we store a
# 1 for that field. All other column values get a 0 stored.
SIMPLE_INDICATORS = {
    'ASMA': 'asma',
    'CARDIOPATI': 'doenca_cardiovascular_cronica',
    'DESC_RESP': 'number_of_people_with_symptom_diffbreathing',
    'DIABETES': 'diabetes_mellitus',
    'DISPNEIA': 'number_of_people_with_symptom_dispneia',
    'FEBRE': 'number_of_people_with_symptom_fever',
    'GARGANTA': 'number_of_people_with_symptom_sorethroat',
    'HEMATOLOGI': 'doenca_hematologica_cronica',
    'HEPATICA': 'doenca_hepatica_cronica',
    'IMUNODEPRE': 'imunodeficiencia_ou_imunodepressao',
    'NEUROLOGIC': 'doenca_neurologica_cronica',
    'OBESIDADE': 'obesidade',
    'OUT_MORBI': 'outros',
    'PNEUMOPATI': 'outra_pneumatopatia_cronica',
    'PUERPERA': 'puerpera',
    'RENAL': 'doenca_renal_cronica',
    'SIND_DOWN': 'sindrome_de_down',
    'TOSSE': 'number_of_people_with_symptom_cough',
    'UTI': 'number_of_people_admitted_icu',
}

# Mapping from comorbidity column to (indicator column, text). When the indicator value
# is 1, set the comorbidity column value to the text representation of the comorbidity.
COMORBIDITY_MAPPING = {
    'dimension_asma': ('asma', 'Asma'),
    'dimension_diabetes_mellitus': ('diabetes_mellitus', 'Diabetes mellitus'),
    'dimension_doenca_cardiovascular_cronica': (
        'doenca_cardiovascular_cronica',
        'Doença Cardiovascular Crônica',
    ),
    'dimension_doenca_hematologica_cronica': (
        'doenca_hematologica_cronica',
        'Doença Hematológica Crônica',
    ),
    'dimension_doenca_hepatica_cronica': (
        'doenca_hepatica_cronica',
        'Doença Hepática Crônica',
    ),
    'dimension_doenca_neurologica_cronica': (
        'doenca_neurologica_cronica',
        'Doença Neurológica Crónica',
    ),
    'dimension_doenca_renal_cronica': ('doenca_renal_cronica', 'Doença Renal Crônica'),
    'dimension_hospitalizado': (
        'number_of_people_hospitalized',
        'Pacientes Internados',
    ),
    'dimension_imunodeficiencia': (
        'imunodeficiencia_ou_imunodepressao',
        'Imunodeficiência ou Imunodepressão',
    ),
    'dimension_obesidade': ('obesidade', 'Obesidade'),
    'dimension_outra_pneumatopatia_cronica': (
        'outra_pneumatopatia_cronica',
        'Outra Pneumatopatia Crônica',
    ),
    'dimension_outros': ('outros', 'Outros'),
    'dimension_puerpera': ('puerpera', 'Puérpera'),
    'dimension_sindrome_de_down': ('sindrome_de_down', 'Síndrome de Down'),
}

# Mapping from EVOLUCAO value to the indicator for that hospitalization code.
HOSPITALIZED_INDICATORS = {
    '1': 'number_of_people_hospitalized_cured',
    '2': 'number_of_people_hospitalized_died_of_infl',
    '3': 'number_of_people_hospitalized_other',
    # NOTE(stephen): We didn't see any actual values for this in the raw data. It might
    # need further investigation. The previous dataprep produced 149 rows with this
    # value.
    '4': 'number_of_people_hospitalized_died_in_investigation',
    '9': 'number_of_people_hospitalized_ignored',
}

# These fields indicate that the patient died while in the hospital.
HOSPITALIZATION_DEATH_INDICATORS = [
    'number_of_people_hospitalized_died_in_investigation',
    'number_of_people_hospitalized_died_of_infl',
    'number_of_people_hospitalized_other',
]

# Mapping from CLASSI_FIN code to the SRAG classification. This includes all non-SARS
# indicators. The key is the classification code, and the value is tuple containing the
# (field_id, final classification status dimension value).
SRAG_CLASSIFICATION_INDICATORS = {
    '0': ('srag_em_investigacao', 'SRAG em investigação'),
    '1': ('srag_por_influenza', 'SRAG por Influenza'),
    '2': ('srag_por_outro_virus_respiratorio', 'SRAG por outro virus respiratório'),
    '3': ('srag_por_outro_agente_etiologico', 'SRAG por outro agente etiológico'),
    '4': ('srag_por_nao_especificado', 'SRAG não especificado'),
    '5': ('srag_covid19', 'COVID-19'),
}

# Date column *excluding* the primary date for each row DT_SIN_PRI.
INPUT_OPTIONAL_DATE_COLUMNS = [
    'DT_COLETA',
    'DT_ENTUTI',
    'DT_EVOLUCA',
    'DT_INTERNA',
    'DT_NOTIFIC',
    'DT_SAIDUTI',
]

# Mapping from date diff field to a tuple containing (start date, end date) columns.
# The formula applied will be end date - start date. If either date does not have a
# value, it will be skipped.
DATE_DIFF_INDICATORS = {
    'oportunidade_de_notificacao': ('DT_SIN_PRI', 'DT_NOTIFIC'),
    'oportunidade_de_coleta_de_amostra_desde_a_notificacao': (
        'DT_NOTIFIC',
        'DT_COLETA',
    ),
    'oportunidade_de_coleta_de_amostra_desde_inicio_dos_sintomas': (
        'DT_SIN_PRI',
        'DT_COLETA',
    ),
    'tempo_de_internacao_na_uti_entrada_e_a_saida': ('DT_ENTUTI', 'DT_SAIDUTI'),
    'tempo_em_dias_entre_entrada_na_uti_e_o_desfecho_obito_caso_va_a_obito_na_uti': (
        'DT_ENTUTI',
        'DT_EVOLUCA',
    ),
    'tempo_de_internacao': ('DT_INTERNA', 'DT_EVOLUCA'),
}

# These are the columns that we will need to use from the original raw CSV to build the
# indicators and dimensions.
# NOTE(stephen): Specifying this directly allows us to significantly reduce memory usage
# that this step takes. The original raw dataset has a huge number of columns, and most
# of them are not necessary for this converter.
REQUIRED_INPUT_COLUMNS = [
    'CLASSI_FIN',
    'CO_MUN_RES',
    'CS_RACA',
    'CS_SEXO',
    'DT_SIN_PRI',
    'EVOLUCAO',
    'HOSPITAL',
    'NU_IDADE_N',
    'PCR_SARS2',
    'SG_UF',
    *INPUT_OPTIONAL_DATE_COLUMNS,
    *SIMPLE_INDICATORS.keys(),
]


def build_age_group(value: str) -> str:
    '''Convert the raw age value string into an age range. Ages that are less than 0
    will be placed into the 0-9 bucket, and ages that are greater than 80 (sometimes are
    greater than 100) will be placed into the 80+ bucket.
    '''
    # NOTE(stephen): Safeguarding against unparseable age values. Defaulting to an age
    # of 0 when this happens.
    try:
        age = int(value)
    except:
        age = 0

    if age < 10:
        return '0-9'
    if age < 20:
        return '10-19'
    if age < 30:
        return '20-29'
    if age < 40:
        return '30-39'
    if age < 50:
        return '40-49'
    if age < 60:
        return '50-59'
    if age < 70:
        return '60-69'
    if age < 80:
        return '70-79'
    return '80+'


def build_race(value: str) -> str:
    if value == '1':
        return 'branca'
    if value == '2':
        return 'preta'
    if value == '3':
        return 'amarela'
    if value == '4':
        return 'parda'
    if value == '5':
        return 'indígena'
    if value == '9':
        return 'ignorado'
    # All other values fall under "no information". Possible values seen are nan.
    return 's/informação'


def write_csv_by_date_of_evaluation(df: pd.DataFrame, output_filename: str):
    ''' HACK(stephen): Write a condensed version of the dataframe using the date of
    evaluation as the primary date column for specific indicators. See T8643.
    It was easier to write a separate CSV than to figure out how to align the
    indicator values correctly in the original dataframe since we are creating a small
    number of new indicators and want to drop some of the additional dimensions.
    '''
    LOG.info('Writing date of evaluation output CSV')

    # Get the list of SRAG field_ids in the output dataframe.
    srag_fields = [
        f'*field_{field_id}' for field_id, _ in SRAG_CLASSIFICATION_INDICATORS.values()
    ]

    # Rename the SRAG fields to include the `by_evaluation_date` suffix.
    srag_field_rename = {
        field_id: f'{field_id}_by_evaluation_date' for field_id in srag_fields
    }

    # Find all rows with a valid date of evaluation AND that have an SRAG indicator that
    # is not zero.
    date_of_evaluation_slice = df['DT_EVOLUCA'].notna() & df[srag_fields].any(
        axis='columns'
    )

    # Replace the date column for the valid rows.
    df.loc[date_of_evaluation_slice, 'date'] = pd.to_datetime(
        df[date_of_evaluation_slice]['DT_EVOLUCA']
    ).dt.strftime(STANDARD_DATA_DATE_FORMAT)

    # Rename the SRAG fields.
    df.rename(columns=srag_field_rename, inplace=True)

    # Only include the subset of dimensions and fields that matter for this "date of
    # evaluation" analysis.
    output_columns = [
        'date',
        Dimension.STATE,
        Dimension.MUNICIPALITY,
        Dimension.AGE_GROUP,
        Dimension.GENDER,
        Dimension.RACE,
        Dimension.DEAD,
        Dimension.FINAL_CLASSIFICATION_OF_CASE,
        'dimension_hospitalizado',
        *sorted(srag_field_rename.values()),
    ]

    # Write the slice of the dataframe to CSV.
    LOG.info('Writing output CSV for date of evaluation fields')
    with LZ4Writer(output_filename) as output_file:
        df[date_of_evaluation_slice].to_csv(
            output_file, columns=output_columns, index=False
        )


def main():
    Flags.PARSER.add_argument(
        '--input_file', type=str, required=True, help='Raw SIVEP dataset to convert'
    )
    Flags.PARSER.add_argument(
        '--output_file', type=str, required=True, help='Output SIVEP data to process'
    )
    Flags.PARSER.add_argument(
        '--output_file_by_evaluation_date',
        type=str,
        required=True,
        help='Output SRAG indicator rows by date of evaluation',
    )
    Flags.InitArgs()

    # Read the CSV files with all columns as strings. This allows us to perform a more
    # straightforward and safe transformation since Pandas won't try to infer datatypes.
    LOG.info('Reading raw dataset')

    # NOTE(stephen): Only a subset of columns from the original raw dataset are read in.
    # If you are introducing new indicators to build, or need access to additional
    # columns in the dataframe, you should modify `REQUIRED_INPUT_COLUMNS`.
    df = pd.read_csv(
        Flags.ARGS.input_file,
        sep=';',
        usecols=REQUIRED_INPUT_COLUMNS,
        dtype=str,
        keep_default_na=False,
    )

    LOG.info('Finished reading data. Row count: %s', len(df))

    # Keep track of all the field IDs created.
    field_ids = set()

    # Track total number of cases.
    df['total_number_of_cases_seen'] = 1
    field_ids.add('total_number_of_cases_seen')

    # Simple indicators just check if the column value in the input dataset is '1'.
    for column, field_id in SIMPLE_INDICATORS.items():
        df[field_id] = 0
        df.loc[df[column] == '1', field_id] = 1
        field_ids.add(field_id)

    LOG.info('Building hospitalization fields')
    df['number_of_people_hospitalized'] = 0

    # NOTE(stephen): Number of people hospitalized requires looking at two different
    # columns. I am not sure why, but that was the original specification.
    df.loc[
        (df['HOSPITAL'] == '1') | (df['EVOLUCAO'] == '2'),
        'number_of_people_hospitalized',
    ] = 1
    field_ids.add('number_of_people_hospitalized')

    # Track hospital outcome. The hospitalized indicators stores a mapping from EVOLUCAO
    # code to the field ID for that specific code.
    for hospitalization_code, field_id in HOSPITALIZED_INDICATORS.items():
        df[field_id] = 0
        df.loc[df['EVOLUCAO'] == hospitalization_code, field_id] = 1
        field_ids.add(field_id)

    LOG.info('Building SRAG classification fields')
    df[Dimension.FINAL_CLASSIFICATION_OF_CASE] = ''

    for code, (field_id, dimension_value) in SRAG_CLASSIFICATION_INDICATORS.items():
        df[field_id] = 0
        # NOTE(stephen): Special case for COVID-19 indicator since it can be found in
        # both the SRAG classification column AND a special SARS column.
        if field_id == 'srag_covid19':
            srag_match_slice = (df['CLASSI_FIN'] == code) | (df['PCR_SARS2'] == '1')
        # NOTE(abby): Special case for investigation indicator since it can be
        # represented by the code or by a missing value.
        elif field_id == 'srag_em_investigacao':
            srag_match_slice = (df['CLASSI_FIN'] == code) | (df['CLASSI_FIN'] == '')
        else:
            srag_match_slice = (df['CLASSI_FIN'] == code) & (df['PCR_SARS2'] != '1')
        df.loc[srag_match_slice, field_id] = 1
        df.loc[
            srag_match_slice, Dimension.FINAL_CLASSIFICATION_OF_CASE
        ] = dimension_value
        field_ids.add(field_id)

    # The `dead` dimension is built off the hospitalization indicators that indicate a
    # patient died. If the patient has died, then we mark the death column with a string
    # value indicating death.
    df[Dimension.DEAD] = ''
    df.loc[
        df[HOSPITALIZATION_DEATH_INDICATORS].any(axis='columns'), Dimension.DEAD
    ] = 'Óbito'

    # Parse the primary date column. In testing, there were no invalid values found.
    LOG.info('Parsing dates')

    # NOTE(stephen): Having to do some weird things with `.dt.date` because pandas
    # inexplicably would return a Timestamp OR a datetime64 date type depending on usage
    # and I could not figure out how to get it to always return a Timestamp.
    date_series = pd.to_datetime(df['DT_SIN_PRI'], format=INPUT_DATE_FORMAT)

    # Set up the output date column.
    df['date'] = date_series.dt.strftime(STANDARD_DATA_DATE_FORMAT)

    # Replace the input date column with the parsed value so it can be used for date
    # diff calculations later.
    df['DT_SIN_PRI'] = date_series.dt.date

    for date_column in INPUT_OPTIONAL_DATE_COLUMNS:
        # Ignore any dates that are not in the date range 2019-2021.
        valid_date_slice = df[date_column].str.endswith(
            ('/2019', '/2020', '/2021'), na=False
        )

        # Parse the valid dates and set them in the same column in place.
        df.loc[valid_date_slice, date_column] = pd.to_datetime(
            df[valid_date_slice][date_column], format=INPUT_DATE_FORMAT
        ).dt.date

        # Replace all invalid dates with nan. Invalid dates are date values that are
        # outside the valid time range OR are later than today.
        df.loc[
            ~valid_date_slice | (df[valid_date_slice][date_column] > TODAY), date_column
        ] = np.nan

    LOG.info('Building date diff fields')
    for field_id, (start_date, end_date) in DATE_DIFF_INDICATORS.items():
        df[field_id] = 0

        valid_date_slice = df[start_date].notna() & df[end_date].notna()
        valid_date_df = df[valid_date_slice]

        df.loc[valid_date_slice, field_id] = (
            valid_date_df[end_date].subtract(valid_date_df[start_date]).dt.days
        )

        # Handle negative values.
        df[field_id].clip(lower=0, inplace=True)
        field_ids.add(field_id)

    # The comorbidity dimension that process_csv will create is a multi-valued dimension
    # that takes on the values of all these columns.
    LOG.info('Building comorbidity multi-value dimension columns')
    for comorbidity_id, (field_id, dimension_value) in COMORBIDITY_MAPPING.items():
        df[comorbidity_id] = ''
        df.loc[df[field_id] == 1, comorbidity_id] = dimension_value

    # Setup dimension columns for output.
    LOG.info('Building output dimension columns')
    df[Dimension.STATE] = df['SG_UF']
    df[Dimension.MUNICIPALITY] = df['CO_MUN_RES']
    df[Dimension.GENDER] = df['CS_SEXO']
    df[Dimension.AGE_GROUP] = df['NU_IDADE_N'].apply(build_age_group)
    df[Dimension.RACE] = df['CS_RACA'].apply(build_race)

    # Rename fields to wildcard format that process_csv will use.
    field_column_rename = {field_id: f'*field_{field_id}' for field_id in field_ids}
    df.rename(columns=field_column_rename, inplace=True)

    output_columns = [
        'date',
        Dimension.STATE,
        Dimension.MUNICIPALITY,
        Dimension.AGE_GROUP,
        Dimension.GENDER,
        Dimension.RACE,
        Dimension.DEAD,
        Dimension.FINAL_CLASSIFICATION_OF_CASE,
        *sorted(COMORBIDITY_MAPPING.keys()),
        *sorted(field_column_rename.values()),
    ]

    LOG.info('Writing output CSV')
    with LZ4Writer(Flags.ARGS.output_file) as output_file:
        df.to_csv(output_file, columns=output_columns, index=False)

    write_csv_by_date_of_evaluation(df, Flags.ARGS.output_file_by_evaluation_date)
    LOG.info('Successfully wrote converted CSVs')
    return 0


if __name__ == '__main__':
    sys.exit(main())
