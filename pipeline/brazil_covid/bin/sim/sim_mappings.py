#!/usr/bin/env python
from config.br_covid.datatypes import Dimension
from pipeline.brazil_covid.bin.sim.convert_cid_csv import (
    CATEGORY_1_COL,
    CATEGORY_2_COL,
    CATEGORY_3_COL,
    CATEGORY_4_COL,
    PARENT_COL,
    TITLE_COL,
)

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
    'SEXO': {'1': 'Masculino', '2': 'Feminino', '0': 'Ignorado'},
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
    CATEGORY_1_COL: Dimension.CAUSES_CATEGORY_1,
    CATEGORY_2_COL: Dimension.CAUSES_CATEGORY_2,
    CATEGORY_3_COL: Dimension.CAUSES_CATEGORY_3,
    CATEGORY_4_COL: Dimension.CAUSES_CATEGORY_4,
    PARENT_COL: Dimension.CAUSES_PARENT,
    TITLE_COL: Dimension.CAUSES_TITLE,
}

INPUT_DATE_COLUMN_BEFORE_1996 = 'DATAOBITO'
INPUT_DATE_COLUMN_AFTER_1996 = 'DTOBITO'
INPUT_MUNICIPALITY_RESIDENCE_COLUMN = 'CODMUNRES'
INPUT_MUNICIPALITY_OCCURRENCE_COLUMN = 'CODMUNOCOR'
INPUT_MUNICIPALITY_CERTIFICATION_COLUMN = 'COMUNSVOIM'
INPUT_CAUSE_OF_DEATH_COLUMN = 'CAUSABAS'
INPUT_AGE_COLUMN = 'IDADE'

LOCATION_COLUMNS_RENAME = {
    'MUNIOCOR': INPUT_MUNICIPALITY_OCCURRENCE_COLUMN,
    'MUNIRES': INPUT_MUNICIPALITY_RESIDENCE_COLUMN,
}

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
