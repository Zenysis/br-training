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
from pipeline.brazil_covid.bin.sim.convert_occupation_codes import (
    FAMILY_COLUMN,
    GROUP_COLUMN,
    PRINCIPAL_SUBGROUP_COLUMN,
    SUBGROUP_COLUMN,
    TITLE_COLUMN as OCCUPATION_TITLE_COLUMN,
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

# These columns have numerical values and require no mapping. They do need to have the
# placeholder values converted to empty strings.
INPUT_NUMERIC_COLUMNS_TO_PLACEHOLDERS = {
    'IDADEMAE': ['00', '99'],
    'QTDFILVIVO': ['99'],
    'QTDFILMORT': ['99'],
    'SEMAGESTAC': ['99'],
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
INPUT_AGE_COLUMN = 'IDADE'
INPUT_OCCUPATION_COLUMN = 'OCUP'
INPUT_MOTHER_OCCUPATION_COLUMN = 'OCUPMAE'

CAUSE_A_COL_RENAME = {
    CATEGORY_1_COL: Dimension.CAUSES_A_CATEGORY_1,
    CATEGORY_2_COL: Dimension.CAUSES_A_CATEGORY_2,
    CATEGORY_3_COL: Dimension.CAUSES_A_CATEGORY_3,
    CATEGORY_4_COL: Dimension.CAUSES_A_CATEGORY_4,
    PARENT_COL: Dimension.CAUSES_A_PARENT,
    TITLE_COL: Dimension.CAUSES_A_TITLE,
}
CAUSE_B_COL_RENAME = {
    CATEGORY_1_COL: Dimension.CAUSES_B_CATEGORY_1,
    CATEGORY_2_COL: Dimension.CAUSES_B_CATEGORY_2,
    CATEGORY_3_COL: Dimension.CAUSES_B_CATEGORY_3,
    CATEGORY_4_COL: Dimension.CAUSES_B_CATEGORY_4,
    PARENT_COL: Dimension.CAUSES_B_PARENT,
    TITLE_COL: Dimension.CAUSES_B_TITLE,
}
CAUSE_C_COL_RENAME = {
    CATEGORY_1_COL: Dimension.CAUSES_C_CATEGORY_1,
    CATEGORY_2_COL: Dimension.CAUSES_C_CATEGORY_2,
    CATEGORY_3_COL: Dimension.CAUSES_C_CATEGORY_3,
    CATEGORY_4_COL: Dimension.CAUSES_C_CATEGORY_4,
    PARENT_COL: Dimension.CAUSES_C_PARENT,
    TITLE_COL: Dimension.CAUSES_C_TITLE,
}
CAUSE_D_COL_RENAME = {
    CATEGORY_1_COL: Dimension.CAUSES_D_CATEGORY_1,
    CATEGORY_2_COL: Dimension.CAUSES_D_CATEGORY_2,
    CATEGORY_3_COL: Dimension.CAUSES_D_CATEGORY_3,
    CATEGORY_4_COL: Dimension.CAUSES_D_CATEGORY_4,
    PARENT_COL: Dimension.CAUSES_D_PARENT,
    TITLE_COL: Dimension.CAUSES_D_TITLE,
}
CAUSE_2_COL_RENAME = {
    CATEGORY_1_COL: Dimension.CAUSES_2_CATEGORY_1,
    CATEGORY_2_COL: Dimension.CAUSES_2_CATEGORY_2,
    CATEGORY_3_COL: Dimension.CAUSES_2_CATEGORY_3,
    CATEGORY_4_COL: Dimension.CAUSES_2_CATEGORY_4,
    PARENT_COL: Dimension.CAUSES_2_PARENT,
    TITLE_COL: Dimension.CAUSES_2_TITLE,
}

# NOTE(abby): This comes from the raw data documentation and should not change.
MAX_NUMBER_OF_CODES_PER_CAUSE_OF_DEATH_COL = {
    'LINHAA': 4,
    'LINHAB': 4,
    'LINHAC': 4,
    'LINHAD': 4,
    'LINHAII': 6,
}
CAUSE_OF_DEATH_COLUMNS_TO_RENAME = {
    'LINHAA': CAUSE_A_COL_RENAME,
    'LINHAB': CAUSE_B_COL_RENAME,
    'LINHAC': CAUSE_C_COL_RENAME,
    'LINHAD': CAUSE_D_COL_RENAME,
    'LINHAII': CAUSE_2_COL_RENAME,
}
# Use primary cause of death for cause of death indicators
CAUSE_OF_DEATH_FIELDS = [
    f'{Dimension.CAUSES_A_CATEGORY_1}_{i}'
    for i in range(MAX_NUMBER_OF_CODES_PER_CAUSE_OF_DEATH_COL['LINHAA'])
]

PRE_1996_COLUMNS_RENAME = {
    'MUNIOCOR': INPUT_MUNICIPALITY_OCCURRENCE_COLUMN,
    'MUNIRES': INPUT_MUNICIPALITY_RESIDENCE_COLUMN,
    'OCUPACAO': INPUT_OCCUPATION_COLUMN,
    # HACK(abby): This is technically a different column, but the cause of death was reported
    # differently pre 1996, so just use the only cause of death column as cause of death A.
    'CAUSABAS': 'LINHAA',
}

INPUT_REQUIRED_COLUMNS = {
    INPUT_DATE_COLUMN_BEFORE_1996,
    INPUT_DATE_COLUMN_AFTER_1996,
    INPUT_MUNICIPALITY_RESIDENCE_COLUMN,
    INPUT_MUNICIPALITY_OCCURRENCE_COLUMN,
    INPUT_MUNICIPALITY_CERTIFICATION_COLUMN,
    INPUT_AGE_COLUMN,
    INPUT_OCCUPATION_COLUMN,
    INPUT_MOTHER_OCCUPATION_COLUMN,
    *COLUMN_MAPPING_VALUES.keys(),
    *INPUT_NUMERIC_COLUMNS_TO_PLACEHOLDERS.keys(),
    *CAUSE_OF_DEATH_COLUMNS_TO_RENAME.keys(),
}

OCCUPATION_COLUMNS_RENAME = {
    OCCUPATION_TITLE_COLUMN: Dimension.OCCUPATION_TITLE,
    FAMILY_COLUMN: Dimension.OCCUPATION_FAMILY,
    SUBGROUP_COLUMN: Dimension.OCCUPATION_SUBGROUP,
    PRINCIPAL_SUBGROUP_COLUMN: Dimension.OCCUPATION_PRINCIPAL_SUBGROUP,
    GROUP_COLUMN: Dimension.OCCUPATION_GROUP,
}
MOTHERS_OCCUPATION_COLUMNS_RENAME = {
    OCCUPATION_TITLE_COLUMN: Dimension.MOTHERS_OCCUPATION_TITLE,
    FAMILY_COLUMN: Dimension.MOTHERS_OCCUPATION_FAMILY,
    SUBGROUP_COLUMN: Dimension.MOTHERS_OCCUPATION_SUBGROUP,
    PRINCIPAL_SUBGROUP_COLUMN: Dimension.MOTHERS_OCCUPATION_PRINCIPAL_SUBGROUP,
    GROUP_COLUMN: Dimension.MOTHERS_OCCUPATION_GROUP,
}
