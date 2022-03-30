############################################################################
# Datatypes

from enum import Enum
from pylib.base.flags import Flags

from data.pipeline.datatypes.base_row import BaseRow
from data.pipeline.datatypes.base_row_factory import BaseRowFactory
from data.pipeline.datatypes.dimension_factory import DimensionFactory

# Output field information
RAW_PREFIX = 'Raw'
CLEANED_PREFIX = 'Clean'
CANONICAL_PREFIX = 'Canonical'


class Dimension:
    SOURCE = (BaseRow.SOURCE_FIELD,)
    DATE = (BaseRow.DATE_FIELD,)
    REGION = 'RegionName'
    STATE = 'StateName'
    HEALTH_REGION = 'HealthRegionName'
    MUNICIPALITY = 'MunicipalityName'

    AGE_GROUP = 'AgeGroup'
    GENDER = 'Gender'
    RACE = 'Race'

    DEAD = 'Dead'
    COMORBIDITIES_AND_RISK_FACTORS = 'ComorbiditiesAndRiskFactors'
    FINAL_CLASSIFICATION_OF_CASE = 'FinalClassificationOfCase'
    HOSPITALIZATION = 'Hospitalization'

    # SIM integration
    # NOTE(abby): This Age dimension has a conflict in the pt translation file with
    # mz's Age dimension, use BrazilAge here.
    AGE = 'BrazilAge'
    MOTHERS_AGE = 'MothersAge'
    SCHOOLING = 'Schooling'
    MOTHERS_SCHOOLING = 'MothersSchooling'
    OCCUPATION_TITLE = 'OccupationTitle'
    OCCUPATION_FAMILY = 'OccupationFamily'
    OCCUPATION_SUBGROUP = 'OccupationSubgroup'
    OCCUPATION_PRINCIPAL_SUBGROUP = 'OccupationPrincipalSubgroup'
    OCCUPATION_GROUP = 'OccupationGroup'
    MOTHERS_OCCUPATION_TITLE = 'MothersOccupationTitle'
    MOTHERS_OCCUPATION_FAMILY = 'MothersOccupationFamily'
    MOTHERS_OCCUPATION_SUBGROUP = 'MothersOccupationSubgroup'
    MOTHERS_OCCUPATION_PRINCIPAL_SUBGROUP = 'MothersOccupationPrincipalSubgroup'
    MOTHERS_OCCUPATION_GROUP = 'MothersOccupationGroup'
    PLACE_OF_DEATH = 'PlaceOfDeath'
    MEDICAL_CARE = 'MedicalCare'
    IS_AUTOPSY = 'IsAutopsy'
    VIOLENT_DEATH_TYPE = 'ViolentDeathType'
    IS_WORK_RELATED = 'IsWorkRelated'
    INFO_SOURCE = 'InfoSource'
    NUMBER_LIVING_CHILDREN = 'NumberLivingChildren'
    NUMBER_DECEASED_CHILDREN = 'NumberDeceasedChildren'
    PREGNANCY_TYPE = 'PregnancyType'
    GESTATIONAL_PHASE = 'GestationalPhase'
    GESTATION_WEEKS = 'GestationWeeks'
    PREGNANCY_KIND = 'PregnancyKind'
    MOMENT_OF_CHILDBIRTH = 'MomentOfChildbirth'
    CAUSES_A_TITLE = 'CausesATitle'
    CAUSES_A_PARENT = 'CausesAParent'
    CAUSES_A_CATEGORY_1 = 'CausesACategory1'
    CAUSES_A_CATEGORY_2 = 'CausesACategory2'
    CAUSES_A_CATEGORY_3 = 'CausesACategory3'
    CAUSES_A_CATEGORY_4 = 'CausesACategory4'
    CAUSES_B_TITLE = 'CausesBTitle'
    CAUSES_B_PARENT = 'CausesBParent'
    CAUSES_B_CATEGORY_1 = 'CausesBCategory1'
    CAUSES_B_CATEGORY_2 = 'CausesBCategory2'
    CAUSES_B_CATEGORY_3 = 'CausesBCategory3'
    CAUSES_B_CATEGORY_4 = 'CausesBCategory4'
    CAUSES_C_TITLE = 'CausesCTitle'
    CAUSES_C_PARENT = 'CausesCParent'
    CAUSES_C_CATEGORY_1 = 'CausesCCategory1'
    CAUSES_C_CATEGORY_2 = 'CausesCCategory2'
    CAUSES_C_CATEGORY_3 = 'CausesCCategory3'
    CAUSES_C_CATEGORY_4 = 'CausesCCategory4'
    CAUSES_D_TITLE = 'CausesDTitle'
    CAUSES_D_PARENT = 'CausesDParent'
    CAUSES_D_CATEGORY_1 = 'CausesDCategory1'
    CAUSES_D_CATEGORY_2 = 'CausesDCategory2'
    CAUSES_D_CATEGORY_3 = 'CausesDCategory3'
    CAUSES_D_CATEGORY_4 = 'CausesDCategory4'
    CAUSES_2_TITLE = 'Causes2Title'
    CAUSES_2_PARENT = 'Causes2Parent'
    CAUSES_2_CATEGORY_1 = 'Causes2Category1'
    CAUSES_2_CATEGORY_2 = 'Causes2Category2'
    CAUSES_2_CATEGORY_3 = 'Causes2Category3'
    CAUSES_2_CATEGORY_4 = 'Causes2Category4'


class LocationTypeEnum(Enum):
    NATION = 1
    REGION = 2
    STATE = 3
    HEALTH_REGION = 4
    MUNICIPALITY = 5


LOCATION_TYPES = set(location_type.name for location_type in LocationTypeEnum)

HIERARCHICAL_DIMENSIONS = [
    Dimension.REGION,
    Dimension.STATE,
    Dimension.HEALTH_REGION,
    Dimension.MUNICIPALITY,
]
DIMENSION_PARENTS = {
    Dimension.STATE: [Dimension.REGION],
    Dimension.HEALTH_REGION: [Dimension.REGION, Dimension.STATE],
    Dimension.MUNICIPALITY: [
        Dimension.REGION,
        Dimension.STATE,
        Dimension.HEALTH_REGION,
    ],
}

NON_HIERARCHICAL_DIMENSIONS = []

# pylint: disable=invalid-name
BrazilCovidDimensionFactory = DimensionFactory(
    HIERARCHICAL_DIMENSIONS,
    NON_HIERARCHICAL_DIMENSIONS,
    RAW_PREFIX,
    CLEANED_PREFIX,
    CANONICAL_PREFIX,
)

BaseRowType = BaseRowFactory(
    Dimension, HIERARCHICAL_DIMENSIONS, DIMENSION_PARENTS
)
DimensionFactoryType = BrazilCovidDimensionFactory


class PipelineArgs:
    @classmethod
    def add_source_processing_args(cls):
        Flags.PARSER.add_argument(
            '--output_file', type=str, required=True, help='Processed data output file'
        )
        Flags.PARSER.add_argument(
            '--location_list',
            type=str,
            required=True,
            help='Output list of region/district/facility for matching',
        )
        Flags.PARSER.add_argument(
            '--field_list',
            type=str,
            required=True,
            help='Output list of all possible fields with data for this source',
        )
