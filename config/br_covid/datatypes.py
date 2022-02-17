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
    Dimension, HIERARCHICAL_DIMENSIONS, DIMENSION_PARENTS, NON_HIERARCHICAL_DIMENSIONS
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
