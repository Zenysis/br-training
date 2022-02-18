from config.br_covid.datatypes import (
    Dimension,
    DIMENSION_PARENTS,
    HIERARCHICAL_DIMENSIONS,
)
from data.query.models.granularity import GranularityExtraction
from db.druid.js_formulas.week_extraction import CDC_EPI_WEEK_EXTRACTION_FORMULA
from models.python.config.calendar_settings import CalendarSettings, DateOption


# Geo fields from least specific to most specific.
GEO_FIELD_ORDERING = HIERARCHICAL_DIMENSIONS

# Given a query on a field, which fields should we ask Druid for?
# Unless otherwise specified, querying on a field will just ask for itself.
DIMENSION_SLICES = {
    dimension: [dimension] + parents for dimension, parents in DIMENSION_PARENTS.items()
}

DIMENSION_PARENTS = DIMENSION_PARENTS

# When the server starts, query for distinct values along these dimensions and
# build a geo hierarchy in memory.  Used to populate geo filter dropdown.
DISTINCT_GEOS_TO_QUERY = HIERARCHICAL_DIMENSIONS

DIMENSION_ID_MAP = {
    dimension: dimension.replace('Name', 'ID') for dimension in HIERARCHICAL_DIMENSIONS
}

# Map from whereType API query param to latlng fields.
GEO_TO_LATLNG_FIELD = {
    dimension: (dimension.replace('Name', 'Lat'), dimension.replace('Name', 'Lon'))
    for dimension in HIERARCHICAL_DIMENSIONS
}

# Dimension category mapping from parent name to list of dimensions. Used by AQT.
DIMENSION_CATEGORIES = [
    ('Geografia', GEO_FIELD_ORDERING),
    (
        'SIVEP',
        [
            Dimension.DEAD,
            Dimension.COMORBIDITIES_AND_RISK_FACTORS,
            Dimension.FINAL_CLASSIFICATION_OF_CASE,
            Dimension.HOSPITALIZATION,
        ],
    ),
    ('Outros', [Dimension.AGE_GROUP, Dimension.GENDER, Dimension.RACE]),
]

# List of queryable dimensions.
DIMENSIONS = [
    dimension for _, dimensions in DIMENSION_CATEGORIES for dimension in dimensions
]

CALENDAR_SETTINGS = CalendarSettings.create_default(enable_all_granularities=True)

# Translations
CALENDAR_SETTINGS.granularity_settings.day.name = 'Dia'
CALENDAR_SETTINGS.granularity_settings.week.name = 'Semana'
CALENDAR_SETTINGS.granularity_settings.month.name = 'Mês'
CALENDAR_SETTINGS.granularity_settings.quarter.name = 'Quarto'
CALENDAR_SETTINGS.granularity_settings.year.name = 'Ano'

# Date Extraction
CALENDAR_SETTINGS.granularity_settings.day_of_year.name = 'Dia do Ano'
CALENDAR_SETTINGS.granularity_settings.week_of_year.name = 'Semana do Ano'
CALENDAR_SETTINGS.granularity_settings.month_of_year.name = 'Mês do Ano'
CALENDAR_SETTINGS.granularity_settings.quarter_of_year.name = 'Quarto do Ano'

# NOTE(stephen): BR COVID uses the CDC epi week definition (starting on Sunday). They
# also do not want to use the W## prefix since that is language specific.
CALENDAR_SETTINGS.granularity_settings.epi_week = DateOption(
    CALENDAR_SETTINGS.granularity_settings.epi_week.id,
    'Semana Epidemiológica',
    'cc YYYY',
    'cc',
)
CALENDAR_SETTINGS.granularity_settings.epi_week_of_year = DateOption(
    CALENDAR_SETTINGS.granularity_settings.epi_week_of_year.id,
    'Semana Epidemiológica do Ano',
    'cc',
)

# HACK(stephen): GIANT ENORMOUS HACK. DO NOT REPEAT THIS ANYWHERE ELSE. We don't have a
# great way of switching the `epi_week_of_year` granularity extraction to use the CDC
# epi week definition instead of the WHO one which is the default. This hack swaps it
# out for us.
# pylint: disable=protected-access
GranularityExtraction.EXTRACTION_MAP[
    'epi_week_of_year'
]._func = CDC_EPI_WEEK_EXTRACTION_FORMULA

# Mapping from Dimension ID to a comparator for sorting a list of dimensions,
# and a display ID used by the frontend to communicate what sort is being used
BACKEND_SORTS = {}


def get_data_key(data):
    '''Generate a unique aggregation key for a given set of data based
    on the location the data is representing.'''
    # NOTE(stephen): Need to OR with an empty string since the data object
    # can actually contain None as a value which breaks string operations
    return '__'.join([(data.get(f) or '') for f in GEO_FIELD_ORDERING]).lower()


# For each external alert type map the name to:
#   'data_dimension': the druid dimension used to store the external alert JSON data
#   'field_id': the druid field text id to store an alert, must also be an indicator
#   'druid_dimension': the druid dimension the alert is being triggered for
EXTERNAL_ALERT_TYPES = {}