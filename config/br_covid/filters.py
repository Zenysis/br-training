from config.br_covid.datatypes import BaseRowType, Dimension, HIERARCHICAL_DIMENSIONS

# Geography filter dimensions are required. They are hierarchical, ordered from
# biggest to smallest.
_GEOGRAPHY_FILTER_DIMENSIONS = ['_all'] + HIERARCHICAL_DIMENSIONS

# Map from filter ID to an ordered list of dimensions that will display in the
# filter dropdown.
FILTER_DIMENSIONS = {
    'geography': _GEOGRAPHY_FILTER_DIMENSIONS,
    'other': ['_all', *BaseRowType.UNMAPPED_KEYS],
    'source': ['_all', 'source'],
}

# Configuration of filters for public portal.
PUBLIC_FILTER_DIMENSIONS = {}

# Dimensions that we are able to restrict querying on
# No values now because we want to give public users access to everything.
AUTHORIZABLE_DIMENSIONS = set()
