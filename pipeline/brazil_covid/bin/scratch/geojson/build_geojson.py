#!/usr/bin/env python
import csv
import json
import sys

from pylib.base.flags import Flags

from data.pipeline.gis.geojson_builder import GeojsonBuilder, GeojsonFeature
from log import LOG

MUNICIPALITIES_TO_SKIP = ('4300001', '4300002')  # Lagoa Mirim  # Lagoa dos Patos


def main():
    Flags.PARSER.add_argument(
        '--input_file', type=str, required=True, help='Input geojson file'
    )
    Flags.PARSER.add_argument(
        '--municipality_code_mapping_file',
        type=str,
        default='../../../static_data/mappings/municipality_code_mapping.csv',
        help='CSV containing a mapping from municipality code to all the geo hierarchy',
    )
    Flags.PARSER.add_argument(
        '--output_file', type=str, required=True, help='Output geojson file'
    )
    Flags.InitArgs()

    with open(Flags.ARGS.input_file) as input_file, open(
        Flags.ARGS.municipality_code_mapping_file
    ) as municipality_code_mapping_file:
        reader = csv.DictReader(municipality_code_mapping_file)
        municipality_mapping = {row.pop('MunicipalityCodeLong'): row for row in reader}

        builder = GeojsonBuilder()
        LOG.info('Loading municipality geojson file')
        input_geojson = json.load(input_file)
        LOG.info('Finished loading municipality geojson file')

        LOG.info('Processing features')
        feature_count = 0
        codes = set()
        for feature in input_geojson['features']:
            municipality_code = feature['properties']['CD_MUN']
            if municipality_code in MUNICIPALITIES_TO_SKIP:
                LOG.info('Skipping municipality: %s', municipality_code)
                continue
            codes.add(municipality_code)
            new_properties = municipality_mapping.get(municipality_code)
            assert new_properties, 'Municipality is missing! %s' % feature['properties']
            feature['properties'] = new_properties
            builder.add_feature(GeojsonFeature.from_dict(feature))
            feature_count += 1

        LOG.info('FInished processing features')
        LOG.info('Simplifying features to try and improve file size')
        builder.simplify(0.003)
        LOG.info('Writing output geojson')
        builder.write_geojson_file(Flags.ARGS.output_file)
        print(set(municipality_mapping.keys()) - codes)
        assert feature_count == len(municipality_mapping), (
            'Feature count %s does not match expected municipality count %s'
            % (feature_count, len(municipality_mapping))
        )
        LOG.info('Finished everything!')
    return 0


if __name__ == '__main__':
    sys.exit(main())
