#!/usr/bin/env python
# Takes the Brazil Locations API at
# https://servicodados.ibge.gov.br/api/docs/localidades and outputs static
# mapping files

import csv
import json

# Municipalities
with open('location_api_mappings.csv', 'w') as f_out:
    fieldnames = [
        'StateName',
        'StateCode',
        'StateId',
        'MunicipalityName',
        'MunicipalityId',
    ]
    writer = csv.DictWriter(f_out, fieldnames=fieldnames)
    writer.writeheader()

    locations = json.load(open('./municipios.json', 'r'))
    for location in locations:
        state = location['microrregiao']['mesorregiao']['UF']
        statename = state['nome']
        statecode = state['sigla']
        stateid = state['id']
        muniname = location['nome']
        muniid = location['id']
        writer.writerow(
            {
                'StateName': statename,
                'StateCode': statecode,
                'StateId': stateid,
                'MunicipalityName': muniname,
                'MunicipalityId': muniid,
            }
        )

print('Done.')
