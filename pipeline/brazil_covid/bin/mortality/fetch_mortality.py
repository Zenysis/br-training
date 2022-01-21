#!/usr/bin/env python

import gzip
import json
import sys
import time
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from pylib.base.flags import Flags

# List cities with more than 0 cases
CITIES_URL = 'https://transparencia.registrocivil.org.br/api/cities'

# List top cities
COVID_CITIES_URL = (
    'https://transparencia.registrocivil.org.br/api/covid-cities?total=30'
)

BR_STATE_CODES = [
    'AC',
    'AL',
    'AP',
    'AM',
    'BA',
    'CE',
    'DF',
    'ES',
    'GO',
    'MA',
    'MT',
    'MS',
    'MG',
    'PA',
    'PB',
    'PR',
    'PE',
    'PI',
    'RJ',
    'RN',
    'RS',
    'RO',
    'RR',
    'SC',
    'SP',
    'SE',
    'TO',
]

# Cities that we should include no matter what
MANUALLY_ENUMERATED_CITIES = [
    {"uf": "SP", "name": "São Paulo", "id": 1},
    {"uf": "SP", "name": "São Bernardo do Campo", "id": 2},
    {"uf": "SP", "name": "Barueri", "id": 6},
    {"uf": "SP", "name": "Campinas", "id": 8},
    {"uf": "SP", "name": "Guaruja", "id": 43},
    {"uf": "SP", "name": "Guarulhos", "id": 47},
    {"uf": "SP", "name": "Sorocaba", "id": 53},
    {"uf": "SP", "name": "Osasco", "id": 75},
    {"uf": "AC", "name": "Rio Branco", "id": 650},
    {"uf": "AL", "name": "Maceio", "id": 670},
    {"uf": "AM", "name": "Manaus", "id": 771},
    {"uf": "AP", "name": "Macapa", "id": 833},
    {"uf": "BA", "name": "Porto Seguro", "id": 852},
    {"uf": "BA", "name": "Salvador", "id": 855},
    {"uf": "CE", "name": "Fortaleza", "id": 1280},
    {"uf": "DF", "name": "Brasília", "id": 1457},
    {"uf": "ES", "name": "Vitória", "id": 1462},
    {"uf": "GO", "name": "Goiânia", "id": 1574},
    {"uf": "MA", "name": "São Luís", "id": 1791},
    {"uf": "MS", "name": "Campo Grande", "id": 2005},
    {"uf": "MT", "name": "Cuiaba", "id": 2104},
    {"uf": "PA", "name": "Belém", "id": 2241},
    {"uf": "PB", "name": "Joao Pessoa", "id": 2366},
    {"uf": "PE", "name": "Recife", "id": 2603},
    {"uf": "PI", "name": "Teresina", "id": 2772},
    {"uf": "PR", "name": "Curitiba", "id": 3009},
    {"uf": "RN", "name": "Natal", "id": 3397},
    {"uf": "RO", "name": "Porto Velho", "id": 3565},
    {"uf": "RR", "name": "Boa Vista", "id": 3621},
    {"uf": "RS", "name": "Porto Alegre", "id": 3647},
    {"uf": "SC", "name": "Florianopolis", "id": 4152},
    {"uf": "SE", "name": "Aracaju", "id": 4426},
    {"uf": "TO", "name": "Palmas", "id": 4633},
    {"uf": "RJ", "name": "Rio de Janeiro", "id": 4646},
    {"uf": "RJ", "name": "Petropolis", "id": 4648},
    {"uf": "RJ", "name": "Volta Redonda", "id": 4650},
    {"uf": "RJ", "name": "Nova Iguaçu", "id": 4653},
    {"uf": "RJ", "name": "Niteroi", "id": 4657},
    {"uf": "RJ", "name": "Duque de Caxias", "id": 4658},
    {"uf": "MG", "name": "Belo Horizonte", "id": 4750},
]

START_DATE = '2019-01-01'
END_DATE = datetime.today().strftime('%Y-%m-%d')

retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=['HEAD', 'GET', 'OPTIONS'],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount('https://', adapter)
http.mount('http://', adapter)


def get_disease_count(disease, obj):
    if disease not in obj:
        return 0
    if len(obj[disease]) < 1:
        return 0
    return obj[disease][0]['total']


class Scraper:
    def __init__(self, token):
        print('Using XSRF token', token)
        self.token = token

    def get_json(self, url):
        print('Requesting', url)
        resp = http.get(
            url,
            headers={
                'X-XSRF-TOKEN': self.token,
                'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0',
            },
        )

        return json.loads(resp.text)

    def scrape(self, output_path):
        print('Loading cities list')
        # resp = self.get_json(CITIES_URL)
        # cities = resp['cities']

        # resp = self.get_json(COVID_CITIES_URL)
        # cities = resp
        # cities.extend(MANUALLY_ENUMERATED_CITIES)
        cities = MANUALLY_ENUMERATED_CITIES[:]
        print('Cities:', cities)

        # Add states manually...
        for state_code in BR_STATE_CODES:
            cities.append({'uf': state_code, 'id': 'all', 'name': 'all'})

        count = 0
        with gzip.open(output_path, 'wt') as f_out:
            seen_city_ids = set()
            for city in cities:
                state = city.get('uf', city.get('state'))
                city_id = str(city.get('id', city.get('city_id')))
                if city_id in seen_city_ids and city_id != 'all':
                    # Already processed this city
                    continue
                seen_city_ids.add(city_id)

                # if city_id < 592:
                #    continue
                city_name = city.get('name', city.get('nome'))

                for i in range(3):
                    try:
                        print('Scraping', city_name, city_id, state)
                        start_time = time.time()
                        url = f'https://transparencia.registrocivil.org.br/api/covid-covid-registral?start_date={START_DATE}&end_date={END_DATE}&state={state}&city_id={city_id}&chart=chart5&places[]=HOSPITAL&places[]=DOMICILIO&places[]=VIA_PUBLICA&places[]=OUTROS&cor_pele=I&diffCity=false'
                        raw_timeseries = self.get_json(url)['chart']

                        for date, vals in raw_timeseries.items():
                            record = {
                                'date': date,
                                'state': state,
                                'city': city_name,
                                'city_id': city_id,
                                'pneumonia': get_disease_count('PNEUMONIA', vals),
                                'outras': get_disease_count('OUTRAS', vals),
                                'septicemia': get_disease_count('SEPTICEMIA', vals),
                                'respiratory_insufficiency': get_disease_count(
                                    'INSUFICIENCIA_RESPIRATORIA', vals
                                ),
                                'covid': get_disease_count('COVID', vals),
                                'indeterminate': get_disease_count(
                                    'INDETERMINADA', vals
                                ),
                                'sars': get_disease_count('SRAG', vals),
                            }
                            f_out.write(json.dumps(record) + '\n')
                            f_out.flush()
                            count += 1
                        break
                    except AttributeError:
                        # Sometimes the API barfs and returns {'chart': []}
                        print('Retrying...')
                        continue

                print('Got', count, 'records now')
                print('Took %f seconds' % (time.time() - start_time))

        print('Done.')


def main():
    Flags.PARSER.add_argument(
        '--token_file',
        type=str,
        required=True,
        help='JSON file containing scraped token',
    )
    Flags.PARSER.add_argument(
        '--output_file', type=str, required=True, help='csv output'
    )
    Flags.InitArgs()

    token = json.load(open(Flags.ARGS.token_file, 'r'))[0]['token']

    scrappy = Scraper(token)
    scrappy.scrape(Flags.ARGS.output_file)
    return 0


if __name__ == '__main__':
    sys.exit(main())
