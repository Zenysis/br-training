CALCULATED_INDICATOR_GROUP_DEFINITIONS = [
    {
        'groupId': 'Calculated Indicators',
        'groupText': 'Calculated Indicators',
        'groupTextShort': 'Calculated',
        'calculated_indicators': [
            {
                'formula': 'sinan_number_of_people_hospitalized_died_in_investigation + sinan_number_of_people_hospitalized_died_of_infl + sinan_number_of_people_hospitalized_other',
                'id': 'calculated_sivep_total_deaths',
                'text': 'SINAN: Óbitos Total',
                'enableDimensions': ['StateName', 'MunicipalityName'],
                'definition': 'Soma das mortes em SINAN',
            },
            {
                'formula': 'sivep_number_of_people_hospitalized_died_of_infl',
                'id': 'calculated_sinan_total_deaths',
                'text': 'SIVEP: Óbitos Total',
                'enableDimensions': ['StateName', 'MunicipalityName'],
                'definition': 'Soma das mortes em SIVEP',
            },
            {
                'formula': 'covid_covid19_teste_negativo + covid_teste_covid_19_positivo + covid_covid19_teste_noinfo',
                'id': 'calculated_covid_tests_collected',
                'text': 'Teste COVID-19 coletado',
                'enableDimensions': ['StateName', 'MunicipalityName'],
                'definition': 'Soma dos testes COVID-19',
            },
        ],
    }
]
