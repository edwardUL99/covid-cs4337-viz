"""
This module outlines constant variables and the names of important fields
"""
# Fields in the CSSE dataset
COUNTRY_REGION = 'Country/Region'
CONFIRMED = 'Confirmed'
DEATHS = 'Deaths'
RECOVERED = 'Recovered'
ALL_FIELDS = [COUNTRY_REGION, CONFIRMED, DEATHS, RECOVERED]

# Fields in the vaccinations dataset found at https://www.kaggle.com/gpreda/covid-world-vaccination-progress
TOTAL_VACCINATIONS = 'total_vaccinations'
PEOPLE_VACCINATED = 'people_vaccinated'
DAILY_VACCINATIONS = 'daily_vaccinations'
FULLY_VACCINATED = 'people_fully_vaccinated'

# The following fields are custom fields added in that are not in the original data
DATE_RECORDED = 'DateRecorded'
WEEK = 'Week'
NEW_CASES = 'NewCases'

VACCINE_FIELDS = [COUNTRY_REGION, DATE_RECORDED, TOTAL_VACCINATIONS,
                  PEOPLE_VACCINATED, DAILY_VACCINATIONS, FULLY_VACCINATED]
