"""
This module outlines constant variables and the names of important fields
"""
# Fields in the CSSE dataset
COUNTRY_REGION = 'Country/Region'
CONFIRMED = 'Confirmed'
DEATHS = 'Deaths'
RECOVERED = 'Recovered'
ALL_FIELDS = [COUNTRY_REGION, CONFIRMED, DEATHS, RECOVERED]

# Fields in the vaccinations dataset
TOTAL_VACCINATIONS = 'Doses'
DAILY_VACCINATIONS = 'daily_vaccinations'
FULLY_VACCINATED = 'People_fully_vaccinated'
PARTIALLY_VACCINATED = 'People_partially_vaccinated'

# The following fields are custom fields added in that are not in the original data
DATE_RECORDED = 'DateRecorded'
WEEK = 'Week'
NEW_CASES = 'NewCases'
NEW_DEATHS = 'NewDeaths'
POPULATION = 'Population'
UNVACCINATED = 'Unvaccinated'
CASES_PER_THOUSAND = 'CasesPerThousand'
DEATHS_PER_THOUSAND = 'DeathsPerThousand'
PERCENTAGE_VACCINATED = 'PercentageVaccinated'

VACCINE_FIELDS = [COUNTRY_REGION, DATE_RECORDED, TOTAL_VACCINATIONS,
                  FULLY_VACCINATED, PARTIALLY_VACCINATED]
