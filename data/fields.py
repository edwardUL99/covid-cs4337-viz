"""
This module outlines constant variables and the names of important fields
"""
# Fields in the CSSE dataset
COUNTRY_REGION = 'Country/Region'
CONFIRMED = 'Confirmed'
DEATHS = 'Deaths'
CSSE_FIELDS = [COUNTRY_REGION, CONFIRMED, DEATHS]

# Fields in the vaccinations dataset
TOTAL_VACCINATIONS = 'Doses'
DAILY_VACCINATIONS = 'daily_vaccinations'
FULLY_VACCINATED = 'people_fully_vaccinated'
TOTAL_BOOSTERS = 'total_boosters'
BOOSTERS_PER_HUNDRED = 'total_boosters_per_hundred'

# Fields in testing data
TOTAL_TESTS = 'Tests'
DAILY_TESTS = 'DailyTests'
POSITIVE_RATE = 'PositiveRate'

# The following fields are custom fields added in that are not in the original data
DATE_RECORDED = 'DateRecorded'
WEEK = 'Week'
NEW_CASES = 'NewCases'
NEW_DEATHS = 'NewDeaths'
POPULATION = 'Population'
PARTIALLY_VACCINATED = 'people_partially_vaccinated'
UNVACCINATED = 'Unvaccinated'
INCIDENT_RATE = 'IncidentRate'
DEATH_RATE = 'DeathRate'
PERCENTAGE_VACCINATED = 'PercentageVaccinated'
VARIANT = 'variant'
NUMBER_DETECTIONS_VARIANT = 'number_detections_variant'
PERCENT_VARIANT = 'percent_variant'

VACCINE_FIELDS = [COUNTRY_REGION, DATE_RECORDED, TOTAL_VACCINATIONS,
                  FULLY_VACCINATED, TOTAL_BOOSTERS, PARTIALLY_VACCINATED, BOOSTERS_PER_HUNDRED]
VARIANT_FIELDS = [COUNTRY_REGION, DATE_RECORDED, VARIANT, NUMBER_DETECTIONS_VARIANT, PERCENT_VARIANT]
TESTING_FIELDS = [COUNTRY_REGION, DATE_RECORDED, TOTAL_TESTS, DAILY_TESTS, POSITIVE_RATE]

ALL_FIELDS = list(set(CSSE_FIELDS + VACCINE_FIELDS + VARIANT_FIELDS + [NEW_CASES, NEW_DEATHS, UNVACCINATED,
                                                                       INCIDENT_RATE, DEATH_RATE,
                                                                       PERCENTAGE_VACCINATED] + TESTING_FIELDS))
