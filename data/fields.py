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
VARIANT = 'variant'
LINEAGE = 'lineage'
NUMBER_DETECTIONS_VARIANT = 'number_detections_variant'
PERCENT_VARIANT = 'percent_variant'
COUNTRY_ISO = 'COUNTRY_ISO'

VACCINE_FIELDS = [COUNTRY_REGION, DATE_RECORDED, TOTAL_VACCINATIONS,
                  FULLY_VACCINATED, PARTIALLY_VACCINATED]
VARIANT_FIELDS = [COUNTRY_REGION, DATE_RECORDED, LINEAGE, NUMBER_DETECTIONS_VARIANT, PERCENT_VARIANT]

ALL_FIELDS = list(set(CSSE_FIELDS + VACCINE_FIELDS + VARIANT_FIELDS + [NEW_CASES, NEW_DEATHS, UNVACCINATED,
                                                                       CASES_PER_THOUSAND, DEATHS_PER_THOUSAND,
                                                                       PERCENTAGE_VACCINATED, COUNTRY_ISO]))
