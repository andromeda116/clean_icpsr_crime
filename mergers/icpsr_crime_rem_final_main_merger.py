import pandas as pd

final_main = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/final_crime_other_main/final_main.csv')

#print(list(final_main))
final_main_sans_crime = final_main.drop(['murder', 'rape', 'robbery', 'aggravated_assault', 'simple_assault', 'burglary',
                                        'larceny', 'auto_theft', 'total_main_crime', 'violent_crime', 'property_crime',
                                        'murder_rate', 'rape_rate', 'robbery_rate', 'aggravated_assault_rate', 'simple_assault_rate',
                                        'burglary_rate', 'larceny_rate', 'auto_theft_rate', 'violent_crime_rate',
                                        'property_crime_rate', 'total_main_crime_rate', 'manslaughter', 'prison_occupancy_count',
                                        'jail_occupancy_count', 'perc_felonies', 'perc_misdemeanors'], axis=1)

final_icpsr_clnd_cr = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/crime_icpsr_90_15_req_93_fixed_events_inflated_fxd.csv')

final_rem_main_icpsr_cleaned_cr = final_main_sans_crime.merge(final_icpsr_clnd_cr, on=['ORI', 'YEAR'])

final_rem_main_icpsr_cleaned_cr.to_csv('/Users/salma/Research/clean_icpsr_crime/data/final_crime_other_main/crime_icpsr_clnd_rem_main.csv', index=False)

print(list(final_rem_main_icpsr_cleaned_cr))