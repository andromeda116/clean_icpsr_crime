import pandas as pd
import numpy as np

##### get the crime predictors cleaned file to get the correct 93 crime data into the cleaned icpsr crime file.
##### coz the icpsr data has the inflated crime values for 93

icpsr_clnd_req = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/crime_icpsr_90_15_req.csv')


def fix_inflated_93_crime_disc():
    # get only 93 data
    icpsr_clnd_req_93 = icpsr_clnd_req.query('YEAR == 1993')

    crime_predictors_clnd = pd.read_excel('/Users/salma/Research/clean_icpsr_crime/data/ref_docs/Crime_Predictors_Cleaned.xlsx')

    #get only 93
    cr_predictors_cleaned_93 = crime_predictors_clnd.query('year == 1993')

    cr_predictors_cleaned_93_req = cr_predictors_cleaned_93.loc[:,['ori', 'murder_sum', 'rape_sum', 'robbery_sum',
                                                                   'agg_assault_sum', 'simple_assault_sum', 'burglary_sum',
                                                                   'larceny_sum', 'vehicle_sum']]

    icpsr_clnd_req_93_cr_pred_clnd_df = icpsr_clnd_req_93.merge(cr_predictors_cleaned_93_req,left_on=['ORI'], right_on=['ori'])

    # print(list(icpsr_clnd_req_93_cr_pred_clnd_df))
    icpsr_clnd_req_93_cr_pred_clnd_df.drop(['ori', 'murder', 'rape', 'robbery', 'agg_assault', 'simple_assault',
                                            'burglary', 'larceny', 'motor_vehicle_theft'], axis=1, inplace=True)

    icpsr_clnd_req_93_cr_pred_clnd_df.rename({'murder_sum': 'murder', 'rape_sum': 'rape', 'robbery_sum': 'robbery',
                                              'agg_assault_sum': 'agg_assault', 'simple_assault_sum': 'simple_assault',
                                              'burglary_sum': 'burglary', 'larceny_sum': 'larceny', 'vehicle_sum': 'motor_vehicle_theft'},
                                             axis=1, inplace=True)


    icpsr_clnd_req_without_93 = icpsr_clnd_req.query('YEAR != 1993')

    icpsr_clnd_req_93_fxd = pd.concat([icpsr_clnd_req_without_93, icpsr_clnd_req_93_cr_pred_clnd_df], sort=False, ignore_index=True)

    icpsr_clnd_req_93_fxd.sort_values(['ORI', 'YEAR'], inplace=True)

    return icpsr_clnd_req_93_fxd


icpsr_clnd_req_93_fxd_df = fix_inflated_93_crime_disc()
icpsr_clnd_req_93_fxd_df.to_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/crime_icpsr_90_15_req_93_fixed.csv', index=False)


def fix_events_inflated_crimes(df):
    # fix extreme murder values for NY and OK
    df['murder'] = np.where((df['ORI'] == 'NY03030')
                                            & (df['YEAR'] == 2001),  # Identifies the case to apply to
                                            660,  # This is the value that is inserted
                                        df['murder'])  # This is the column that is affected

    df['murder'] = np.where((df['ORI'] == 'OK05506')
                                            & (df['YEAR'] == 1995),  # For rows with column1 > 90
                                            58,  # We place column3 values
                                        df['murder'])  # In column two
    return df


icpsr_clnd_req_93_fxd_events_inflated_fxd = fix_events_inflated_crimes(icpsr_clnd_req_93_fxd_df)
icpsr_clnd_req_93_fxd_events_inflated_fxd.to_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/crime_icpsr_90_15_req_93_fixed_events_inflated_fxd.csv', index=False)