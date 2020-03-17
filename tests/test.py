import pandas as pd

df = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/crime_data/crime_data_req_monthly/consolidated/icpsr_cr_monthly_90_15_pop_gte_10k.csv')

print(set(df['ORI']).__len__())

df1 = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/crime_data/crime_data_req_monthly/consolidated/icpsr_cr_monthly_90_15_pop_gte_10k_fbi_month_zero_or_not_rep_fixed_neg_fixed_tot_cr.csv')

df1_req = df1.query('rep_month_flag_total_true == 12')

print(set(df1_req['ORI']).__len__())

print(df1_req['YEAR'].value_counts())