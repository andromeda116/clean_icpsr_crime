import pandas as pd
import os

def get_outliers(fl_path, st_dev, cols, op_path):
    df = pd.read_csv(fl_path)
    fl_name = os.path.basename(fl_path).split('.')[0]

    df_z = df[cols].groupby('ORI').transform(lambda group: (group - group.mean()).div(group.std()))

    for col in list(df_z):
        outliers = df_z[f'{col}'].abs() > st_dev
        outliers_df = df[outliers]
        print(f'{fl_name}: ', set(outliers_df.ORI).__len__())
        outliers_df.to_csv(f'/{op_path}/{fl_name}_{col}_within_ori_3z_ol.csv', index=False)


#df = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/crime_icpsr_90_15.csv')

'''  rep1_12_gte_10k  '''

get_outliers(fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep1_12_gte_10k.csv',
             st_dev=3, cols=['ORI', 'rep_month_flag_total_true'],
             op_path='/Users/salma/Research/clean_icpsr_crime/data/cleaned_check')


get_outliers(fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep1_12_large_cities.csv',
             st_dev=3, cols=['ORI', 'rep_month_flag_total_true'],
             op_path='/Users/salma/Research/clean_icpsr_crime/data/cleaned_check')


get_outliers(fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep1_12_medium_cities.csv',
             st_dev=3, cols=['ORI', 'rep_month_flag_total_true'],
             op_path='/Users/salma/Research/clean_icpsr_crime/data/cleaned_check')


get_outliers(fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep1_12_small_cities.csv',
             st_dev=3, cols=['ORI', 'rep_month_flag_total_true'],
             op_path='/Users/salma/Research/clean_icpsr_crime/data/cleaned_check')



'''  rep1_10_11_12_gte_10k  '''

get_outliers(fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep_1_10_11_12_gte_10k.csv',
             st_dev=3, cols=['ORI', 'rep_month_flag_total_true'],
             op_path='/Users/salma/Research/clean_icpsr_crime/data/cleaned_check')


get_outliers(fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep_1_10_11_12_large_cities.csv',
             st_dev=3, cols=['ORI', 'rep_month_flag_total_true'],
             op_path='/Users/salma/Research/clean_icpsr_crime/data/cleaned_check')


get_outliers(fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep_1_10_11_12_medium_cities.csv',
             st_dev=3, cols=['ORI', 'rep_month_flag_total_true'],
             op_path='/Users/salma/Research/clean_icpsr_crime/data/cleaned_check')


get_outliers(fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep_1_10_11_12_small_cities.csv',
             st_dev=3, cols=['ORI', 'rep_month_flag_total_true'],
             op_path='/Users/salma/Research/clean_icpsr_crime/data/cleaned_check')



'''  rep12_26_gte_10k  -- wont be any outliers coz all values 26'''