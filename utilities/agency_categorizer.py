import pandas as pd
import numpy as np

from custom_utilities import initial_core_file_retriever as ini_core_retr


def categorize_agencies(df, op_path, fl_name, pop_mean_calc=False):

    df_population_mn = df

    if pop_mean_calc:
        # groupby ORI and calculate mean of population values from 90-15
        df_ORI_grpd = df.groupby('ORI').agg({'population': np.mean}).reset_index()
        df_ORI_grpd.rename({'population': 'population_mean'}, axis=1, inplace=True)

        # calculate dm of population var
        df_ori_yr_pop = df.loc[:, ['ORI', 'YEAR', 'population']]
        df_ori_yr_pop['population_dm'] = df_ori_yr_pop.groupby('ORI').population.transform(
            lambda x: x - x.mean())

        # get mean and dm vars of population in 1 file by merging
        df_ori_yr_pop_mn_dmn = df_ori_yr_pop.merge(df_ORI_grpd, on='ORI')
        df_ori_yr_pop_mn_dmn.to_csv(f'{op_path}/{fl_name}_ori_yr_mn_dmn.csv', index=False)

        # merge df_ORI_grpd with df so that population mean for a given ORI will be associated with that ORI for 90-15
        df_population_mn = df.merge(df_ORI_grpd, on='ORI')

    # get only those ORIs whose population_mean is greater than or equal to 10k
    # without reset_index, the original index with actual locations of records in df_grpd df is used as the index for
    # ORIs_ab_10k_mean. If we don't take drop=True, the current index is retained as a new column.
    df_gte_10k = df_population_mn.query('population_mean >= 10000').reset_index(drop=True)
    df_gte_10k.to_csv(f'{op_path}/{fl_name}_gte_10k.csv', index=False)

    # get the core vars for gte 10k
    ini_core_retr.get_top_level_vars(fl_path=f'{op_path}/{fl_name}_gte_10k.csv',
                                    op_path=op_path,
                                    fl_name=f'{fl_name}_gte_10k_core')

    # get only those ORIs whose population_mean is less than 10k
    df_lt_10k = df_population_mn.query('population_mean < 10000').reset_index(drop=True)
    df_lt_10k.to_csv(f'{op_path}/{fl_name}_lt_10k.csv', index=False)
    print('df_lt_10k: ', set(df_lt_10k.ORI).__len__())


    small_size_cities = df_population_mn.query('population_mean >= 10000 & population_mean < 50000').reset_index(drop=True)
    small_size_cities.to_csv(f'{op_path}/{fl_name}_small_cities.csv', index=False)
    print('small_size_cities: ', set(small_size_cities.ORI).__len__())

    # get the core vars for small cities
    ini_core_retr.get_top_level_vars(fl_path = f'{op_path}/{fl_name}_small_cities.csv',
                                     op_path = op_path,
                                     fl_name = f'{fl_name}_small_cities_core')


    med_size_cities = df_population_mn.query('population_mean >= 50000 & population_mean < 100000').reset_index(drop=True)
    med_size_cities.to_csv(f'{op_path}/{fl_name}_medium_cities.csv', index=False)
    print('med_size_cities: ', set(med_size_cities.ORI).__len__())

    ini_core_retr.get_top_level_vars(fl_path=f'{op_path}/{fl_name}_medium_cities.csv',
                                     op_path = op_path,
                                     fl_name = f'{fl_name}_medium_cities_core')

    large_size_cities = df_population_mn.query('population_mean >= 100000 ').reset_index(drop=True)
    large_size_cities.to_csv(f'{op_path}/{fl_name}_large_cities.csv', index=False)
    print('large_size_cities: ', set(large_size_cities.ORI).__len__())

    ini_core_retr.get_top_level_vars(fl_path=f'{op_path}/{fl_name}_large_cities.csv',
                                     op_path = op_path,
                                     fl_name = f'{fl_name}_large_cities_core')


if __name__ == "__main__":

    fnl_df = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/incarceration/crime_icpsr_clnd_rem_main_incarc_cnts_all_rates_renamed_arrest_vars.csv')

    categorize_agencies(df=fnl_df, op_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories',
                        fl_name='final_main_icpsr_crime',
                        pop_mean_calc = True)