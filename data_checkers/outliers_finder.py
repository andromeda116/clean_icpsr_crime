import pandas as pd

pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 500)

if __name__ == "__main__":
    from custom_utilities import outliers_finder as ol_finder


## get outliers within ORI
def get_ol_within_ori():

    ol_finder.get_outliers(fl_path = '/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_large_cities_core_counts.csv',
                 op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/large_agencies/by_ori/counts',
                 var_type='counts',
                 by_ori=True)


    ol_finder.get_outliers(fl_path = '/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_large_cities_core_rates.csv',
                 op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/large_agencies/by_ori/rates',
                 var_type='rates',
                 by_ori=True)


    ol_finder.get_outliers(fl_path = '/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_medium_cities_core_counts.csv',
                 op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/medium_agencies/by_ori/counts',
                 var_type='counts',
                 by_ori=True)


    ol_finder.get_outliers(fl_path = '/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_medium_cities_core_rates.csv',
                 op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/medium_agencies/by_ori/rates',
                 var_type='rates',
                 by_ori=True)

    ol_finder.get_outliers(fl_path = '/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_small_cities_core_counts.csv',
                 op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/small_agencies/by_ori/counts',
                 var_type='counts',
                 by_ori=True)


    ol_finder.get_outliers(fl_path = '/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_small_cities_core_rates.csv',
                 op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/small_agencies/by_ori/rates',
                 var_type='rates',
                 by_ori=True)


#get_ol_within_ori()


# ### ol_finder.get_outliers across the dataset
def get_ol_across():
    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_large_cities_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/large_agencies/overall/counts',
        var_type='counts')

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_large_cities_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/large_agencies/overall/rates',
        var_type='rates')

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_medium_cities_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/medium_agencies/overall/counts',
        var_type='counts')

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_medium_cities_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/medium_agencies/overall/rates',
        var_type='rates')

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_small_cities_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/small_agencies/overall/counts',
        var_type='counts')

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_small_cities_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/small_agencies/overall/rates',
        var_type='rates')


#get_ol_across()


def get_ol_fnl_gte_10k():
    # by ori
    ol_finder.get_outliers(
        fl_path = '/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/fnl_mn_gte_10k/by_ori/counts',
        var_type='counts',
        by_ori=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/fnl_mn_gte_10k/by_ori/rates',
        var_type='rates',
        by_ori=True)

    # overall
    ol_finder.get_outliers(
        fl_path = '/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/fnl_mn_gte_10k/overall/counts',
        var_type='counts')

    ol_finder.get_outliers(
        fl_path = '/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers/fnl_mn_gte_10k/overall/rates',
        var_type='rates')


#get_ol_fnl_gte_10k()


def get_ol_fnl_gte_10k_rep12_26():
    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep12_26_large_cities_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep12_26/large_agencies/by_ori/counts',
        var_type='counts',
        by_ori=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep12_26_large_cities_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep12_26/large_agencies/by_ori/rates',
        var_type='rates',
        by_ori=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep12_26_medium_cities_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep12_26/medium_agencies/by_ori/counts',
        var_type='counts',
        by_ori=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep12_26_medium_cities_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep12_26/medium_agencies/by_ori/rates',
        var_type='rates',
        by_ori=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep12_26_small_cities_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep12_26/small_agencies/by_ori/counts',
        var_type='counts',
        by_ori=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep12_26_small_cities_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep12_26/small_agencies/by_ori/rates',
        var_type='rates',
        by_ori=True)


# get_ol_fnl_gte_10k_rep12_26()


def get_ol_fnl_gte_10k_rep_1_12():
    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep1_12_large_cities_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/large_agencies/by_ori/counts',
        var_type='counts',
        by_ori=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep1_12_large_cities_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/large_agencies/by_ori/rates',
        var_type='rates',
        by_ori=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep1_12_medium_cities_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/medium_agencies/by_ori/counts',
        var_type='counts',
        by_ori=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep1_12_medium_cities_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/medium_agencies/by_ori/rates',
        var_type='rates',
        by_ori=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep1_12_small_cities_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/small_agencies/by_ori/counts',
        var_type='counts',
        by_ori=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep1_12_small_cities_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/small_agencies/by_ori/rates',
        var_type='rates',
        by_ori=True)


# get_ol_fnl_gte_10k_rep_1_12()



def get_ol_fnl_gte_10k_rep_1_12_clean1():
    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_rep1_12_gte_10k_0_cr_drpd_0_main_arrests_repl_with_blanks_large_cities_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/large_agencies/by_ori/counts',
        var_type='counts',
        by_ori=True,
        lower_end_ol=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_rep1_12_gte_10k_0_cr_drpd_0_main_arrests_repl_with_blanks_large_cities_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/large_agencies/by_ori/rates',
        var_type='rates',
        by_ori=True,
        lower_end_ol=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_rep1_12_gte_10k_0_cr_drpd_0_main_arrests_repl_with_blanks_medium_cities_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/medium_agencies/by_ori/counts',
        var_type='counts',
        by_ori=True,
        lower_end_ol=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_rep1_12_gte_10k_0_cr_drpd_0_main_arrests_repl_with_blanks_medium_cities_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/medium_agencies/by_ori/rates',
        var_type='rates',
        by_ori=True,
        lower_end_ol=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_rep1_12_gte_10k_0_cr_drpd_0_main_arrests_repl_with_blanks_small_cities_core_counts.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/small_agencies/by_ori/counts',
        var_type='counts',
        by_ori=True,
        lower_end_ol=True)

    ol_finder.get_outliers(
        fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_rep1_12_gte_10k_0_cr_drpd_0_main_arrests_repl_with_blanks_small_cities_core_rates.csv',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/small_agencies/by_ori/rates',
        var_type='rates',
        by_ori=True,
        lower_end_ol=True)


get_ol_fnl_gte_10k_rep_1_12_clean1()

