import pandas as pd
import os

from custom_utilities import df_cleaner as df_clnr

## Get the directory in which files to merged are present


def merge_ol_files(dir_path, agency_type, op_path, var_types, ol_type, rep_flag_fl=False):

    os.chdir(dir_path)

    df_req = pd.DataFrame()

    count = 0

    for fl in os.listdir():
        if os.path.basename(fl) != '.DS_Store':
            count += 1
            fl_name = os.path.basename(fl).split('.')[0]

            # Get the req core var name
            core_var = fl_name.split('counts_')[1].split('_3z')[0]

            # then need to get ORI, Agency, YEAR and that var from the file.
            df = pd.read_csv(fl)

            if count == 1:
                df_req = df.loc[:, ['ORI', 'AGENCY', 'YEAR', 'Govt_level', 'POP100', 'population', 'population_mean', core_var]]

            if count > 1:
                df_core_var = df.loc[:, ['ORI', 'AGENCY', 'YEAR', 'Govt_level', 'POP100', 'population', 'population_mean', core_var]]
                df_req = df_req.merge(df_core_var, on=['ORI', 'YEAR'], how='outer')

                ## columns that shouldn't be duplicated and in which the new values should be entered.
                col_list = ['AGENCY', 'Govt_level', 'POP100', 'population', 'population_mean']

                for col in col_list:
                    df_req[f'{col}'] = df_req[f'{col}_x'].where(df_req[f'{col}_y'].isnull(), df_req[f'{col}_y'])

                df_req = df_req.drop([x for x in df_req if(x.endswith('_x')) | (x.endswith('_y'))], 1)

    # now merge with rep flag true ol df
    if rep_flag_fl!= False:
        rep_ol = pd.read_csv(rep_flag_fl, engine='python')
        rep_ol_req = rep_ol.loc[:, ['ORI', 'AGENCY', 'YEAR', 'Govt_level', 'POP100', 'population', 'population_mean', 'rep_month_flag_total_true']]

        df_req = df_req.merge(rep_ol_req, on=['ORI', 'YEAR'], how='outer')

        for col in col_list:
            df_req[f'{col}'] = df_req[f'{col}_x'].where(df_req[f'{col}_y'].isnull(), df_req[f'{col}_y'])

        df_req = df_req.drop([x for x in df_req if (x.endswith('_x')) | (x.endswith('_y'))], 1)


    df_req_arngd = df_clnr.rearrange_cols(df_req, ['ORI', 'AGENCY', 'YEAR', 'Govt_level', 'POP100', 'population', 'population_mean'])
    df_req_arngd.to_csv(f'{op_path}/{agency_type}_{var_types}_{ol_type}_ol_merger.csv', index=False)


def merge_rep1_12_ol_files():
    ''' merge crime outlier files'''
    # merge large agencies' ol files
    merge_ol_files(dir_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all/crime/large_cities',
                   rep_flag_fl = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all/crime/final_main_icpsr_crime_gte_10k_rep1_12_large_cities_rep_month_flag_total_true_within_ori_3z_ol.csv',
                   agency_type = 'large_cities',
                   var_types = 'crime',
                   ol_type  = 'all',
                   op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all')


    # merge medium agencies' ol files
    merge_ol_files(dir_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all/crime/medium_cities',
                   rep_flag_fl = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all/crime/final_main_icpsr_crime_gte_10k_rep1_12_medium_cities_rep_month_flag_total_true_within_ori_3z_ol.csv',
                   agency_type = 'medium_cities',
                   var_types = 'crime',
                   ol_type  = 'all',
                   op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all')


    # merge small agencies' ol files
    merge_ol_files(dir_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all/crime/small_cities',
                   rep_flag_fl = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all/crime/final_main_icpsr_crime_gte_10k_rep1_12_small_cities_rep_month_flag_total_true_within_ori_3z_ol.csv',
                   agency_type = 'small_cities',
                   var_types = 'crime',
                   ol_type  = 'all',
                   op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all')


    ''' merge arrests,incarceration,officers outliers files '''
    # merge large agencies' ol files
    merge_ol_files(dir_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all/arrests_incarceration_officers/large_cities',
                   agency_type = 'large_cities',
                   var_types = 'arrests_incarc_officers',
                   ol_type  = 'all',
                   op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all')


    # merge medium agencies' ol files
    merge_ol_files(dir_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all/arrests_incarceration_officers/medium_cities',
                   agency_type = 'medium_cities',
                   var_types = 'arrests_incarc_officers',
                   ol_type  = 'all',
                   op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all')


    # merge small agencies' ol files
    merge_ol_files(dir_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all/arrests_incarceration_officers/small_cities',
                   agency_type = 'small_cities',
                   var_types = 'arrests_incarc_officers',
                   ol_type  = 'all',
                   op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all')


    ''' merge per capita, empl ol files'''
    # merge large agencies' ol files
    merge_ol_files(
        dir_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all/pci_emp/large_cities',
        agency_type='large_cities',
        var_types='pci_emp',
        ol_type='all',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all')

    # merge medium agencies' ol files
    merge_ol_files(
        dir_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all/pci_emp/medium_cities',
        agency_type='medium_cities',
        var_types='pci_emp',
        ol_type='all',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all')

    # merge small agencies' ol files
    merge_ol_files(
        dir_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all/pci_emp/small_cities',
        agency_type='small_cities',
        var_types='pci_emp',
        ol_type='all',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12/ol_merger/ol_files_1_12_all')


# merge_rep1_12_ol_files()


def merge_rep1_12_clean1_ol_files():
    merge_ol_files(dir_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/crime/large_cities',
                   agency_type = 'large_cities',
                   var_types = 'crime',
                   ol_type  = 'all',
                   op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all')


    # merge medium agencies' ol files
    merge_ol_files(dir_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/crime/medium_cities',
                   agency_type = 'medium_cities',
                   var_types = 'crime',
                   ol_type  = 'all',
                   op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all')


    # merge small agencies' ol files
    merge_ol_files(dir_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/crime/small_cities',
                   agency_type = 'small_cities',
                   var_types = 'crime',
                   ol_type  = 'all',
                   op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all')


    ''' merge arrests,incarceration,officers outliers files '''
    # merge large agencies' ol files
    merge_ol_files(dir_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/arrests_incarceration_officers/large_cities',
                   agency_type = 'large_cities',
                   var_types = 'arrests_incarc_officers',
                   ol_type  = 'all',
                   op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all')


    # merge medium agencies' ol files
    merge_ol_files(dir_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/arrests_incarceration_officers/medium_cities',
                   agency_type = 'medium_cities',
                   var_types = 'arrests_incarc_officers',
                   ol_type  = 'all',
                   op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all')


    # merge small agencies' ol files
    merge_ol_files(dir_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/arrests_incarceration_officers/small_cities',
                   agency_type = 'small_cities',
                   var_types = 'arrests_incarc_officers',
                   ol_type  = 'all',
                   op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all')


    ''' merge per capita, empl ol files'''
    # merge large agencies' ol files
    merge_ol_files(
        dir_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/pci_emp/large_cities',
        agency_type='large_cities',
        var_types='pci_emp',
        ol_type='all',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all')

    # merge medium agencies' ol files
    merge_ol_files(
        dir_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/pci_emp/medium_cities',
        agency_type='medium_cities',
        var_types='pci_emp',
        ol_type='all',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all')

    # merge small agencies' ol files
    merge_ol_files(
        dir_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/pci_emp/small_cities',
        agency_type='small_cities',
        var_types='pci_emp',
        ol_type='all',
        op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all')


# merge_rep1_12_clean1_ol_files()


def update_merge_vals(df, cols):
    for col in cols:
        df[f'{col}'] = df[f'{col}_x'].where(df[f'{col}_y'].isnull(), df[f'{col}_y'])
    df = df.drop([x for x in df if (x.endswith('_x')) | (x.endswith('_y'))], 1)
    return df


def merge_agency_lev_ol_files(cr_fl, arrests_fl, pci_fl, op_path, agency_type):
    col_list = ['AGENCY', 'Govt_level', 'POP100', 'population', 'population_mean']
    cr = pd.read_csv(cr_fl)
    arrests = pd.read_csv(arrests_fl)
    pci_emp = pd.read_csv(pci_fl)

    cr_arrests = cr.merge(arrests, on=['ORI', 'YEAR'], how='outer')
    cr_arrests = update_merge_vals(cr_arrests, col_list)

    cr_arrests_pci = cr_arrests.merge(pci_emp, on=['ORI', 'YEAR'], how='outer')
    cr_arrests_pci = update_merge_vals(cr_arrests_pci, col_list)

    cr_arrests_pci.to_csv(f'{op_path}/{agency_type}_all_ol.csv', index=False)


# merge_agency_lev_ol_files(cr_fl = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/large_cities_crime_all_ol_merger.csv',
#                            arrests_fl = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/large_cities_arrests_incarc_officers_all_ol_merger.csv',
#                            pci_fl = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/large_cities_pci_emp_all_ol_merger.csv',
#                            op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all',
#                           agency_type = 'large_agencies')
#
#
# merge_agency_lev_ol_files(cr_fl = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/medium_cities_crime_all_ol_merger.csv',
#                            arrests_fl = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/medium_cities_arrests_incarc_officers_all_ol_merger.csv',
#                            pci_fl = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/medium_cities_pci_emp_all_ol_merger.csv',
#                            op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all',
#                           agency_type='medium_agencies')
#
#
# merge_agency_lev_ol_files(cr_fl = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/small_cities_crime_all_ol_merger.csv',
#                            arrests_fl = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/small_cities_arrests_incarc_officers_all_ol_merger.csv',
#                            pci_fl = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/small_cities_pci_emp_all_ol_merger.csv',
#                            op_path='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all',
#                           agency_type = 'small_agencies')


def consolidate_agency_lev_ol_files():
    large = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/large_agencies_all_ol.csv')
    print(large.shape[0])
    medium = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/medium_agencies_all_ol.csv')
    print(medium.shape[0])
    small = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/small_agencies_all_ol.csv')
    print(small.shape[0])

    all_ol_files = large.append([medium, small], sort=False)

    all_ol_files.to_csv('/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/ol_files_1_12_all/rep1_12_clean1_all_ols.csv', index=False)


# consolidate_agency_lev_ol_files()