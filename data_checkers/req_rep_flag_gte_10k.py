import pandas as pd


from utilities import agency_categorizer as agncy_cat

df_cr_fnl_gte_10k = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k.csv')


def get_fnl_cr_reqd_vars():
    df_cr_fnl_gte_10k_req = df_cr_fnl_gte_10k.loc[:, ['ORI', 'YEAR', 'AGENCY', 'population', 'population_mean', 'murder', 'rape', 'robbery', 'agg_assault',
                                      'simple_assault', 'burglary', 'larceny', 'motor_vehicle_theft', 'rep_month_flag_total_true', 'fbi_month_total_true']]

    df_cr_fnl_gte_10k_req.to_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/final_main_icpsr_crime_gte_10k_req_pop_mean.csv', index=False)


# get_fnl_cr_reqd_vars()


def get_within_grp_rep_totals(df_req):
    # df = df_cr_fnl_gte_10k.groupby(['ORI', 'rep_month_flag_total_true']).size().unstack()
    ### ORI - index

    df = df_req.groupby(['ORI', 'rep_month_flag_total_true']).size().unstack().reset_index()
    ### with reset index - gets back to 0 based index with ORI as a column

    df.columns = ['rep_month_flag_total_true_' + str(x) for x in df.columns]

    df.to_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/rep_month_flag_total_true_count_by_ori.csv', index=False)

    for col in list(df):
        print(df[f'{col}'].value_counts())
        print(df[f'{col}'].value_counts(normalize=True))


# get_within_grp_rep_totals(df_cr_fnl_gte_10k_req)


## get oris who have reported data for all the months in all the 26 yrs from 1990-2015
def get_rep_26():
    df_grpd_ori = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/rep_month_flag_total_true_count_by_ori.csv')

    df_grpd_ori_rep_26 = df_grpd_ori.query('rep_month_flag_total_true_12 == 26')
    df_req_rep = df_cr_fnl_gte_10k[df_cr_fnl_gte_10k['ORI'].isin(df_grpd_ori_rep_26.rep_month_flag_total_true_ORI)]

    df_req_rep.to_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/final_main_icpsr_crime_gte_10k_rep12_26.csv', index=False)
    ### categorize agencies, initial core files
    agncy_cat.categorize_agencies(df= df_req_rep,
                                  op_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories',
                                  fl_name='final_main_icpsr_crime_gte_10k_rep12_26')


# get_rep_26()


# get the oris whose rep_month_flag_total_true is 1 or 12
def get_rep_1_12():
    #df_grpd_ori_rep_1_12 = df_cr_fnl_gte_10k.query('rep_month_flag_total_true == 1 | rep_month_flag_total_true == 12')

    df_grpd_ori_rep_1_12 = df_cr_fnl_gte_10k[ df_cr_fnl_gte_10k.rep_month_flag_total_true.isin([1, 12]) ]

    df_grpd_ori_rep_1_12.to_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/final_main_icpsr_crime_gte_10k_rep1_12.csv', index=False)
    ### categorize agencies, initial core files
    agncy_cat.categorize_agencies(df= df_grpd_ori_rep_1_12,
                                  op_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories',
                                  fl_name='final_main_icpsr_crime_gte_10k_rep1_12')


#get_rep_1_12()


def get_rep_1_10_11_12():
   # df_grpd_ori_rep_1_10_11_12 = df_cr_fnl_gte_10k.query('rep_month_flag_total_true == 1 | rep_month_flag_total_true == 10 | rep_month_flag_total_true == 11 | rep_month_flag_total_true == 12')

    df_grpd_ori_rep_1_10_11_12 = df_cr_fnl_gte_10k[df_cr_fnl_gte_10k.rep_month_flag_total_true.isin([1, 10, 11, 12])]

    df_grpd_ori_rep_1_10_11_12.to_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/final_main_icpsr_crime_gte_10k_rep1_10_11_12.csv', index=False)
    print(df_grpd_ori_rep_1_10_11_12.shape[0])
    print(set(df_grpd_ori_rep_1_10_11_12.ORI).__len__())

    #### categorize only if needed. keep the files for now, dnt have to delete
    # ## categorize agencies, initial core files
    # agncy_cat.categorize_agencies(df= df_grpd_ori_rep_1_10_11_12,
    #                               op_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories',
    #                               fl_name='final_main_icpsr_crime_gte_10k_rep_1_10_11_12')


# get_rep_1_10_11_12()


def cat_fnl_gte_10k_rep_1_12_zero_cr_drpd_zero_arsts_repl_blanks():
    df_zero_cr_drpd_zero_arsts_repl_blanks = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/final_main_rep1_12_gte_10k_0_cr_drpd_0_main_arrests_repl_with_blanks.csv')
    agncy_cat.categorize_agencies(df=df_zero_cr_drpd_zero_arsts_repl_blanks,
                                  # op_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories',
                                  op_path='/Users/salma/Research/us_crime_data_analysis/data',
                                  #fl_name='final_main_rep1_12_gte_10k_0_cr_drpd_0_main_arrests_repl_with_blanks')
                                  fl_name='final_main')


cat_fnl_gte_10k_rep_1_12_zero_cr_drpd_zero_arsts_repl_blanks()