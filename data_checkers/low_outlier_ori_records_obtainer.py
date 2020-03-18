import pandas as pd
from mergers import outlier_merger as ol_mrgr
from custom_utilities import df_cleaner as df_clnr


def get_core_var_low_z_cols():

    fnl_core_counts_df = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_rep1_12_gte_10k_0_cr_drpd_0_main_arrests_repl_with_blanks_gte_10k_core_counts.csv')

    fnl_core_counts_df_req_cols = fnl_core_counts_df.loc[:, ['ORI', 'agg_assault', 'burglary', 'motor_vehicle_theft', 'larceny',
                                                        'murder', 'simple_assault','robbery', 'rape', 'total_officers',
                                                        'drug_tot_arrests', 'disorder_arrests_tot_index', 'jail_occupancy_count',
                                                        'prison_occupancy_count']]

    id_cols = ['ORI', 'AGENCY', 'YEAR', 'Govt_level', 'POP100', 'population', 'population_mean']
    dup_id_cols = ['AGENCY', 'Govt_level', 'POP100', 'population', 'population_mean']

    fnl_core_counts_df_req_z = fnl_core_counts_df_req_cols.groupby('ORI').transform(lambda group: (group - group.mean()).div(group.std()))

    fnl_req_counts_ol_df = fnl_core_counts_df.loc[:, id_cols]

    #stds = -3
    stds = 3

    count = 0

    for col in list(fnl_core_counts_df_req_z):

        count += 1

        fnl_req_counts_ol_df_to_merge = fnl_core_counts_df.loc[:, id_cols]

        fnl_req_counts_ol_df_to_merge[f'{col}'] = fnl_core_counts_df[f'{col}']
        fnl_req_counts_ol_df_to_merge[f'{col}_z'] = fnl_core_counts_df_req_z[f'{col}']

        fnl_req_counts_ol_df_to_merge = fnl_req_counts_ol_df_to_merge.loc[fnl_req_counts_ol_df_to_merge[f'{col}_z'] > stds]

        if count == 1:
            fnl_req_counts_ol_df = fnl_req_counts_ol_df.merge(fnl_req_counts_ol_df_to_merge, on=['ORI', 'YEAR'])

        elif count > 1:
            fnl_req_counts_ol_df = fnl_req_counts_ol_df.merge(fnl_req_counts_ol_df_to_merge, on=['ORI', 'YEAR'], how='outer')

        fnl_req_counts_ol_df = ol_mrgr.update_merge_vals(fnl_req_counts_ol_df, dup_id_cols)

    df_req_arngd = df_clnr.rearrange_cols(fnl_req_counts_ol_df, ['ORI', 'AGENCY', 'YEAR', 'Govt_level', 'POP100', 'population',
                                                   'population_mean'])

    df_req_arngd.to_csv('/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/final_main_rep1_12_clean1_core_counts_high_3z.csv', index=False)
    # df_req_arngd.to_csv(
    #     '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/final_main_rep1_12_clean1_core_counts_0.3_perc_diff.csv',
    #     index=False)

    
get_core_var_low_z_cols()


def get_ol_ori_rec(ol_fl_pth, fnl_df_path, op_fl_name, op_path):

    ol_df = pd.read_csv(ol_fl_pth)

    fnl_core_counts_df = pd.read_csv(fnl_df_path)

    fnl_core_counts_df_req = fnl_core_counts_df.loc[:, ['ORI', 'AGENCY', 'YEAR', 'Govt_level', 'POP100', 'population',
                                                   'population_mean', 'agg_assault', 'burglary', 'motor_vehicle_theft', 'larceny',
                                                        'murder', 'simple_assault','robbery', 'rape', 'total_officers',
                                                        'drug_tot_arrests', 'disorder_arrests_tot_index', 'jail_occupancy_count',
                                                        'prison_occupancy_count']]

    fnl_core_ol_vars = fnl_core_counts_df_req[fnl_core_counts_df_req.ORI.isin(ol_df.ORI)]

    ol_df_req = ol_df.loc[:, ['ORI', 'YEAR', 'agg_assault_z', 'burglary_z', 'motor_vehicle_theft_z', 'larceny_z',
                                                        'murder_z', 'simple_assault_z','robbery_z', 'rape_z', 'total_officers_z',
                                                        'drug_tot_arrests_z', 'disorder_arrests_tot_index_z', 'jail_occupancy_count_z',
                                                        'prison_occupancy_count_z']]

    fnl_core_ol_vars_rec = fnl_core_ol_vars.merge(ol_df_req, on=['ORI', 'YEAR'], how='outer')

    fnl_core_ol_vars_rec.sort_values(['ORI', 'YEAR'], inplace=True)

    fnl_core_ol_vars_rec = fnl_core_ol_vars_rec.reindex(sorted(fnl_core_ol_vars_rec.columns), axis=1)

    fnl_core_ol_vars_rec = df_clnr.rearrange_cols(fnl_core_ol_vars_rec,
                                          ['ORI', 'AGENCY', 'YEAR', 'Govt_level', 'POP100', 'population',
                                           'population_mean'])

    fnl_core_ol_vars_rec.to_csv(f'{op_path}/{op_fl_name}_3z_rec.csv', index=False)


# #### Get All 3z Records ####
# get_ol_ori_rec(ol_fl_pth='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/final_main_rep1_12_clean1_core_counts_all_3z.csv',
#                fnl_df_path = '/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_rep1_12_gte_10k_0_cr_drpd_0_main_arrests_repl_with_blanks_gte_10k_core_counts.csv',
#                op_fl_name = 'final_main_rep1_12_clean1_core_counts_all',
#                op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger')

#### Get low 3z Records ####
# get_ol_ori_rec(ol_fl_pth='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/final_main_rep1_12_clean1_core_counts_low_3z.csv',
#                fnl_df_path = '/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_rep1_12_gte_10k_0_cr_drpd_0_main_arrests_repl_with_blanks_gte_10k_core_counts.csv',
#                op_fl_name = 'final_main_rep1_12_clean1_core_counts_low',
#                op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger')


### Get high 3z Records ####
get_ol_ori_rec(ol_fl_pth='/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger/final_main_rep1_12_clean1_core_counts_high_3z.csv',
               fnl_df_path = '/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_rep1_12_gte_10k_0_cr_drpd_0_main_arrests_repl_with_blanks_gte_10k_core_counts.csv',
               op_fl_name = 'final_main_rep1_12_clean1_core_counts_high',
               op_path = '/Users/salma/Research/clean_icpsr_crime/data/core_vars_outliers_rep1_12_clean1/ol_merger')