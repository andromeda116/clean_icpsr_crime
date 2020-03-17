import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 50)

monthly_df = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/crime_icpsr_90_15.csv')


def get_rep_fbi_flags_info():
    ###### Get the number of ORIs in the final cleaned crime_icpsr_90_15.csv file #######
    print('icpsr fixed fl: ', set(monthly_df.ORI).__len__())


    ##### Get the freq count of various values under rep_month_flag_total_true and fbi_month_total_true ######
    print(monthly_df['rep_month_flag_total_true'].value_counts())
    print(monthly_df['fbi_month_total_true'].value_counts())


    ##### Get the records where rep_month_flag counts and fbi_month_flag counts are not the same #####
    monthly_df_flags_true = monthly_df.query('rep_month_flag_total_true != fbi_month_total_true')
    monthly_df_flags_true.to_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/crime_icpsr_90_15_unequal_rep_fbi_flags.csv', index=False)


    ##### Get the number of ORIs where the rep_month_flag counts and fbi_month_flag counts are not the same #######
    print('icpsr_cr_monthly_90_15_unequal_rep_flags_after_card_label_fix: ', set(monthly_df_flags_true.ORI).__len__())

    ##### Get the records where rep_month_flag is true and fbi_month_flag is false
    ## coz month_included_in had blanks so fbi flag set to false eventhough non zero crimes
    monthly_df_flags_true = monthly_df.query('rep_month_flag_total_true == True and fbi_month_total_true == False')
    monthly_df_flags_true.to_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/crime_icpsr_90_15_rep_true_fbi_false.csv', index=False)


    ##### Get the records where rep_month_flag is false and fbi_month_flag is true
    ### though zero crimes, fbi flag is true coz card 1 type has P and card pt has P values.
    monthly_df_flags_true = monthly_df.query('rep_month_flag_total_true == False and fbi_month_total_true == True')
    monthly_df_flags_true.to_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/crime_icpsr_90_15_rep_false_fbi_true.csv', index=False)


# get_rep_fbi_flags_info()


def get_outliers(df, st_dev, cols, op_path):
    df_z = df[cols].groupby('ORI').transform(lambda group: (group - group.mean()).div(group.std()))

    for col in cols:
        outliers = df_z[f'{col}'].abs() > st_dev
        outliers_df = df[outliers]
        outliers_df.to_csv(f'/{op_path}/{col}_within_ori_3z_ol.csv', index=False)


get_outliers(df=monthly_df, st_dev=3, cols=['rep_month_flag_total_true', 'fbi_month_total_true'], op_path='/Users/salma/Research/clean_icpsr_crime/data/cleaned_check')