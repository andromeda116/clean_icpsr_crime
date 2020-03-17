import pandas as pd
import numpy as np

pd.set_option('display.width', 5000)
pd.set_option('display.max_columns', 50)

crimes = ['murder', 'rape', 'robbery']
months = ['jan', 'feb', 'mar']
df1 = pd.DataFrame({'jan_murder': ['Zero or not reported'], 'jan_rape': ['2.0'], 'jan_robbery': [2],
                    'feb_murder': [1], 'feb_rape': ['Zero or not reported'], 'feb_robbery': [1],
                    'mar_murder': 'Zero or not reported', 'mar_rape': 'Zero or not reported', 'mar_robbery': np.nan})

# df1 = pd.read_csv(
#     '/Users/salma/Research/clean_icpsr_crime/data/crime_data/crime_data_req_monthly/consolidated/icpsr_cr_monthly_90_15_pop_gte_10k_fbi_month.csv')
#
#
# df1 = df1.query('jan_murder == "Zero or not reported"')
#
# df1 = df1.loc[:, ['jan_murder', 'feb_murder', 'mar_murder', 'jan_rape', 'feb_rape', 'mar_rape', 'jan_robbery', 'feb_robbery', 'mar_robbery']].head(2)
# print(df1)


df1_copy = df1.copy()

for mon in months:
    for cr in crimes:
        cr_copy = crimes.copy()
        cr_copy.remove(cr)
        cr_copy_rem_mon = [mon + '_' + cr for cr in cr_copy]

        if mon=='jan':
           # print(df1_copy[cr_copy_rem_mon])
            print(df1_copy[cr_copy_rem_mon]._get_numeric_data())

#            print(df1_copy[cr_copy_rem_mon]._get_numeric_data().sum(axis=1, min_count=1))
            df1[f'{mon}_{cr}'] = np.where(((df1_copy[f'{mon}_{cr}'] == 'Zero or not reported') &
                                           (df1_copy[cr_copy_rem_mon]._get_numeric_data().sum(axis=1, min_count=1) >= 0)),
                                           0, df1_copy[f'{mon}_{cr}'])
            break

        # gte_10k_df[f'{mon}_{crime}'] = np.where(((pd.isnull(gte_10k_df[f'{mon}_{crime}'] == np.nan))
        #                                          & (gte_10k_df[ini_crimes_copy_rem_mon].sum(axis=1) > 0)), 0,
        #                                         gte_10k_df[f'{mon}_{crime}'])



        # gte_10k_df[f'{mon}_{crime}'] = np.where(((gte_10k_df[f'{mon}_{crime}'] == 'Zero or not reported')
        #                     & (gte_10k_df[f'{mon}_fbi_month'] == True)),  # Identifies the case to apply to
        #                     0, # This is the value that is inserted
        #                     gte_10k_df[f'{mon}_{crime}']) # This is the column that is affected
        #
        # gte_10k_df[f'{mon}_{crime}'] = np.where(((gte_10k_df[f'{mon}_{crime}'] == 'Zero or not reported')
        #                                         & (gte_10k_df[f'{mon}_fbi_month'] == False)), # Identifies the case to apply to
        #                                         np.nan,  # This is the value that is inserted
        #                                         gte_10k_df[f'{mon}_{crime}']) # This is the column that is affected


        # print(cr_copy_rem_mon)
        # print(df1[cr_copy_rem_mon].sum(axis=1) >= 0)

        # if (pd.isnull(df[1]))
        #     if (np.isnan(df[1]))


        # if (pd.isnull(df1[f'{mon}_{cr}'])):
        #     print('nan')
            #print(df1[cr_copy_rem_mon].sum(axis=1))
            # if df1[cr_copy_rem_mon].sum(axis=1) >= 0:
            #     df1[f'{mon}_{cr}'] = 0

        # df1[f'{mon}_{cr}'] = np.where(((pd.isnull(df1[f'{mon}_{cr}'])) & (df1[cr_copy_rem_mon]._get_numeric_data().sum(axis=1) >= 0)),
        #                                  0, df1[f'{mon}_{cr}'])
                                         #df1[f'{mon}_{cr}'])

        #print(df1_copy[f'{mon}_{cr}'])
        #if mon == 'jan':
        #print(df1[f'{mon}_{cr}'])
        # print(df1_copy[cr_copy_rem_mon])
        # print('rem cr: ', cr_copy_rem_mon, ' rem cr totals: ', df1_copy[cr_copy_rem_mon]._get_numeric_data().sum(axis=1, min_count=1))


df1.replace('Zero or not reported', np.nan, inplace=True)

# print()
# print(df1)