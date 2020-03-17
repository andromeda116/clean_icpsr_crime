import pandas as pd


df = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/final_crime_other_main/crime_icpsr_clnd_rem_main_incarc_cnts_all_rates.csv')


def rename_arrest_vars(df):
    # rename arrest var to include race in arrest var name
    df.rename({'agg_assault_tot_black': 'agg_assault_tot_arrests_black',
                              'agg_assault_tot_white': 'agg_assault_tot_arrests_white',
                              'all_other_tot_black': 'all_other_tot_arrests_black',
                              'all_other_tot_white': 'all_other_tot_arrests_white',
                              'arson_tot_black': 'arson_tot_arrests_black',
                              'arson_tot_white': 'arson_tot_arrests_white',
                              'burglary_tot_black': 'burglary_tot_arrests_black',
                              'burglary_tot_white': 'burglary_tot_arrests_white',
                              'mtr_veh_theft_tot_black': 'mtr_veh_theft_tot_arrests_black',
                              'mtr_veh_theft_tot_white': 'mtr_veh_theft_tot_arrests_white',
                              'murder_tot_black': 'murder_tot_arrests_black',
                              'murder_tot_white': 'murder_tot_arrests_white',
                              'rape_tot_black': 'rape_tot_arrests_black',
                              'rape_tot_white': 'rape_tot_arrests_white',
                              'robbery_tot_black': 'robbery_tot_arrests_black',
                              'robbery_tot_white': 'robbery_tot_arrests_white',
                              'sale_cannabis_tot_black': 'sale_cannabis_tot_arrests_black',
                              'sale_cannabis_tot_white': 'sale_cannabis_tot_arrests_white',
                              'sale_drug_total_tot_arrests': 'sale_drug_tot_arrests',
                              'sale_drug_total_tot_black': 'sale_drug_tot_arrests_black',
                              'sale_drug_total_tot_white': 'sale_drug_tot_arrests_white',
                              'weapons_tot_black': 'weapons_tot_arrests_black',
                              'weapons_tot_white': 'weapons_tot_arrests_white',
                              'poss_cannabis_tot_black': 'poss_cannabis_tot_arrests_black',
                              'poss_cannabis_tot_white': 'poss_cannabis_tot_arrests_white',
                              'poss_drug_total_tot_arrests': 'poss_drug_tot_arrests',
                              'poss_drug_total_tot_black': 'poss_drug_tot_arrests_black',
                              'poss_drug_total_tot_white': 'poss_drug_tot_arrests_white',
                              'agg_assault_tot_black_rate': 'agg_assault_tot_arrests_black_rate',
                              'agg_assault_tot_white_rate': 'agg_assault_tot_arrests_white_rate',
                              'all_other_tot_black_rate': 'all_other_tot_arrests_black_rate',
                              'all_other_tot_white_rate': 'all_other_tot_arrests_white_rate',
                              'arson_tot_black_rate': 'arson_tot_arrests_black_rate',
                              'arson_tot_white_rate': 'arson_tot_arrests_white_rate',
                              'burglary_tot_black_rate': 'burglary_tot_arrests_black_rate',
                              'burglary_tot_white_rate': 'burglary_tot_arrests_white_rate',
                              'mtr_veh_theft_tot_black_rate': 'mtr_veh_theft_tot_arrests_black_rate',
                              'mtr_veh_theft_tot_white_rate': 'mtr_veh_theft_tot_arrests_white_rate',
                              'murder_tot_black_rate': 'murder_tot_arrests_black_rate',
                              'murder_tot_white_rate': 'murder_tot_arrests_white_rate',
                              'rape_tot_black_rate': 'rape_tot_arrests_black_rate',
                              'rape_tot_white_rate': 'rape_tot_arrests_white_rate',
                              'robbery_tot_black_rate': 'robbery_tot_arrests_black_rate',
                              'robbery_tot_white_rate': 'robbery_tot_arrests_white_rate',
                              'larceny_theft_arrests_tot': 'larceny_theft_tot_arrests',
                              'larceny_theft_arrests_black': 'larceny_theft_tot_arrests_black',
                              'larceny_theft_arrests_white': 'larceny_theft_tot_arrests_white',
                              'sale_cannabis_tot_black_rate': 'sale_cannabis_tot_arrests_black_rate',
                              'sale_cannabis_tot_white_rate': 'sale_cannabis_tot_arrests_white_rate',
                              'sale_drug_total_tot_arrests_rate': 'sale_drug_tot_arrests_rate',
                              'sale_drug_total_tot_black_rate': 'sale_drug_tot_arrests_black_rate',
                              'sale_drug_total_tot_white_rate': 'sale_drug_tot_arrests_white_rate',
                              'weapons_tot_black_rate': 'weapons_tot_arrests_black_rate',
                              'weapons_tot_white_rate': 'weapons_tot_arrests_white_rate',
                              'poss_cannabis_tot_black_rate': 'poss_cannabis_tot_arrests_black_rate',
                              'poss_cannabis_tot_white_rate': 'poss_cannabis_tot_arrests_white_rate',
                              'poss_drug_total_tot_arrests_rate': 'poss_drug_tot_arrests_rate',
                              'poss_drug_total_tot_black_rate': 'poss_drug_tot_arrests_black_rate',
                              'poss_drug_total_tot_white_rate': 'poss_drug_tot_arrests_white_rate',
                              'total_main_arrests': 'main_tot_arrests',
                              'violent_arrests': 'violent_tot_arrests',
                              'property_arrests': 'property_tot_arrests',
                              'drug_total_arrests': 'drug_tot_arrests',
                              'drug_arrests_black': 'drug_tot_arrests_black',
                              'drug_arrests_white': 'drug_tot_arrests_white',
                              'disorder_arrests_black_index': 'disorder_tot_arrests_black_index',
                              'disorder_arrests_white_index': 'disorder_tot_arrests_white_index',
                              'larceny_theft_arrests_tot_rate': 'larceny_theft_tot_arrests_rate',
                              'larceny_theft_arrests_black_rate': 'larceny_theft_tot_arrests_black_rate',
                              'larceny_theft_arrests_white_rate': 'larceny_theft_tot_arrests_white_rate',
                              'violent_arrests_rates': 'violent_tot_arrests_rate',
                              'property_arrests_rates': 'property_tot_arrests_rate',
                              'total_main_arrests_rates': 'main_tot_arrests_rate',
                              'drug_total_arrests_rate': 'drug_tot_arrests_rate',
                              'drug_arrests_black_rate': 'drug_tot_arrests_black_rate',
                              'drug_arrests_white_rate': 'drug_tot_arrests_white_rate',
                              'disorder_arrests_black_index_rate': 'disorder_tot_arrests_black_index_rate',
                              'disorder_arrests_white_index_rate': 'disorder_tot_arrests_white_index_rate'}, axis=1, inplace=True)

    return df


def create_arrests_index_by_race(df):
    '''
            ###### For new arrests vars ######
        '''
    df['violent_tot_arrests_black'] = df[['murder_tot_arrests_black', 'agg_assault_tot_arrests_black',
                                          'rape_tot_arrests_black', 'robbery_tot_arrests_black']].sum(axis=1)
    df['property_tot_arrests_black'] = df[['burglary_tot_arrests_black', 'larceny_theft_tot_arrests_black',
                                           'mtr_veh_theft_tot_arrests_black','arson_tot_arrests_black']].sum(axis=1)
    df['main_arrests_tot_black'] = df[['violent_tot_arrests_black', 'property_tot_arrests_black']].sum(axis=1)

    df['violent_tot_arrests_white'] = df[['murder_tot_arrests_white', 'agg_assault_tot_arrests_white',
                                          'rape_tot_arrests_white', 'robbery_tot_arrests_white']].sum(axis=1)
    df['property_tot_arrests_white'] = df[['burglary_tot_arrests_white', 'larceny_theft_tot_arrests_white',
                                           'mtr_veh_theft_tot_arrests_white', 'arson_tot_arrests_white']].sum(axis=1)
    df['main_arrests_tot_white'] = df[['violent_tot_arrests_white', 'property_tot_arrests_white']].sum(axis=1)

    # New arrests vars rates
    arrest_vars = ['violent_tot_arrests_black', 'property_tot_arrests_black', 'main_arrests_tot_black',
                   'violent_tot_arrests_white', 'property_tot_arrests_white', 'main_arrests_tot_white']

    for var in arrest_vars:
        df[f'{var}_rate'] = (df[f'{var}'] / df['population']) * 10000

    return df


renamed_df = rename_arrest_vars(df)

renamed_df_arrests_race_indices = create_arrests_index_by_race(renamed_df)
print(list(renamed_df_arrests_race_indices))
renamed_df_arrests_race_indices.to_csv('/Users/salma/Research/clean_icpsr_crime/data/incarceration/crime_icpsr_clnd_rem_main_incarc_cnts_all_rates_renamed_arrest_vars.csv', index=False)
