import pandas as pd
import numpy as np


### Get the final cleaned icpsr crime file
final_icpsr_cr = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/final_crime_other_main/crime_icpsr_clnd_rem_main.csv')


def create_crime_indexes(df):
    ## create violent, property, total crime indexes

    df['violent_crime'] = df.loc[:, ['murder', 'rape', 'robbery', 'agg_assault']].sum(axis=1)
    df['property_crime'] = df.loc[:, ['burglary', 'larceny', 'motor_vehicle_theft']].sum(axis=1)

    # misdemeanors: simple assault + larceny
    df['misdemeanors'] = df.loc[:, ['simple_assault', 'larceny']].sum(axis=1)

    df['main_crime'] = df.loc[:, ['violent_crime', 'property_crime']].sum(axis=1)

    return df


# final_icpsr_cr_indexes = create_crime_indexes(final_icpsr_cr)


def create_incarceration_counts(df):
    df['tot_felonies_agency'] = df.loc[:, ['murder', 'rape', 'robbery', 'agg_assault', 'burglary', 'motor_vehicle_theft']].sum(axis=1)

    df['tot_misdemeanors_agency'] = df.loc[:, ['larceny', 'simple_assault']].sum(axis=1)

    df['tot_major_offenses_agency'] = df.loc[:, ['tot_felonies_agency', 'tot_misdemeanors_agency']].sum(axis=1)

    cnty_cr_totals_df = pd.read_csv('/Users/salma/Research/clean_icpsr_crime/data/final_crime_other_main/agency_1st_rep_yr_fel_misd_pcts_98_08.csv')

    cnty_agency_totals_98_08 = cnty_cr_totals_df[['ORI', 'perc_felonies', 'perc_misdemeanors']]

    # So here merging with agency_1st_rep_yr_fel_misd_pcts_98_08.csv file to have only those ORIs which have
    # reported data for all the years between 98-08
    df = df.merge(cnty_agency_totals_98_08, on=['ORI'])

    df['prison_occupancy_count'] = (df['perc_felonies'] * df['total_prison_pop']) / 100
    df['jail_occupancy_count'] = (df['perc_misdemeanors'] * df['jail_interp']) / 100

    df.replace(np.inf, 0, inplace=True)

    df['prison_occupancy_count'] = df['prison_occupancy_count'].round()
    df['jail_occupancy_count'] = df['jail_occupancy_count'].round()

    return df


# final_icpsr_cr_indexes_incarc = create_incarceration_counts(final_icpsr_cr_indexes)


### create rates for crime and incarc vars
def create_rates(df):
    crime_vars = ['murder', 'rape', 'robbery', 'agg_assault', 'simple_assault', 'burglary', 'larceny', 'motor_vehicle_theft',
                  'violent_crime', 'property_crime', 'misdemeanors', 'main_crime']

    '''
        ###### For crime variables ######
    '''
    for rate_var in crime_vars:
        df[f'{rate_var}_rate'] = (df[f'{rate_var}'] / df['population']) * 100000

    '''
        ###### For incarceration variables ######
    '''
    incarc_vars = ['prison_occupancy_count', 'jail_occupancy_count']
    for var in incarc_vars:
        df[f'{var}_rate'] = (df[f'{var}'] / df['total_count_county']) * 100000

    '''
        ###### For econ variables ######
    '''
    econ_vars = ['pci_white', 'pci_black', 'emp_total', 'emp_total_white', 'emp_total_black']
    for var in econ_vars:
        df[f'{var}_rate'] = (df[f'{var}'] / df['POP100']) * 100000

    return df


# final_icpsr_cr_indexes_incarc_rates = create_rates(final_icpsr_cr_indexes_incarc)

# final_icpsr_cr_indexes_incarc_rates.to_csv('/Users/salma/Research/clean_icpsr_crime/data/final_crime_other_main/crime_icpsr_clnd_rem_main_incarc_cnts_all_rates.csv', index=False)


def drop_zero_cr_zero_arrests_blank(fl_path):
    df = pd.read_csv(fl_path)

    '''
        # delete all rows for which column 'Age' has value greater than 30 and Country is India
        indexNames = dfObj[ (dfObj['Age'] >= 30) & (dfObj['Country'] == 'India') ].index

        dfObj.drop(indexNames , inplace=True)
    '''
    df.drop(df.query('main_crime == 0').index, inplace=True)

    arrest_var_str = ['murder', 'rape', 'robbery', 'agg_assault', 'burglary', 'larceny', 'mtr_veh_theft', 'disorder',
                      'violent', 'property']

    # replace main_tot_arrests and main_tot_arrests_rate with nan after the loop so that it doesn't get set to nan before others
    for var in arrest_var_str:
        for col in list(df):
            if all(x in col for x in [var, 'arrests']):
                df.loc[df['main_tot_arrests'] == 0,[f'{col}']] = np.nan

    df.loc[df['main_tot_arrests'] == 0, ['main_tot_arrests', 'main_tot_arrests_rate',
                                                                             'main_arrests_tot_black', 'main_arrests_tot_white',
                                                                             'main_arrests_tot_black_rate', 'main_arrests_tot_white_rate']] = np.nan
    df.to_csv('/Users/salma/Research/clean_icpsr_crime/data/cleaned_check/final_main_rep1_12_gte_10k_0_cr_drpd_0_main_arrests_repl_with_blanks.csv', index=False)


drop_zero_cr_zero_arrests_blank(fl_path='/Users/salma/Research/clean_icpsr_crime/data/agency_categories/final_main_icpsr_crime_gte_10k_rep1_12_gte_10k.csv')