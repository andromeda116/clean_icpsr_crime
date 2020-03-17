import pandas as pd
import os
import re


def create_req_cr_files():
    os.chdir('/Users/salma/Research/clean_icpsr_crime/data/crime_data')

    for fl in os.listdir():
        if fl != '.DS_Store' and fl != 'crime_data_req_cols' and fl!= 'crime_data_req_monthly':
            if os.path.splitext(fl)[1] == '.tsv':
                df = pd.read_csv(fl, sep='\t', encoding='cp1252')

            elif os.path.splitext(fl)[1] == '.xpt':
                # with open(fl, 'rb') as f:
                #    df = xport.to_dataframe(f)
                # faster than xport
                df = pd.read_sas(fl)

            elif os.path.splitext(fl)[1] == '.txt':
                df = pd.read_table(fl, encoding='cp1252')
                #data = pd.read_csv('file.txt', sep=',')

            elif os.path.splitext(fl)[1] == '.sav':
                df = pd.read_spss(fl)

            fl_name = os.path.splitext(fl)[0]
            fl_name_split = re.split('_|-', fl_name)

            if '1990' in fl_name_split:
                df.rename(columns={'V7': 'ORI', 'V10': 'YEAR', 'V17': 'last_month_reported', 'V35': 'AGENCY'}, inplace=True)

                df['population'] = df[['V19', 'V22', 'V25']].sum(axis=1)
                df_req = df.loc[:, ['ORI', 'YEAR', 'AGENCY', 'last_month_reported', 'population']]
                df_req['murder'] = df.loc[:,
                               ['V80', 'V198', 'V316', 'V434', 'V552', 'V670','V788', 'V906','V1024','V1142','V1260','V1378']].sum(axis=1)
                df_req['rape'] = df.loc[:,
                             ['V82','V200', 'V318', 'V436', 'V554', 'V672','V790', 'V908','V1026','V1144','V1262','V1380']].sum(axis=1)
                df_req['robbery'] = df.loc[:,
                                ['V85','V203', 'V321', 'V439', 'V557', 'V675','V793', 'V911','V1029','V1147','V1265','V1383']].sum(axis=1)
                df_req['all_assault'] = df.loc[:,
                                    ['V90','V208', 'V326', 'V444', 'V562','V680','V798', 'V916','V1034','V1152','V1270','V1388']].sum(axis=1)
                df_req['simple_assault'] = df.loc[:,
                                       ['V95','V213', 'V331', 'V449', 'V567', 'V685','V803','V921','V1039','V1157','V1275','V1393']].sum(axis=1)
                df_req['aggravated_assault'] = df_req['all_assault'] - df_req['simple_assault']
                df_req['burglary'] = df.loc[:,
                                 ['V96','V214', 'V332', 'V450', 'V568','V686','V804', 'V922','V1040','V1158','V1276','V1394']].sum(axis=1)
                df_req['larceny'] = df.loc[:,
                                ['V100','V218', 'V336', 'V454', 'V572','V690','V808', 'V926','V1044','V1162','V1280','V1398']].sum(axis=1)
                df_req['motor_vehicle_theft'] = df.loc[:,
                                            ['V101','V219', 'V337', 'V455', 'V573','V691','V809', 'V927','V1045','V1163','V1281','V1399']].sum(axis=1)

            elif any(x in ['1991', '1992', '1994', '1995', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005',
                           '2006', '2007',
                           '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015'] for x in fl_name_split):
                df.rename(columns={'V3': 'ORI', 'V6': 'YEAR', 'V12': 'last_month_reported', 'V26': 'AGENCY'}, inplace=True)

                df['population'] = df[['V14', 'V17', 'V20']].sum(axis=1)
                df_req = df.loc[:, ['ORI', 'YEAR', 'AGENCY', 'last_month_reported', 'population']]
                df_req['murder'] = df.loc[:,
                               ['V70', 'V188', 'V306', 'V424', 'V542', 'V660', 'V778', 'V896', 'V1014', 'V1132', 'V1250',
                                'V1368']].sum(axis=1)
                df_req['rape'] = df.loc[:,
                             ['V72', 'V190', 'V308', 'V426', 'V544', 'V662', 'V780', 'V898', 'V1016', 'V1134', 'V1252',
                              'V1370']].sum(axis=1)
                df_req['robbery'] = df.loc[:,
                                ['V75', 'V193', 'V311', 'V429', 'V547', 'V665', 'V783', 'V901', 'V1019', 'V1137', 'V1255',
                                 'V1373']].sum(axis=1)
                df_req['all_assault'] = df.loc[:,
                                    ['V80', 'V198', 'V316', 'V434', 'V552', 'V670', 'V788', 'V906', 'V1024', 'V1142',
                                     'V1260', 'V1378']].sum(axis=1)
                df_req['simple_assault'] = df.loc[:,
                                       ['V85', 'V203', 'V321', 'V439', 'V557', 'V675', 'V793', 'V911', 'V1029', 'V1147',
                                        'V1265', 'V1383']].sum(axis=1)
                df_req['aggravated_assault'] = df_req['all_assault'] - df_req['simple_assault']
                df_req['burglary'] = df.loc[:,
                                 ['V86', 'V204', 'V322', 'V440', 'V558', 'V676', 'V794', 'V912', 'V1030', 'V1148', 'V1266',
                                  'V1384']].sum(axis=1)
                df_req['larceny'] = df.loc[:,
                                ['V90', 'V208', 'V326', 'V444', 'V562', 'V680', 'V798', 'V916', 'V1034', 'V1152', 'V1270',
                                 'V1388']].sum(axis=1)
                df_req['motor_vehicle_theft'] = df.loc[:,
                                            ['V91', 'V209', 'V327', 'V445', 'V563', 'V681', 'V799', 'V917', 'V1035',
                                             'V1153', 'V1271', 'V1389']].sum(axis=1)

            elif '1993' in fl_name_split:
                df.rename(columns={'V7': 'ORI', 'V10': 'YEAR', 'V16': 'last_month_reported', 'V31': 'AGENCY'}, inplace=True)

                df['population'] = df[['V18', 'V21', 'V24']].sum(axis=1)
                df_req = df.loc[:, ['ORI', 'YEAR', 'AGENCY', 'last_month_reported', 'population']]
                df_req['murder'] = df.loc[:,
                               ['V75', 'V193','V311','V429','V547','V665','V783','V901','V1019','V1137','V1255','V1373']].sum(axis=1)
                df_req['rape'] = df.loc[:,
                             ['V77','V195','V313','V431','V549','V667','V785','V903','V1021','V1139','V1257','V1375']].sum(axis=1)
                df_req['robbery'] = df.loc[:,
                                ['V80', 'V198','V316','V434','V552','V670','V788','V906','V1024','V1142','V1260','V1378']].sum(axis=1)
                df_req['all_assault'] = df.loc[:,
                                    ['V85','V203','V321','V439','V557','V675','V793','V911','V1029','V1147','V1265','V1383']].sum(axis=1)
                df_req['simple_assault'] = df.loc[:,
                                       ['V90','V208','V326','V444','V562','V680','V798','V916','V1034','V1152','V1270','V1388']].sum(axis=1)
                df_req['aggravated_assault'] = df_req['all_assault'] - df_req['simple_assault']
                df_req['burglary'] = df.loc[:,
                                 ['V91','V209','V327','V445','V563','V681','V799','V917','V1035','V1153','V1271','V1389']].sum(axis=1)
                df_req['larceny'] = df.loc[:,
                                ['V95','V213','V331','V449','V567','V685','V803','V921','V1039','V1157','V1275','V1393']].sum(axis=1)
                df_req['motor_vehicle_theft'] = df.loc[:,
                                            ['V96', 'V214','V332','V450','V568','V686','V804','V922','V1040','V1158','V1276','V1394']].sum(axis=1)

            elif any(x in ['1996', '1997'] for x in fl_name_split):
                df.rename(columns={'V7': 'ORI', 'V10': 'YEAR', 'V16': 'last_month_reported', 'V30': 'AGENCY'}, inplace=True)

                df['population'] = df[['V18', 'V21', 'V24']].sum(axis=1)
                df_req = df.loc[:, ['ORI', 'YEAR', 'AGENCY', 'last_month_reported', 'population']]
                df_req['murder'] = df.loc[:,
                               ['V74','V192','V310','V428','V546','V664','V782','V900','V1018','V1136','V1254','V1372']].sum(axis=1)
                df_req['rape'] = df.loc[:,
                             ['V76','V194','V312','V430','V548','V666','V784','V902','V1020','V1138','V1256','V1374']].sum(axis=1)
                df_req['robbery'] = df.loc[:,
                                ['V79','V197','V315','V433','V551','V669','V787','V905','V1023','V1141','V1259','V1377']].sum(axis=1)
                df_req['all_assault'] = df.loc[:,
                                    ['V84','V202','V320','V438','V556','V674','V792','V910','V1028','V1146','V1264','V1382']].sum(axis=1)
                df_req['simple_assault'] = df.loc[:,
                                       ['V89','V207','V325','V443','V561','V679','V797','V915','V1033','V1151','V1269','V1387']].sum(axis=1)
                df_req['aggravated_assault'] = df_req['all_assault'] - df_req['simple_assault']
                df_req['burglary'] = df.loc[:,
                                 ['V90','V208','V326','V444','V562','V680','V798','V916','V1034','V1152','V1270','V1388']].sum(axis=1)
                df_req['larceny'] = df.loc[:,
                                ['V94','V212','V330','V448','V566','V684','V802','V920','V1038','V1156','V1274','V1392']].sum(axis=1)
                df_req['motor_vehicle_theft'] = df.loc[:,
                                            ['V95','V213','V331','V449','V567','V685','V803','V921','V1039','V1157','V1275','V1393']].sum(axis=1)

                df_req.drop(['all_assault'], axis=1, inplace=True)


            # create total_main_crime column
            df_req['total_main_crime'] = df_req.loc[:, ['murder', 'rape', 'robbery', 'simple_assault', 'aggravated_assault',
                                                        'burglary', 'larceny', 'motor_vehicle_theft']].sum(axis=1)

            ## create rates
            cols = ['murder', 'rape', 'robbery', 'simple_assault', 'aggravated_assault', 'burglary', 'larceny',
                    'motor_vehicle_theft', 'total_main_crime']

            for col in cols:
                df_req[f'{col}_rate'] = (df_req[f'{col}']/df_req['population']) * 100000

                df_req.to_csv(f'/Users/salma/Research/clean_icpsr_crime/data/crime_data/crime_data_req_cols/{os.path.splitext(fl)[0]}_req.csv',
                          index=False)


create_req_cr_files()


def consolidate_cr_files():
    os.chdir('/Users/salma/Research/clean_icpsr_crime/data/crime_data/crime_data_req_cols')

    #cr_1st_file_df = pd.read_csv(first_file, engine='python')
    cr_fls = pd.DataFrame()

    for fl in os.listdir():
        if fl != '.DS_Store' and fl!= 'consolidated':
            df = pd.read_csv(fl)
            cr_fls = cr_fls.append([df], sort=False)

    cr_fls.sort_values(['ORI', 'YEAR'], inplace=True)

    cr_fls.to_csv('/Users/salma/Research/clean_icpsr_crime/data/crime_data/crime_data_req_cols/consolidated/icpsr_cr_90_15.csv',
                              index=False)


consolidate_cr_files()
























'''
ori=V3;
yearucr=&a;
statenum=V2;
agency=V26;
pop=sum(v14,v17,v20);
months=v12+0;
if months=12 and pop>0;
/* Removes state police agencies that show non-zero population   */
if index(agency,'STATE')>0 then delete;
if index(agency,'SP:')>0 then delete;
/* Calculate counts or sums of events   */
murder_sum=sum(v70,v188,v306,v424,v542,v660,v778,v896,v1014,v1132,v1250,v1368);
rape_sum=sum(v72,v190,v308,v426,v544,v662,v780,v898,v1016,v1134,v1252,v1370);
robbery_sum=sum(v75,v193,v311,v429,v547,v665,v783,v901,v1019,v1137,v1255,v1373);
all_assault_sum=sum(v80,v198,v316,v434,v552,v670,v788,v906,v1024,v1142,v1260,v1378);
assault_simple_sum=sum(v85,v203,v321,v439,v557,v675,v793,v911,v1029,v1147,v1265,v1383);
agg_assault_sum=(all_assault_sum-assault_simple_sum);
burglary_sum=sum(v86,v204,v322,v440,v558,v676,v794,v912,v1030,v1148,v1266,v1384);
larceny_sum=sum(v90,v208,v326,v444,v562,v680,v798,v916,v1034,v1152,v1270,v1388);
vehicle_sum=sum(v91,v209,v327,v445,v563,v681,v799,v917,v1035,v1153,v1271,v1389);
/* Calculate offense or crime rates per 100,000 population   */
murder_rate=(murder_sum/pop)*100000;
robbery_rate=(robbery_sum/pop)*100000;
agg_assault_rate=(agg_assault_sum/pop)*100000;
rape_rate=(rape_sum/pop)*100000;
burglary_rate=(burglary_sum/pop)*100000;
larceny_rate=(larceny_sum/pop)*100000;
vehicle_rate=(vehicle_sum/pop)*100000;
violent_rate=sum(murder_rate,rape_rate,robbery_rate,agg_assault_rate);
property_rate=sum(burglary_rate,larceny_rate, vehicle_rate);
keep ori v6 yearucr pop agency months murder_sum rape_sum robbery_sum agg_assault_sum 
all_assault_sum assault_simple_sum burglary_sum larceny_sum vehicle_sum murder_rate rape_rate
robbery_rate agg_assault_rate burglary_rate larceny_rate vehicle_rate violent_rate property_rate;
run;
    '''