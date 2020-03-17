import pandas as pd

df = pd.DataFrame({'V1': 10, 'V2': [20], 'V3': [30],'V11': 5, 'V22': [10], 'V33': [15]})

all_assault = ['V1', 'V2', 'V3']
simple_assault = ['V11', 'V22', 'V33']

months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

for mon, all, sim in zip(months, all_assault, simple_assault):
    df[f'{mon}_agg_assault'] = df[f'{all}'] - df[f'{sim}']


print(df)


"""

import pandas as pd
import os
import re

from builtins import zip


def create_cr_file_with_monthly_vars():
    os.chdir('/Users/salma/Research/clean_icpsr_crime/data/crime_data')

    for fl in os.listdir():
        if fl != '.DS_Store' and fl != 'crime_data_req_cols' and fl != 'crime_data_req_monthly':
            if os.path.splitext(fl)[1] == '.tsv':
                df = pd.read_csv(fl, sep='\t', encoding='cp1252')

            elif os.path.splitext(fl)[1] == '.xpt':
                # with open(fl, 'rb') as f:
                #    df = xport.to_dataframe(f)
                # faster than xport
                df = pd.read_sas(fl)

            elif os.path.splitext(fl)[1] == '.txt':
                df = pd.read_table(fl, encoding='cp1252')
                # data = pd.read_csv('file.txt', sep=',')

            elif os.path.splitext(fl)[1] == '.sav':
                df = pd.read_spss(fl)

            fl_name = os.path.splitext(fl)[0]
            fl_name_split = re.split('_|-', fl_name)

            if '1990' in fl_name_split:
                df_req = df.loc[:, ['V7', 'V10', 'V19', 'V22', 'V25', 'V35',
                                    'V80', 'V198', 'V316', 'V434', 'V552', 'V670', 'V788', 'V906', 'V1024', 'V1142','V1260', 'V1378', # murder
                                    'V82', 'V200', 'V318', 'V436', 'V554', 'V672', 'V790', 'V908', 'V1026', 'V1144', 'V1262', 'V1380',  # rape
                                    'V85', 'V203', 'V321', 'V439', 'V557', 'V675', 'V793', 'V911', 'V1029', 'V1147','V1265', 'V1383',  # robbery
                                    'V90', 'V208', 'V326', 'V444', 'V562', 'V680', 'V798', 'V916', 'V1034', 'V1152', 'V1270', 'V1388', # all_assault
                                    'V95', 'V213', 'V331', 'V449', 'V567', 'V685', 'V803', 'V921', 'V1039', 'V1157', 'V1275', 'V1393', # simple_assault
                                    'V96', 'V214', 'V332', 'V450', 'V568', 'V686', 'V804', 'V922', 'V1040', 'V1158', 'V1276', 'V1394', # burglary
                                    'V100', 'V218', 'V336', 'V454', 'V572', 'V690', 'V808', 'V926', 'V1044', 'V1162', 'V1280', 'V1398', # larceny
                                    'V101', 'V219', 'V337', 'V455', 'V573', 'V691', 'V809', 'V927', 'V1045', 'V1163', 'V1281', 'V1399', # motor_vehicle_theft
                                    'V43', 'V161', 'V279', 'V397', 'V515', 'V633', 'V751', 'V869', 'V987', 'V1105', 'V1223', 'V1341', # month_included_in
                                    'V51', 'V169', 'V287', 'V405', 'V523', 'V641', 'V759', 'V877', 'V995', 'V1113','V1231', 'V1349', # card_1_pt
                                    'V46', 'V164', 'V282', 'V400', 'V518', 'V636', 'V754', 'V872', 'V990', 'V1108', 'V1226', 'V1344' # card_1_type
                                    ]]

                df_req['population'] = df_req[['V19', 'V22', 'V25']].sum(axis=1)

                df_req.rename(columns={'V7': 'ORI', 'V10': 'YEAR', 'V35': 'AGENCY'}, inplace=True)

                cr_cols = ['murder', 'rape', 'robbery', 'all_assault', 'simple_assault', 'burglary', 'larceny',
                           'motor_vehicle_theft', 'month_included_in', 'card_1_pt', 'card_1_type']

                months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

                # calculate agg_assault for each month
                all_assault = ['V90', 'V208', 'V326', 'V444', 'V562', 'V680', 'V798', 'V916', 'V1034', 'V1152', 'V1270', 'V1388']  # all_assault
                simple_assault = ['V95', 'V213', 'V331', 'V449', 'V567', 'V685', 'V803', 'V921', 'V1039', 'V1157', 'V1275', 'V1393']  # simple_assault

                for mon, all, sim in zip(months, all_assault, simple_assault):
                    df_req[f'{mon}_agg_assault'] = df_req[f'{all}'] - df_req[f'{sim}']

                # drop pop1, pop2, pop3, all_assault columns
                df_req.drop(
                    ['V19', 'V22', 'V25', 'V90', 'V208', 'V326', 'V444', 'V562', 'V680', 'V798', 'V916', 'V1034',
                     'V1152', 'V1270', 'V1388'], axis=1, inplace=True)

                # rename columns
                cols_to_be_renamed = [['V80', 'V198', 'V316', 'V434', 'V552', 'V670', 'V788', 'V906', 'V1024', 'V1142','V1260', 'V1378'],
                                      ['V82', 'V200', 'V318', 'V436', 'V554', 'V672', 'V790', 'V908', 'V1026', 'V1144', 'V1262', 'V1380'],
                                      ['V85', 'V203', 'V321', 'V439', 'V557', 'V675', 'V793', 'V911', 'V1029', 'V1147','V1265', 'V1383'],
                                      ['V90', 'V208', 'V326', 'V444', 'V562', 'V680', 'V798', 'V916', 'V1034', 'V1152', 'V1270', 'V1388'],
                                      ['V95', 'V213', 'V331', 'V449', 'V567', 'V685', 'V803', 'V921', 'V1039', 'V1157', 'V1275', 'V1393'],
                                      ['V96', 'V214', 'V332', 'V450', 'V568', 'V686', 'V804', 'V922', 'V1040', 'V1158', 'V1276', 'V1394'],
                                      ['V100', 'V218', 'V336', 'V454', 'V572', 'V690', 'V808', 'V926', 'V1044', 'V1162', 'V1280', 'V1398'],
                                      ['V101', 'V219', 'V337', 'V455', 'V573', 'V691', 'V809', 'V927', 'V1045', 'V1163', 'V1281', 'V1399'],
                                      ['V43', 'V161', 'V279', 'V397', 'V515', 'V633', 'V751', 'V869', 'V987', 'V1105', 'V1223', 'V1341'],
                                      ['V51', 'V169', 'V287', 'V405', 'V523', 'V641', 'V759', 'V877', 'V995', 'V1113','V1231', 'V1349'],
                                      ['V46', 'V164', 'V282', 'V400', 'V518', 'V636', 'V754', 'V872', 'V990', 'V1108', 'V1226', 'V1344']]


                for cr_col, rename_col in zip(cr_cols, cols_to_be_renamed):
                    for m, c in zip(months, rename_col):
                        df_req.rename(columns = {f'{c}': f'{m}'+'_'+cr_col}, inplace=True)


            elif any(x in ['1991', '1992', '1994', '1995', '1998', '1999', '2000', '2001', '2002', '2003', '2004',
                           '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015']
                     for x in fl_name_split):
                df_req = df.loc[:, ['V3', 'V6', 'V14', 'V17', 'V20', 'V26',
                                    'V70', 'V188', 'V306', 'V424', 'V542', 'V660', 'V778', 'V896', 'V1014', 'V1132',
                                    'V1250', 'V1368',  # murder
                                    'V72', 'V190', 'V308', 'V426', 'V544', 'V662', 'V780', 'V898', 'V1016', 'V1134',
                                    'V1252', 'V1370',  # rape
                                    'V75', 'V193', 'V311', 'V429', 'V547', 'V665', 'V783', 'V901', 'V1019', 'V1137',
                                    'V1255', 'V1373',  # robbery
                                    'V80', 'V198', 'V316', 'V434', 'V552', 'V670', 'V788', 'V906', 'V1024', 'V1142',
                                    'V1260', 'V1378',  # all_assault
                                    'V85', 'V203', 'V321', 'V439', 'V557', 'V675', 'V793', 'V911', 'V1029', 'V1147',
                                    'V1265', 'V1383',  # simple_assault
                                    'V86', 'V204', 'V322', 'V440', 'V558', 'V676', 'V794', 'V912', 'V1030', 'V1148',
                                    'V1266', 'V1384',  # burglary
                                    'V90', 'V208', 'V326', 'V444', 'V562', 'V680', 'V798', 'V916', 'V1034', 'V1152',
                                    'V1270', 'V1388',  # larceny
                                    'V91', 'V209', 'V327', 'V445', 'V563', 'V681', 'V799', 'V917', 'V1035', 'V1153',
                                    'V1271', 'V1389',  # motor_vehicle_theft
                                    'V33', 'V151', 'V269', 'V387', 'V505', 'V623', 'V741', 'V859','V977', 'V1095',
                                    'V1213', 'V1331',  # month_included_in
                                    'V41', 'V159', 'V277', 'V395', 'V513', 'V631', 'V749', 'V867', 'V985', 'V1103',
                                    'V1221', 'V1339',  # card_1_pt
                                    'V36', 'V154', 'V272', 'V390', 'V508', 'V626', 'V744', 'V862', 'V980', 'V1098',
                                    'V1216', 'V1334'  # card_1_type
                                    ]]

                df_req['population'] = df_req[['V14', 'V17', 'V20']].sum(axis=1)

                df_req.rename(columns={'V3': 'ORI', 'V6': 'YEAR', 'V26': 'AGENCY'}, inplace=True)

                cr_cols = ['murder', 'rape', 'robbery', 'all_assault', 'simple_assault', 'burglary', 'larceny',
                           'motor_vehicle_theft', 'month_included_in', 'card_1_pt', 'card_1_type']

                months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

                # calculate agg_assault for each month
                all_assault = ['V80', 'V198', 'V316', 'V434', 'V552', 'V670', 'V788', 'V906', 'V1024', 'V1142', 'V1260', 'V1378']  # all_assault
                simple_assault = ['V85', 'V203', 'V321', 'V439', 'V557', 'V675', 'V793', 'V911', 'V1029', 'V1147',
                                    'V1265', 'V1383']  # simple_assault

                for mon, all, sim in zip(months, all_assault, simple_assault):
                    df_req[f'{mon}_agg_assault'] = df_req[f'{all}'] - df_req[f'{sim}']

                df_req.drop(
                    ['V14', 'V17', 'V20', 'V80', 'V198', 'V316', 'V434', 'V552', 'V670', 'V788', 'V906', 'V1024',
                     'V1142', 'V1260', 'V1378'], axis=1, inplace=True)

                # rename columns
                cols_to_be_renamed = [
                    ['V70', 'V188', 'V306', 'V424', 'V542', 'V660', 'V778', 'V896', 'V1014', 'V1132', 'V1250', 'V1368'],
                    ['V72', 'V190', 'V308', 'V426', 'V544', 'V662', 'V780', 'V898', 'V1016', 'V1134', 'V1252', 'V1370'],
                    ['V75', 'V193', 'V311', 'V429', 'V547', 'V665', 'V783', 'V901', 'V1019', 'V1137', 'V1255', 'V1373'],
                    ['V80', 'V198', 'V316', 'V434', 'V552', 'V670', 'V788', 'V906', 'V1024', 'V1142', 'V1260', 'V1378'],
                    ['V85', 'V203', 'V321', 'V439', 'V557', 'V675', 'V793', 'V911', 'V1029', 'V1147', 'V1265', 'V1383'],
                    ['V86', 'V204', 'V322', 'V440', 'V558', 'V676', 'V794', 'V912', 'V1030', 'V1148', 'V1266', 'V1384'],
                    ['V90', 'V208', 'V326', 'V444', 'V562', 'V680', 'V798', 'V916', 'V1034', 'V1152', 'V1270', 'V1388'],
                    ['V91', 'V209', 'V327', 'V445', 'V563', 'V681', 'V799', 'V917', 'V1035', 'V1153', 'V1271', 'V1389'],
                    ['V33', 'V151', 'V269', 'V387', 'V505', 'V623', 'V741', 'V859','V977', 'V1095', 'V1213', 'V1331'],
                    ['V41', 'V159', 'V277', 'V395', 'V513', 'V631', 'V749', 'V867', 'V985', 'V1103', 'V1221', 'V1339'],
                    ['V36', 'V154', 'V272', 'V390', 'V508', 'V626', 'V744', 'V862', 'V980', 'V1098', 'V1216', 'V1334']]

                for cr_col, rename_col in zip(cr_cols, cols_to_be_renamed):
                    for m, c in zip(months, rename_col):
                        df_req.rename(columns={f'{c}': f'{m}' + '_' + cr_col}, inplace=True)


            elif '1993' in fl_name_split:
                df_req = df.loc[:, ['V7', 'V10', 'V18', 'V21', 'V24', 'V31',
                                    'V75', 'V193','V311','V429','V547','V665','V783','V901','V1019','V1137','V1255','V1373',  # murder
                                    'V77','V195','V313','V431','V549','V667','V785','V903','V1021','V1139','V1257','V1375',  # rape
                                    'V80', 'V198','V316','V434','V552','V670','V788','V906','V1024','V1142','V1260','V1378',  # robbery
                                    'V85','V203','V321','V439','V557','V675','V793','V911','V1029','V1147','V1265','V1383',  # all_assault
                                    'V90','V208','V326','V444','V562','V680','V798','V916','V1034','V1152','V1270','V1388',  # simple_assault
                                    'V91','V209','V327','V445','V563','V681','V799','V917','V1035','V1153','V1271','V1389',  # burglary
                                    'V95','V213','V331','V449','V567','V685','V803','V921','V1039','V1157','V1275','V1393',  # larceny
                                    'V96', 'V214','V332','V450','V568','V686','V804','V922','V1040','V1158','V1276','V1394',  # motor_vehicle_theft
                                    'V38', 'V156', 'V274', 'V392', 'V510', 'V628', 'V746', 'V864', 'V982', 'V1100', 'V1218', 'V1336',  # month_included_in
                                    'V46', 'V164', 'V282', 'V400', 'V518', 'V636', 'V754', 'V872', 'V990', 'V1108', 'V1226', 'V1344',  # card_1_pt
                                    'V41', 'V159', 'V277', 'V395', 'V513', 'V631', 'V749', 'V867', 'V985', 'V1103', 'V1221', 'V1339'  # card_1_type
                                    ]]

                df_req['population'] = df_req[['V18', 'V21', 'V24']].sum(axis=1)

                df_req.rename(columns={'V7': 'ORI', 'V10': 'YEAR', 'V31': 'AGENCY'}, inplace=True)

                cr_cols = ['murder', 'rape', 'robbery', 'all_assault', 'simple_assault', 'burglary', 'larceny',
                           'motor_vehicle_theft', 'month_included_in', 'card_1_pt', 'card_1_type']

                months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

                # calculate agg_assault for each month
                all_assault = ['V85','V203','V321','V439','V557','V675','V793','V911','V1029','V1147','V1265','V1383']  # all_assault
                simple_assault = ['V90','V208','V326','V444','V562','V680','V798','V916','V1034','V1152','V1270','V1388']  # simple_assault

                for mon, all, sim in zip(months, all_assault, simple_assault):
                    df_req[f'{mon}_agg_assault'] = df_req[f'{all}'] - df_req[f'{sim}']

                df_req.drop(
                    ['V18', 'V21', 'V24', 'V85', 'V203', 'V321', 'V439', 'V557', 'V675', 'V793', 'V911', 'V1029',
                     'V1147',
                     'V1265', 'V1383'], axis=1, inplace=True)

                # rename columns
                cols_to_be_renamed = [
                    ['V75', 'V193','V311','V429','V547','V665','V783','V901','V1019','V1137','V1255','V1373'],
                    ['V77','V195','V313','V431','V549','V667','V785','V903','V1021','V1139','V1257','V1375'],
                    ['V80', 'V198','V316','V434','V552','V670','V788','V906','V1024','V1142','V1260','V1378'],
                    ['V85','V203','V321','V439','V557','V675','V793','V911','V1029','V1147','V1265','V1383'],
                    ['V90','V208','V326','V444','V562','V680','V798','V916','V1034','V1152','V1270','V1388'],
                    ['V91','V209','V327','V445','V563','V681','V799','V917','V1035','V1153','V1271','V1389'],
                    ['V95','V213','V331','V449','V567','V685','V803','V921','V1039','V1157','V1275','V1393'],
                    ['V96', 'V214','V332','V450','V568','V686','V804','V922','V1040','V1158','V1276','V1394'],
                    ['V38', 'V156', 'V274', 'V392', 'V510', 'V628', 'V746', 'V864', 'V982', 'V1100', 'V1218', 'V1336'],
                    ['V46', 'V164', 'V282', 'V400', 'V518', 'V636', 'V754', 'V872', 'V990', 'V1108', 'V1226', 'V1344'],
                    ['V41', 'V159', 'V277', 'V395', 'V513', 'V631', 'V749', 'V867', 'V985', 'V1103', 'V1221', 'V1339']]


                for cr_col, rename_col in zip(cr_cols, cols_to_be_renamed):
                    for m, c in zip(months, rename_col):
                        df_req.rename(columns={f'{c}': f'{m}' + '_' + cr_col}, inplace=True)


            elif any(x in ['1996', '1997'] for x in fl_name_split):
                df_req = df.loc[:, ['V7', 'V10', 'V18', 'V21', 'V24', 'V30',
                                    'V74','V192','V310','V428','V546','V664','V782','V900','V1018','V1136','V1254','V1372',  # murder
                                    'V76','V194','V312','V430','V548','V666','V784','V902','V1020','V1138','V1256','V1374',  # rape
                                    'V79','V197','V315','V433','V551','V669','V787','V905','V1023','V1141','V1259','V1377',  # robbery
                                    'V84','V202','V320','V438','V556','V674','V792','V910','V1028','V1146','V1264','V1382',  # all_assault
                                    'V89','V207','V325','V443','V561','V679','V797','V915','V1033','V1151','V1269','V1387',  # simple_assault
                                    'V90','V208','V326','V444','V562','V680','V798','V916','V1034','V1152','V1270','V1388',  # burglary
                                    'V94','V212','V330','V448','V566','V684','V802','V920','V1038','V1156','V1274','V1392',  # larceny
                                    'V95','V213','V331','V449','V567','V685','V803','V921','V1039','V1157','V1275','V1393',  # motor_vehicle_theft
                                    'V37', 'V155', 'V273', 'V391', 'V509', 'V627', 'V745', 'V863', 'V981', 'V1099', 'V1217', 'V1335',  # month_included_in
                                    'V45', 'V163', 'V280', 'V399', 'V517', 'V635', 'V753', 'V871', 'V989', 'V1107', 'V1225', 'V1343',  # card_1_pt
                                    'V40', 'V158', 'V275', 'V394', 'V512', 'V630', 'V748', 'V866', 'V984', 'V1102', 'V1220', 'V1338'  # card_1_type
                                    ]]

                df_req['population'] = df_req[['V18', 'V21', 'V24']].sum(axis=1)

                df_req.rename(columns={'V7': 'ORI', 'V10': 'YEAR', 'V30': 'AGENCY'}, inplace=True)

                cr_cols = ['murder', 'rape', 'robbery', 'all_assault', 'simple_assault', 'burglary', 'larceny',
                           'motor_vehicle_theft', 'month_included_in', 'card_1_pt', 'card_1_type']

                months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

                # calculate agg_assault for each month
                all_assault = ['V84','V202','V320','V438','V556','V674','V792','V910','V1028','V1146','V1264','V1382']  # all_assault
                simple_assault = ['V89','V207','V325','V443','V561','V679','V797','V915','V1033','V1151','V1269','V1387']  # simple_assault

                for mon, all, sim in zip(months, all_assault, simple_assault):
                    df_req[f'{mon}_agg_assault'] = df_req[f'{all}'] - df_req[f'{sim}']

                df_req.drop(
                    ['V18', 'V21', 'V24', 'V84', 'V202', 'V320', 'V438', 'V556', 'V674', 'V792', 'V910', 'V1028',
                     'V1146',
                     'V1264', 'V1382'], axis=1, inplace=True)

                # rename columns
                cols_to_be_renamed = [
                    ['V74','V192','V310','V428','V546','V664','V782','V900','V1018','V1136','V1254','V1372'],
                    ['V76','V194','V312','V430','V548','V666','V784','V902','V1020','V1138','V1256','V1374'],
                    ['V79','V197','V315','V433','V551','V669','V787','V905','V1023','V1141','V1259','V1377'],
                    ['V84','V202','V320','V438','V556','V674','V792','V910','V1028','V1146','V1264','V1382'],
                    ['V89','V207','V325','V443','V561','V679','V797','V915','V1033','V1151','V1269','V1387'],
                    ['V90','V208','V326','V444','V562','V680','V798','V916','V1034','V1152','V1270','V1388'],
                    ['V94','V212','V330','V448','V566','V684','V802','V920','V1038','V1156','V1274','V1392'],
                    ['V95','V213','V331','V449','V567','V685','V803','V921','V1039','V1157','V1275','V1393'],
                    ['V37', 'V155', 'V273', 'V391', 'V509', 'V627', 'V745', 'V863', 'V981', 'V1099', 'V1217', 'V1335'],
                    ['V45', 'V163', 'V280', 'V399', 'V517', 'V635', 'V753', 'V871', 'V989', 'V1107', 'V1225', 'V1343'],
                    ['V40', 'V158', 'V275', 'V394', 'V512', 'V630', 'V748', 'V866', 'V984', 'V1102', 'V1220', 'V1338']]


                for cr_col, rename_col in zip(cr_cols, cols_to_be_renamed):
                    for m, c in zip(months, rename_col):
                        df_req.rename(columns={f'{c}': f'{m}' + '_' + cr_col}, inplace=True)

            df_req.to_csv(
                f'/Users/salma/Research/clean_icpsr_crime/data/crime_data/crime_data_req_monthly/{os.path.splitext(fl)[0]}_req.csv',
                index=False)


create_cr_file_with_monthly_vars()


def consolidate_monthly_cr_files():
    os.chdir('/Users/salma/Research/clean_icpsr_crime/data/crime_data/crime_data_req_monthly')

    #cr_1st_file_df = pd.read_csv(first_file, engine='python')
    cr_fls = pd.DataFrame()

    for fl in os.listdir():
        if fl != '.DS_Store' and fl!= 'consolidated':
            df = pd.read_csv(fl)
            cr_fls = cr_fls.append([df], sort=False)

    cr_fls.sort_values(['ORI', 'YEAR'], inplace=True)

    cr_fls.to_csv('/Users/salma/Research/clean_icpsr_crime/data/crime_data/crime_data_req_monthly/consolidated/icpsr_cr_monthly_90_15.csv',
                              index=False)


consolidate_monthly_cr_files()


"""