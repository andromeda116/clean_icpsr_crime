import pandas as pd
import numpy as np


pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 5000)

df = pd.DataFrame({'jan_card_1_type':[2, 5, 2.0, 5.0, 10, 'type'],
                   'jan_card_1_pt': ['P', 'T', 0, 'pt_test', 77, 'ttt'],
                   'jan_month_included_in':[0, 769, 'ghg', 789, 0, 'ytre'],
                   'feb_card_1_type': [2, 5, 2.0, 5.0, 10, 'type'],
                   'feb_card_1_pt': ['P', 'T', 0, 'pt_test', 77, 'ttt'],
                   'feb_month_included_in': [0, 769, 'ghg', 789, 0, 'ytre']
                   })

months = ['jan', 'feb']

for mon in months:
    df[f'{mon}_fbi_month'] = np.where((((df[f'{mon}_card_1_type'].isin([2, 5, 2.0, 5.0])) |
                                               (df[f'{mon}_card_1_pt'].isin(['P', 'T']))) &
                                               (df[f'{mon}_month_included_in'] == 0)), True, False)

print(df)