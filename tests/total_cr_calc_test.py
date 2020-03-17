import pandas as pd
import timeit

pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 500)

df = pd.DataFrame({'jan_murder': [10], 'feb_murder': [20], 'mar_murder': [30],'jan_rape': [5], 'feb_rape': [10], 'mar_rape': [15]})

months = ['jan', 'feb', 'mar']

start_time = timeit.default_timer()

# Option 1: to calculate monthly total crimes using filter
# for mon in months:
#     df[f'{mon}_total_crime'] = df.filter(regex=f'{mon}').sum(axis=1)
#
# print(timeit.default_timer() - start_time) # 0.004554068000000022 ms
# print(df, '\n')

# Option 2: to calculate monthly total crimes using list comprehension
for mon in months:
    df[f'{mon}_total_crime'] = df.loc[:, [col for col in list(df) if col.startswith(f'{mon}')]].sum(axis=1)

print(timeit.default_timer() - start_time) # 0.003999500000000045 ms

print(df)