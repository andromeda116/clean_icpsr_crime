import pandas as pd

df1 = pd.DataFrame({'A': ['Jan not w oth month', 'January not incl with another month', '5.397605346934027e-79', 9],
                    'B': ['0', 'Missing', '99.0', '5.397605346934027e-79']})

print(df1)

df1.replace('5.397605346934027e-79', 0, inplace=True)
print()

print(df1)