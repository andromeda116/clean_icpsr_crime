import pandas as pd

def get_value_counts(ip_file, var_list, op_path):
    df = pd.read_csv(ip_file)

    for var in var_list:
        counts = [df[f'{col}'].value_counts() for col in list(df) if col.endswith(f'{var}')]
        counts_df = pd.DataFrame(counts)
        print(counts_df)
        counts_df.reset_index().to_csv(f'{op_path}/{var}_value_counts.csv', index=False)