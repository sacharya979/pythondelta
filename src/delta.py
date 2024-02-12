import pandas as pd

def generate_delta_file(original_csv_path, latest_csv_path, delta_csv_path):
    """
    Generates a delta file including records from the latest file,
    accounting for any changes excluding specific columns,
    and including new records.
    """

    # Load CSV files
    df_original = pd.read_csv(original_csv_path, delimiter='|', dtype={'employee_number': str})
    df_latest = pd.read_csv(latest_csv_path, delimiter='|', dtype={'employee_number': str})

    # Identifying columns to exclude from change detection
    excluded_columns = ['name', 'last_name', 'address']

    # Merge DataFrames with proper suffixes for columns in the original DataFrame
    df_merged = pd.merge(df_latest, df_original, on='employee_number', how='outer', suffixes=('', '_last'), indicator=True)

    # Finding all columns that we want to check for changes, excluding the excluded columns
    columns_of_interest = [col for col in df_latest.columns if col not in excluded_columns and col != 'employee_number']

    # Initialize a column to track changes (excluding specific columns)
    df_merged['has_changes'] = False

    # Identify changed entries in columns of interest, excluding name and address-related changes
    for col in columns_of_interest:
        # Correctly handling the comparison
        df_merged[f'{col}_changed'] = (df_merged[col] != df_merged[f'{col}_last']) & pd.notnull(df_merged[f'{col}_last'])
        # Update 'has_changes' to True if any change is detected in columns of interest
        df_merged['has_changes'] |= df_merged[f'{col}_changed']

    # Filter for rows that are new or have changes in columns of interest
    delta_df = df_merged[(df_merged['_merge'] == 'left_only') | (df_merged['has_changes'])]

    # Selecting columns from the latest data to include in the delta DataFrame
    delta_columns = [col for col in df_latest.columns]  # Includes only columns from "today's" data
    delta_df_final = delta_df[delta_columns]

    # Save the delta DataFrame to a new CSV file
    delta_df_final.to_csv(delta_csv_path, index=False, sep='|')
    
if __name__ == '__main__':
    original_csv_path = './resources/yesterday_data.csv'  # Adjust this to your original file's path
    latest_csv_path = './resources/today_data.csv'  # Adjust this to your latest file's path
    delta_csv_path = './resources/delta_file.csv'  # Adjust this to where you want the delta file saved
    generate_delta_file(original_csv_path, latest_csv_path, delta_csv_path)
