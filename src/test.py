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
    df_merged = pd.merge(df_latest, df_original, on='employee_number', how='left', suffixes=('', '_last'), indicator=True)
    # Adding prefix to employee number (if needed)
    df_merged['employee_number'] = 'N' + df_merged['employee_number']
        # Filtering the latest DataFrame for a specific indicator
    df_merged = df_merged[df_merged['labindicator'] == 'Y']
    # Identify new entries explicitly
    df_merged['is_new'] = df_merged['_merge'] == 'left_only'
    
    # Initialize a column to track changes (excluding specific columns for changed entries)
    df_merged['has_changes'] = False

    # Finding all columns to check for changes, excluding the excluded columns for changed entries
    columns_of_interest = [col for col in df_latest.columns if col not in excluded_columns and col != 'employee_number']

    # Identify changed entries in columns of interest
    for col in columns_of_interest:
        df_merged[f'{col}_changed'] = (df_merged[col] != df_merged[f'{col}_last']) & pd.notnull(df_merged[f'{col}_last'])
        df_merged['has_changes'] |= df_merged[f'{col}_changed']

    # Separate handling for new and changed rows to ensure all columns are included for new entries
    new_entries = df_merged[df_merged['is_new']]
    changed_entries = df_merged[(df_merged['has_changes']) & (~df_merged['is_new'])]

    # For changed entries, exclude specific columns
    final_columns_changed = ['employee_number'] + columns_of_interest
    changed_entries_final = changed_entries[final_columns_changed]

    # For new entries, include all columns from the latest file
    new_entries_final = new_entries[df_latest.columns]

    # Combine new and changed entries
    delta_df_final = pd.concat([new_entries_final, changed_entries_final], ignore_index=True)

    # Save the delta DataFrame to a new CSV file
    delta_df_final.to_csv(delta_csv_path, index=False, sep='|')
    
if __name__ == '__main__':
    original_csv_path = './resources/yesterday_data.csv'  # Adjust this to your original file's path
    latest_csv_path = './resources/today_data.csv'  # Adjust this to your latest file's path
    delta_csv_path = './resources/delta_file.csv'  # Adjust this to where you want the delta file saved
    generate_delta_file(original_csv_path, latest_csv_path, delta_csv_path)
