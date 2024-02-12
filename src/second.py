import pandas as pd

def generate_delta_file(original_csv_path, latest_csv_path, delta_csv_path):
     # Load CSV files
    df_original = pd.read_csv(original_csv_path, delimiter='|', dtype={'employee_number': str})
    df_latest = pd.read_csv(latest_csv_path, delimiter='|', dtype={'employee_number': str})

    # Merge DataFrames with proper suffixes for columns in the original DataFrame
    # Exclude the key from the suffix addition
    df_original_suffix = df_original.set_index('employee_number').add_suffix('_orig').reset_index()
    df_merged = pd.merge(df_latest, df_original_suffix, on='employee_number', how='left', indicator=True)
    
    # Adding prefix to employee number (if needed)
    df_merged['employee_number'] = 'N' + df_merged['employee_number']
    # Columns to exclude from change detection (name and address-related)
    excluded_columns = ['name', 'last_name', 'address', 'employee_number']

    # Build the filter condition for changed columns (excluding the excluded columns)
    # Check for columns that exist in both dataframes (with suffix _orig)
    columns_to_check = set(df_original.columns) & set(df_latest.columns) - set(excluded_columns)
    filter_conditions = [f"({col} != {col}_orig)" for col in columns_to_check]
    filter_condition = " or ".join(filter_conditions)

    # Use query to filter rows with changes or new entries (left_only)
    df_delta = df_merged.query(filter_condition + " or _merge == 'left_only'")

    # Select only the columns from the latest dataframe for the output
    df_delta_final = df_delta[df_latest.columns]

    # Save the delta dataframe to the specified CSV file
    df_delta_final.to_csv(delta_csv_path, index=False, sep='|')


if __name__ == '__main__':
    original_csv_path = './resources/yesterday_data.csv'  # Adjust this to your original file's path
    latest_csv_path = './resources/today_data.csv'  # Adjust this to your latest file's path
    delta_csv_path = './resources/delta_file.csv'  # Adjust this to where you want the delta file saved
    generate_delta_file(original_csv_path, latest_csv_path, delta_csv_path)
