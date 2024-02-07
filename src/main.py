import pandas as pd

def generate_delta_file(original_csv_path, latest_csv_path, delta_csv_path):
    """
    Generates a delta file including records from the latest file where 'labindicator' is 'Y',
    accounting for any changes in 'last_name', and excluding records with 'labindicator' 'N'.
    
    :param original_csv_path: Path to the original CSV file (yesterday's data).
    :param latest_csv_path: Path to the latest CSV file (today's data).
    :param delta_csv_path: Path to save the delta CSV file.
    """
    # Load CSV files
    df_original = pd.read_csv(original_csv_path, delimiter='|', dtype={'employee_number': str})
    df_latest = pd.read_csv(latest_csv_path, delimiter='|', dtype={'employee_number': str})

    # Filter latest DataFrame for 'labindicator' == 'Y'
    df_filtered = df_latest[df_latest['labindicator'] == 'Y']

    # Merge the original names to capture any name changes
    merged_df = df_filtered.merge(df_original[['employee_number', 'last_name']], on='employee_number', 
                                  how='left', suffixes=('', '_orig'))

    # Check for last_name changes; if no change, use the latest name
    merged_df['last_name'] = merged_df.apply(lambda row: row['last_name'] if pd.notna(row['last_name']) else row['last_name_orig'], axis=1)
    
    # Drop the original last_name column as it's no longer needed
    clean_df = merged_df.drop(columns=['last_name_orig'])

    # Ensure the final DataFrame has columns in the correct order
    final_df = clean_df[['employee_number', 'last_name', 'first_name', 'address', 'labindicator']]

    # Save the delta DataFrame to a CSV file
    final_df.to_csv(delta_csv_path, sep='|', index=False)
    print(f"Delta file generated successfully at {delta_csv_path}.")

if __name__ == '__main__':
    original_csv_path = './resources/data_1.csv'  # Adjust this to your original file's path
    latest_csv_path = './resources/data_today.csv'  # Adjust this to your latest file's path
    delta_csv_path = './resources/delta_file.csv'  # Adjust this to where you want the delta file saved

    generate_delta_file(original_csv_path, latest_csv_path, delta_csv_path)
