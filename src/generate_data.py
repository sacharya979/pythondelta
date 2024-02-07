import csv
import os
from faker import Faker

fake = Faker()

# Ensure the 'resources' directory exists
os.makedirs('../resources', exist_ok=True)

# Define the header of the CSV file
header = ['employee_number', 'last_name', 'first_name', 'address', 'labindicator']

# Generate 10 CSV files
for file_number in range(2):
    file_name = f'./resources/data_{file_number + 1}.csv'
    
    with open(file_name, mode='w', newline='') as file:
        writer = csv.writer(file, delimiter='|')
        writer.writerow(header)
        
        # Generate 10 sets of fake data for each CSV file
        for _ in range(10):
            employee_number = fake.unique.random_int(min=1000, max=9999)
            last_name = fake.last_name()
            first_name = fake.first_name()
            address = fake.address().replace('\n', ', ')
            labindicator = fake.random_element(elements=('Y', 'N'))
            
            writer.writerow([employee_number, last_name, first_name, address, labindicator])

print("CSV files have been generated.")
