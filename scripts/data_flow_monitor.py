import csv
import numpy as np
from datetime import datetime
import requests
import smtplib
import argparse

parser = argparse.ArgumentParser(description='ENA data flow monitoring tool\nThis tool will take a taxon id as input and return a summary stats on collection dates\n'
                                             'Note: This uses tax_tree style searching')

parser.add_argument('-t',
                    '--taxon_id',
                    help="A valid taxon id",
                    type=str,
                    required=True)

parser.add_argument('-e',
                    '--email',
                    help="Turn email function on or off (Default = off)",
                    choices=['On','Off'],
                    default='Off',
                    required=True)

args = parser.parse_args()

#The below function is for querying the portal API. I reccommend comparing this to a curl query to understand how to structure this in Python
def query_api(taxon):
    url = "https://www.ebi.ac.uk/ena/portal/api/search"
    headers = {'Content-Type':'application/x-www-form-urlencoded'}
    payload = {f'result':'read_run', 'query':f'tax_tree{taxon}', 'fields':['accession%2Ccollection_date%2Cfirst_created'], 'limit':'0','format':'tsv'}
    print(payload)
    r = requests.post(url, data=payload, headers=headers)
    results = r.text
    with open('input.tsv', 'w') as output:
        output.write(results)

# Open the TSV file for reading
def calculate():
    with open('input.tsv', 'r') as tsv_file:
        # Create a CSV reader for the TSV file
        reader = csv.DictReader(tsv_file, delimiter='\t')

        # Initialize variables to store the time differences and latest collection date
        time_diffs = []
        latest_date = None
        earliest_date = None

        # Iterate over the rows in the TSV file
        for row in reader:
            # Try to parse the dates in the "collection_date" and "first_created" columns
            try:
                collection_date = datetime.strptime(row['collection_date'], '%Y-%m-%d')
                first_created = datetime.strptime(row['first_created'], '%Y-%m-%d')
            except ValueError:
                # Skip this row if the date is not in the expected format
                continue

            # Update the latest collection date if the current date is later than the current latest date
            if latest_date is None or collection_date > latest_date:
                latest_date = collection_date

            # Same for earliest date
            if earliest_date is None or collection_date < earliest_date:
                earliest_date = collection_date

            # Calculate the time difference in days
            time_diff = (first_created - collection_date).days

            # Add the time difference to the list
            time_diffs.append(time_diff)

        # Calculate the mean time difference in days
        mean_time_diff = sum(time_diffs) / len(time_diffs)

        # Calculate the median time difference in days
        time_diffs.sort()
        median_time_diff = time_diffs[len(time_diffs) // 2]

        # Calculate the minimum and maximum time differences
        min_time_diff = min(time_diffs)
        max_time_diff = max(time_diffs)

        # Calculate the 10th and 90th percentiles
        p10 = np.percentile(time_diffs, 10)
        p90 = np.percentile(time_diffs, 90)

        # Print the results to the console
        return (
        f'Mean time difference: {mean_time_diff:.2f} days\n'
        f'Median time difference: {median_time_diff:.2f} days\n'
        f'Minimum time difference: {min_time_diff:.2f} days\n'
        f'Maximum time difference: {max_time_diff:.2f} days\n'
        f'10th percentile: {p10:.2f} days\n'
        f'90th percentile: {p90:.2f} days\n'
        f'Latest collection date: {latest_date.strftime("%Y-%m-%d")}\n'
        f'Earliest collection date: {earliest_date.strftime("%Y-%m-%d")}\n'
        )


def send_email(username, password, subject, body, recipients):
  # Set up the SMTP server for Gmail
  server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
  server.login(username, password)

  # Create the email
  msg = f'Subject: {subject}\n\n{body}'

  # Send the email to multiple recipients
  server.sendmail(username, recipients, msg)

  # Disconnect from the server
  server.quit()

#Main function so that the script can be called
if __name__ == '__main__':
    query_api(args.taxon_id)
    if args.email == 'On':
        for_email_body = calculate()
        send_email('#SOME EMAIL#', '#APP KEY#', f'Data Flow Run Stats for tax_id{args.taxon_id}', f'{for_email_body}', ['##RECIPIENT ADDRESS'])
    else:
        for_printing = calculate()
        print(for_printing)