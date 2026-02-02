import io
import pandas as pd
from datetime import datetime, timezone
import time
import json
import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def retrieve_customers(base_url, access_token, realmId, page_size=100):

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json',
        'Content-type': 'application/json',
    }

    full_uri = f'{base_url}/v3/company/{realmId}/query'

    raw_data = []
    data = []

    batch_number = 1
    starting_pos = 1 # Starting row number
    max_tries = 5 # Limit number of tries

    total_count = 0

    starting_utc_window = datetime.now(timezone.utc).isoformat()

    print('Starting extraction window at: ', starting_utc_window)
    
    while True:
        current_delay = 1 # Incremented in powers of 2 in case of errors

        query = f'SELECT * FROM Customer STARTPOSITION {starting_pos} MAXRESULTS {page_size}'

        for attempt in range(max_tries):
            try:
                response = requests.get(
                    full_uri,
                    headers=headers,
                    params={'query': query}
                )
                
                print(response.status_code)
                response.raise_for_status()

                data = response.json().get('QueryResponse', {}).get('Customer', [])

                break

            except requests.exceptions.RequestException as e:
                print(f'API Error in attempt {attempt + 1}:\n{e}. Wait for {current_delay} s.')

                if attempt < max_tries - 1:
                    time.sleep(current_delay)
                    current_delay *= 2
                else:
                    print('Maximum number of tries reached. Stopping execution.')
                    return None

        ending_utc_window = datetime.now(timezone.utc).isoformat()

        print('Finishing extraction window at: ', ending_utc_window)

        if data:
            for customer in data:
                total_count += 1
                customer_id = customer.get('Id')

                raw_data.append({
                    'id': customer_id,
                    'payload': json.dumps(customer),
                    'ingested_at_utc': ending_utc_window,
                    'extract_window_start_utc': starting_utc_window,
                    'extract_window_end_utc': ending_utc_window,
                    'page_number': batch_number,
                    'page_size': page_size,
                    'requested_payload': query
                })

        # Escape the loop if all data has been processed
        if len(data) < page_size:
            break

        starting_pos += page_size
        batch_number += 1

    # Ingestion time is normalized since all rows are inserted into the DB at the same time
    ingestion_utc = datetime.now(timezone.utc).isoformat()

    for row in raw_data:
        row['ingested_at_utc'] = ingestion_utc

    print('Total customers extracted: ', total_count)

    return raw_data

@data_loader
def load_data(access_token, *args, **kwargs):
    """
    Loading data from QuickBooks API
    """
    url = 'https://sandbox-quickbooks.api.intuit.com'

    realmId = get_secret_value('qb_realm_id')

    raw_data = retrieve_customers(url, access_token, realmId)

    df_customers = pd.DataFrame(raw_data)

    return df_customers


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
