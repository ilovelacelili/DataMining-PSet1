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

def _fetch_qb_customers(base_url, access_token, realmId, page_size=100):

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-type': 'application/text'
    }

    full_uri = f'{base_url}/v3/company/{realmId}/query'

    customers = []

    batch_number = 1
    starting_pos = 1 # Starting row number
    max_tries = 5 # Limit number of tries
    current_delay = 1 # Incremented in powers of 2 in case of errors

    while True:
        query = f'SELECT * FROM Customers STARTPOSITION {starting_pos} MAXRESULTS page_size'

        for attempt in range(max_tries):
            try:
                response = requests.get(
                    f'{full_uri}',
                    headers=headers,
                    data=query
                )
                
                print(response.status_code)

                response.raise_for_status()

            except requests.exceptions.RequestException as e:
                print(f'API Error in attempt {attempt + 1}:\n{e}')

                if attempt < max_tries - 1:
                    time.sleep(current_delay)
                    current_delay *= 2
                else:
                    print('Maximum number of tries reached.')

        if not customers:
            break

        starting_pos += page_size
        batch_number += 1

    pass

@data_loader
def load_data(access_token, *args, **kwargs):
    """
    Loading data from QuickBooks API
    """
    url = 'https://sandbox-quickbooks.api.intuit.com'

    realmId = get_secret_value('qb_realm_id')

    response = _fetch_qb_customers(url, access_token, realmId)

    return pd.read_csv(io.StringIO(response.text), sep=',')


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
