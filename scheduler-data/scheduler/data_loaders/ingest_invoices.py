import io
import pandas as pd
from datetime import datetime, timezone, timedelta
import time
import json
import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def retrieve_invoices(logger, base_url, access_token, realmId, start_window, end_window, chunk_days=7, page_size=100):

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json',
        'Content-type': 'application/json',
    }

    full_uri = f'{base_url}/v3/company/{realmId}/query'

    raw_data = []
    data = []

    page_number = 1
    starting_pos = 1 # Starting row number
    max_tries = 5 # Limit number of tries

    total_count = 0

    current_date = datetime.strptime(start_window, '%Y-%m-%d')
    final_date = datetime.strptime(end_window, '%Y-%m-%d')

    final_date_str = final_date.isoformat()

    logger.info(f'Extracting invoices in range [{start_window}, {end_window}]')
    
    while current_date <= final_date:
        current_delay = 1 # Incremented in powers of 2 in case of errors

        chunk_end = current_date + timedelta(days=chunk_days - 1)

        current_date_str = current_date.isoformat()
        
        if chunk_end > final_date:
            chunk_end = final_date

        chunk_end_str = chunk_end.isoformat()
        
        while True:
            query = f"SELECT * FROM Invoice WHERE TxnDate >= '{current_date_str[:10]}' AND TxnDate <= '{chunk_end_str[:10]}' STARTPOSITION {starting_pos} MAXRESULTS {page_size}"

            for attempt in range(max_tries):
                try:
                    response = requests.get(
                        full_uri,
                        headers=headers,
                        params={'query': query}
                    )
                    
                    print(response.status_code)
                    response.raise_for_status()

                    data = response.json().get('QueryResponse', {}).get('Invoice', [])

                    logger.info(f'Found {len(data)} for chunk [{current_date_str[:10]}, {chunk_end_str[:10]}]')

                    break

                except requests.exceptions.RequestException as e:
                    logger.warning(f'API Error in attempt {attempt + 1}:\n{e}. Wait for {current_delay}s.')

                    if attempt < max_tries - 1:
                        time.sleep(current_delay)
                        current_delay *= 2
                    else:
                        logger.error('Maximum number of tries reached. Stopping execution.')
                        return None

            logger.info(f'Finishing extraction window at: {chunk_end}')

            if data:
                for invoice in data:
                    total_count += 1
                    invoice_id = invoice.get('Id')

                    raw_data.append({
                        'id': invoice_id,
                        'payload': json.dumps(invoice),
                        'ingested_at_utc': final_date_str,
                        'extract_window_start_utc': current_date_str,
                        'extract_window_end_utc': final_date_str,
                        'page_number': page_number,
                        'page_size': page_size,
                        'requested_payload': query
                    })

            # Escape the inner loop if all data has been processed for the chunk
            if len(data) < page_size:
                break
            else:
                starting_pos += page_size
                page_number += 1

        current_date = chunk_end + timedelta(days=1)

    # Ingestion time is normalized since all rows are inserted into the DB at the same time
    ingestion_utc = datetime.now(timezone.utc).isoformat()

    for row in raw_data:
        row['ingested_at_utc'] = ingestion_utc

    logger.info(f'Total invoices extracted: {total_count}')

    return raw_data

@data_loader
def load_data(access_token, *args, **kwargs):
    """
    Loading data from QuickBooks API
    """
    logger = kwargs.get('logger')

    url = 'https://sandbox-quickbooks.api.intuit.com'

    realmId = get_secret_value('qb_realm_id')

    start_window = kwargs.get('start_date')
    end_window = kwargs.get('end_date')

    # If a range is not define, the December 2025 (last month) is loaded in
    if not start_window or not end_window:
        logger.warning('Extraction window not defined. Extracting from December 2025.')

        start_window = '2025-12-01'
        end_window = '2025-12-31'

    logger.info('Retriving invoices...')
    raw_data = retrieve_invoices(logger, url, access_token, realmId, start_window, end_window)

    df_invoices = pd.DataFrame(raw_data)

    return df_invoices


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
