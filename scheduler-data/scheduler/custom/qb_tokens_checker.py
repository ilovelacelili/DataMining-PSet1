from mage_ai.data_preparation.shared.secrets import get_secret_value
import requests

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def get_qb_tokens(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    
    auth_url = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'
    
    clientId = get_secret_value('qb_client_id')
    client_secret = get_secret_value('qb_client_secret')
    refresh_token = get_secret_value('qb_refresh_token')

    print('Retrieving access token from the QB API.')

    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
    }

    response = requests.post(
        auth_url,
        data=payload, 
        auth=(clientId, client_secret))

    if response.status_code == 200:
        print('Access token succesfully retrieved.')
        return response.json()['access_token']
    else:
        print('Could not retrieve access token. Please check the validity of refresh token.')
        return None

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
