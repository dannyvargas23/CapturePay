import json
import urllib3
import os

# Global Variables
access_token = os.environ.get('SHOPIFY_API_KEY')
shopify_api_version = os.environ.get('SHOPIFY_API_VERSION', '2020-10')
auth_net_url = os.environ.get('AUTH_NET_URL')
auth_net_name = os.environ.get('AUTH_NET_NAME')
auth_net_key = os.environ.get('AUTH_NET_KEY')
headers = {'Content-Type': 'application/json', 'X-Shopify-Access-Token': f'{access_token}'}

# Handle connection pooling and thread safety
http = urllib3.PoolManager()


def get_data(url, headers=None):
    r = http.request('GET', url, headers=headers)
    return json.loads(r.data)


def post_data(url, obj, headers=None):
    r = http.request('POST', url, headers=headers, body=json.dumps(obj))
    return json.loads(r.data.decode('utf-8-sig'))


def update_order_payment(order_amount, order_number, url, headers=None):
    payload = {
        'transaction': {
            'currency': 'USD',
            'amount': float(order_amount),
            'kind': 'capture',
            'gateway': 'manual'
        }
    }
    r = http.request('POST', url, headers=headers, body=json.dumps(payload))
    log_shopify_response(order_number, payload)
    return json.loads(r.data)


def log_authorize_net_response(order_number, result):
    log = {
        'source': 'authorize.net',
        'order_number': order_number,
        'messages_result_code': result['transactionResponse']['responseCode'],
        'transaction_response_result_code': result['messages']['resultCode'],
        'transaction_response_message_code': result['transactionResponse']['messages'][0]['code'] if 'messages' in result['transactionResponse'] else '',
        'transaction_response_message_text': result['transactionResponse']['messages'][0]['description'] if 'messages' in result['transactionResponse'] else '',
        'transaction_response_error_code': result['transactionResponse']['errors'][0]['errorCode'] if 'errors' in result['transactionResponse'] else '',
        'transaction_response_error_text': result['transactionResponse']['errors'][0]['errorText'] if 'errors' in result['transactionResponse'] else '',
        'transaction_id': result['transactionResponse']['transId'],
        'ref_transaction_id': result['transactionResponse']['refTransID']
    }
    print(json.dumps(log))


def log_shopify_response(order_number, result):
    log = {
        'source': 'shopify',
        'order_number': order_number,
        'transaction_id': result['transaction']['id'],
        'kind': result['transaction']['kind'],
        'gateway': result['transaction']['gateway'],
        'status': result['transaction']['status'],
        'message': result['transaction']['message'],
        'amount': result['transaction']['amount'],
        'currency': result['transaction']['currency'],
        'created_at': result['transaction']['created_at']
    }
    print(json.dumps(log))


def lambda_handler(event, context):
    shop_api_url = f"https://{event['shop_domain']}/admin/api/{shopify_api_version}/orders/{event['order_id']}"
    transaction_url = f'{shop_api_url}/transactions.json'
    order_url = f'{shop_api_url}.json'
    metafields_url = f'{shop_api_url}/metafields.json'

    # Get Order amount
    if event.get('action_source') == 'capture':
        result_temp = get_data(transaction_url, headers)
        transactions = [trxn for trxn in result_temp['transactions'] if trxn['kind'] == 'capture']
        order_amount = transactions[-1]['amount']
    else:
        result_temp = get_data(order_url, headers)
        order_amount = result_temp['order']['total_price']

    # Get Order financial status
    result = get_data(order_url, headers)
    financial_status = result['order']['financial_status']

    # Get Order meta data for Auth.net
    result = get_data(metafields_url, headers)
    metadata = [md for md in result['metafields'] if md['key'] == 'authorize.net_auth_id']

    if len(metadata) > 0:
        payload = {
            'createTransactionRequest': {
                'merchantAuthentication': {
                    'name': auth_net_name,
                    'transactionKey': auth_net_key
                },
                'transactionRequest': {
                    'transactionType': 'priorAuthCaptureTransaction',
                    'amount': float(order_amount),
                    'refTransId': metadata[0]['value'],
                    'order': {
                        'invoiceNumber': event['order_number']
                    }
                }
            }
        }
        result = post_data(auth_net_url, payload, headers)
        print('ran')
        log_authorize_net_response(event['order_number'], result)
        if result['messages']['resultCode'] == 'Ok' and result['transactionResponse']['responseCode'] == '1' and \
                result['transactionResponse']['messages'][0]['code'] == '1' and financial_status not in (
                'paid', 'partially_paid'):
            update_order_payment(order_amount, event['order_number'], transaction_url, headers)
