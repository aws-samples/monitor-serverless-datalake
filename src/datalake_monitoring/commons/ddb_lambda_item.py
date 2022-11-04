SUCCESS_ITEM = dict(
    service_request_id=['requestContext', 'requestId'],
    service_type='',
    service_name=['requestContext', 'functionArn'],
    event_type='',
    timestamp=['timestamp'],
)

FAILURE_ITEM = dict(
    service_request_id=['requestContext', 'requestId'],
    service_type='',
    service_name=['requestContext', 'functionArn'],
    event_type='',
    timestamp=['timestamp'],
    retry_attempts=['requestContext', 'approximateInvokeCount'],
)
