SUCCESS_ITEM = dict(
    service_request_id=['id'],
    service_type='',
    service_name=['detail', 'jobName'],
    event_type='',
    timestamp=['time'],
)

FAILURE_ITEM = dict(
    service_request_id=['id'],
    service_type='',
    service_name=['detail', 'jobName'],
    event_type='',
    timestamp=['time'],
    request_payload={},
)
