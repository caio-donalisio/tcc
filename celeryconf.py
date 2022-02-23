task_acks_late = True
worker_prefetch_multiplier = 1
result_backend_transport_options = {'visibility_timeout': 3600 * 2}  # 2 hours
broker_transport_options = {'visibility_timeout': 3600 * 2}
# 
worker_send_task_events = True
task_send_sent_event = True
