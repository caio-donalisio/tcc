import os

from kombu import Queue

TJSP_USERNAME = os.getenv("TJSP_USERNAME", None)
TJSP_PASSWORD = os.getenv("TJSP_PASSWORD", None)

result_backend = os.getenv("CELERY_BACKEND_URL", "redis://127.0.0.1:6379/0")
broker_url = os.getenv("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")

redis_backend_health_check_interval = int(os.getenv("CELERY_REDIS_HEALTH_CHECK_INTERVAL", 30))

result_backend_transport_options = {}
broker_transport_options = {}

if master_name:
  = os.getenv("CELERY_REDIS_BROKER_MASTER_NAME"):
  result_backend_transport_options["master_name"] = master_name
  broker_transport_options["master_name"] = master_name

OUTPUT_URI = os.getenv("OUTPUT_URI", "/data")

task_reject_on_worker_lost = True
task_acks_late = True
worker_prefetch_multiplier = 1

result_backend_transport_options['visibility_timeout'] = 3600 * 2  # 2 hours
broker_transport_options['visibility_timeout'] = 3600 * 2  # 2 hours

task_default_queue = "crawler-queue"
task_ignore_result = True
task_store_errors_even_if_ignored = True

imports = ("app.crawlers",)

task_queues = (
    Queue("crawler-queue", routing_key="crawlers.#"),
    Queue("schedule-queue", routing_key="schedule.#"),
)

beat_schedule = {}

timezone = "UTC"

worker_send_task_events = True
