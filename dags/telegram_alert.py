import requests
import logging
from airflow.models import Variable

logger = logging.getLogger(__name__)


def send_telegram(message: str):
    """Send a message to Telegram"""
    try:
        token = Variable.get("telegram_bot_token")
        chat_id = Variable.get("telegram_chat_id")
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        response = requests.post(url, data={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        })
        if response.status_code == 200:
            logger.info("Telegram alert sent ✅")
        else:
            logger.warning(f"Telegram alert failed: {response.text}")
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {e}")


def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'Unknown error')

    message = (
        f"🔴 <b>Airflow Task FAILED</b>\n\n"
        f"<b>DAG:</b> {dag_id}\n"
        f"<b>Task:</b> {task_id}\n"
        f"<b>Time:</b> {execution_date}\n"
        f"<b>Error:</b> {str(exception)[:200]}"
    )
    send_telegram(message)


def on_success_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']

    message = (
        f"✅ <b>Airflow Task SUCCESS</b>\n\n"
        f"<b>DAG:</b> {dag_id}\n"
        f"<b>Task:</b> {task_id}\n"
        f"<b>Time:</b> {execution_date}"
    )
    send_telegram(message)