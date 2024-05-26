from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from smtplib import SMTP
import datetime

def print_hello():
    return 'Hello world!'

default_args = {
        'owner': 'abe',
        'start_date':datetime(2024,5,26)
}

def send_smtp_mail():
    smtp = SMTP()
    smtp.set_debuglevel(10)
    smtp.connect('smtp.sparkpostmail.com', 587)
    smtp.login('SMTP_Injection', '425ff0c1a2b58a735ae43e821e0a2f36eaa6a50f')

    from_addr = "AlertPlane <info@alertplane.io>"
    to_addr = "abe24seven@gmail.com"

    subj = "hello"
    date = datetime.datetime.now().strftime( "%d/%m/%Y %H:%M" )

    message_text = "Hello\nThis is a mail from your server\n\nBye\n"

    msg = "From: %s\nTo: %s\nSubject: %s\nDate: %s\n\n%s" % ( from_addr, to_addr, subj, date, message_text )

    smtp.sendmail(from_addr, to_addr, msg)
    smtp.quit()
    return "Email sent!'


dag = DAG('send_email_test', description='SMTP Function DAG',
          schedule_interval='* * * * *',
          default_args = default_args, catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

email = PythonOperator(task_id='email_task', python_callable=send_smtp_mail, dag=dag)

email >> dummy_operator >> hello_operator
