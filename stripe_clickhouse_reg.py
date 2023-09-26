# Import packages
import json
from sqlalchemy import create_engine, Column, String, Boolean, DateTime
import stripe
import pandas as pd
from datetime import datetime, timezone, timedelta
from clickhouse_driver import Client as Clickhouse
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from airflow import settings
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import settings
from airflow.models import Connection
from airflow.utils.session import create_session

# Set up API
load_dotenv()
stripe.api_key = os.getenv("STRIPE_API_KEY")

# Set up ClickHouse database connection
clickhouse_host = 'localhost'
clickhouse_port = 9000
clickhouse_database = 'stripe'
clickhouse_user = 'default'
clickhouse_password = ''
clickhouse_client = Clickhouse(host=clickhouse_host, port=clickhouse_port,
                               database=clickhouse_database, user=clickhouse_user,
                               password=clickhouse_password, settings={'http': True})

# Use the create_engine function from SQLAlchemy to create a connection string
clickhouse_connection_str = f'clickhouse://{clickhouse_user}:{clickhouse_password}@{clickhouse_host}:{clickhouse_port}/{clickhouse_database}'
engine = create_engine(clickhouse_connection_str)


# GET DATA FROM STRIPE API
# TABLE 1: ALL CUSTOMERS (BASIC) 
def ingest_users_data():
    query = "SELECT max(created) FROM customers;"
    result = clickhouse_client.execute(query)
    created = result[0][0] if result and result[0] and result[0][0] else None
    created = datetime.strptime(created, "%Y-%m-%d %H:%M:%S") if created else None
    # Convert to UTC
    created_utc = created.replace(tzinfo=timezone.utc) if created else None
    # Create 'timestamp' using the Unix
    timestamp = int(created_utc.timestamp())

    def get_users_after_timestamp(timestamp):
        # Retrieve accounts created after the given timestamp
        customers = stripe.Customer.list(created={"gt": timestamp})
        
        # Create a dataframe to store the user information
        customers_df = pd.DataFrame(columns=['user_id', 'created', 'email', 'updated_at'])
        
        # Iterate over the accounts and extract relevant information
        for customer in customers.auto_paging_iter():
            user_id = customer.id
            created = pd.to_datetime(customer.created, unit='s').strftime('%Y-%m-%d %H:%M:%S')
            email = customer.email
            # Append the user information to the dataframe
            customers_df = customers_df.append({'user_id': user_id, 'created': created, 'email': email}, ignore_index=True)
        return customers_df
    
    # Get new data from Stripe
    customers_df = get_users_after_timestamp(timestamp)
    # Check if the DataFrame is not empty
    if not customers_df.empty:
        customers_df.fillna('', inplace=True)
        customers_df['updated_at'] = pd.to_datetime(datetime.now())
        # Insert data into clickhouse
        clickhouse_client.execute("INSERT INTO customers (user_id, created, email, updated_at) VALUES", customers_df.to_dict('records'))
    else:
        print("Customers DataFrame is empty, no data to download.")


# TABLE 2: CUSTOMERS WITH SUB STATUS (HISTORICAL)
def ingest_users_h_data():
    # TABLE 2: CUSTOMERS WITH SUB STATUS (HISTORICAL)
    # Initialize an empty DataFrame
    customers_h_df = pd.DataFrame(columns=['user_id', 'sub_id', 'sub_status', 'updated_at']) 
    # Get all customers (dynamic)
    customers_h = stripe.Customer.list(limit=100, expand=['data.subscriptions'])
    
    # Extract information about each customer
    for customer in customers_h.auto_paging_iter():
        # Append data directly to the DataFrame
        customers_h_df = customers_h_df.append({
            'user_id': customer.id,
            'sub_id': customer.subscriptions.data[0].id if customer.subscriptions.data else None,
            'sub_status': customer.subscriptions.data[0].status if customer.subscriptions.data else None
        }, ignore_index=True)
     
    # Check if the DataFrame is not empty
    if not customers_h_df.empty:
        customers_h_df.fillna('', inplace=True)
        customers_h_df['updated_at'] = pd.to_datetime(datetime.now())
        
        # Insert data into clickhouse
        clickhouse_client.execute("INSERT INTO customers_h (user_id, sub_id, sub_status, updated_at) VALUES", customers_h_df.to_dict('records'))
    else:
        print("Customers_h DataFrame is empty, no data to download.")


# TABLE 3: ALL SUBSCRIPTIONS
def ingest_subs_data():
    # Truncate the subscriptions table
    clickhouse_client.execute("TRUNCATE TABLE subscriptions")
    
    # Initialize an empty DataFrame
    subs_df = pd.DataFrame(columns=['user_id', 'sub_id', 'created', 'current_period_start',
                                   'current_period_end', 'cancel_at', 'cancel_at_period_end',
                                   'trial_start', 'status', 'plan', 'product', 'updated_at'])
    
    # Get all subscriptions 
    subs = stripe.Subscription.list(limit=100, status='all', expand=['data.latest_invoice'])
    
    # Extract information about each subscription
    for sub in subs.auto_paging_iter():
        # Append data directly to the DataFrame
        subs_df = subs_df.append({
            'user_id': sub.customer,
            'sub_id': sub.id,
            'created': datetime.fromtimestamp(sub.created).strftime('%Y-%m-%d %H:%M:%S'),
            'current_period_start': datetime.fromtimestamp(sub.current_period_start).strftime('%Y-%m-%d %H:%M:%S'),
            'current_period_end': datetime.fromtimestamp(sub.current_period_end).strftime('%Y-%m-%d %H:%M:%S'),
            'cancel_at': datetime.fromtimestamp(sub.cancel_at).strftime('%Y-%m-%d %H:%M:%S') if sub.cancel_at else None,
            'cancel_at_period_end': sub.cancel_at_period_end,
            'trial_start': datetime.fromtimestamp(sub.trial_start).strftime('%Y-%m-%d %H:%M:%S') if sub.trial_start else None,
            'status': sub.status,
            'plan': sub.plan.nickname if sub.plan else None,
            'product': sub.plan.product if sub.plan else None
        }, ignore_index=True)
    
    # Check if the DataFrame is not empty
    if not subs_df.empty:
        subs_df.fillna('', inplace=True)
        subs_df['updated_at'] = pd.to_datetime(datetime.now())
        # Insert data into clickhouse
        clickhouse_client.execute("INSERT INTO subscriptions (user_id, sub_id, created, current_period_start, current_period_end, cancel_at, cancel_at_period_end, trial_start, status, plan, product, updated_at) VALUES", subs_df.to_dict('records'))
    else:
        print("Subs DataFrame is empty, no data to download.")


# TABLE 4: ALL CHARGES
def ingest_charges_data():
    query = "SELECT max(created) FROM charges;"
    result = clickhouse_client.execute(query)
    created = result[0][0] if result and result[0] and result[0][0] else None
    created = datetime.strptime(created, "%Y-%m-%d %H:%M:%S") if created else None
    # Convert to UTC
    created_utc = created.replace(tzinfo=timezone.utc) if created else None
    # Create 'timestamp' using the Unix
    timestamp = int(created_utc.timestamp())

    def get_users_after_timestamp(timestamp):
        charges = stripe.Charge.list(created={"gt": timestamp}, expand=['data.invoice'])
        charges_df = pd.DataFrame(columns=['charge_id', 'user_id', 'sub_id', 'created', 'amount_paid', 
                                       'percent_off', 'status', 'updated_at'])
        # Extract information about each charge
        for charge in charges.auto_paging_iter():
            charge_id = charge.id
            user_id = charge.customer
            sub_id = charge.invoice.subscription if charge.invoice else None
            created = datetime.fromtimestamp(charge.created).strftime('%Y-%m-%d %H:%M:%S')
            amount_paid = charge.invoice.amount_paid if charge.invoice else None
            percent_off = charge.invoice.discount.coupon.percent_off  if charge.invoice and charge.invoice.discount and charge.invoice.discount.coupon else None
            status = charge.status
            # Append the user information to the dataframe
            charges_df = charges_df.append({'charge_id': charge_id, 'user_id': user_id, 'sub_id': sub_id, 'created': created, 'amount_paid': amount_paid, 
                                            'percent_off': percent_off, 'status': status}, ignore_index=True)
        return charges_df
    
    charges_df = get_users_after_timestamp(timestamp)

    if not charges_df.empty:
        charges_df.fillna('', inplace=True)
        charges_df['updated_at'] = pd.to_datetime(datetime.now())
        # Change the data types of the columns
        charges_df['percent_off'] = pd.to_numeric(charges_df['percent_off'], errors='coerce')
        charges_df['amount_paid'] = pd.to_numeric(charges_df['amount_paid'], errors='coerce')
        # Insert data into clickhouse
        clickhouse_client.execute("INSERT INTO charges (charge_id, user_id, sub_id, created, amount_paid, percent_off, status, updated_at) VALUES", charges_df.to_dict('records'))
    else:
        print(" Charges DataFrame is empty, no data to download.")


# AIRFLOW DATA ORCHESTRATION
# Define the DAG (Directed Acyclic Graph) arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG (Directed Acyclic Graph) and its schedule
with DAG('stripe_clickhouse_reg', start_date=datetime(2023, 9, 1),  # Specify the start date of the DAG
     schedule='@daily',  # Specify the schedule interval (daily)
     default_args=default_args) as dag:

# Define the task that executes the Stripe data extraction and loading function
     ingest_users_reg = PythonOperator(
          task_id='ingest_users_data',
          python_callable=ingest_users_data,
          dag=dag
)
     ingest_users_h_reg = PythonOperator(
          task_id='ingest_users_h_data',
          python_callable=ingest_users_h_data,
          dag=dag
)
     ingest_subscriptions_reg = PythonOperator(
          task_id='ingest_subs_data',
          python_callable=ingest_subs_data,
          dag=dag
)
     ingest_charges_reg = PythonOperator(
          task_id='ingest_charges_data',
          python_callable=ingest_charges_data,
          dag=dag
)
# Add the task to the DAG (Directed Acyclic Graph)
ingest_users_reg
ingest_users_h_reg
ingest_subscriptions_reg
ingest_charges_reg