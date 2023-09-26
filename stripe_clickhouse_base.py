# Import packages
import json
from sqlalchemy import create_engine, Column, String, Boolean, DateTime
import stripe
import pandas as pd
from datetime import datetime, timedelta
from clickhouse_driver import Client as Clickhouse
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

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

# CREATE TABLES
# Define SQL statement to create the customers table
create_table_sql1 = """
CREATE TABLE stripe.customers (
    user_id String,
    created String,
    email String,
    updated_at DateTime
) ENGINE = MergeTree() ORDER BY created;
"""

create_table_sql2 = """
CREATE TABLE stripe.customers_h (
    user_id String,
    sub_id String,
    sub_status String,
    updated_at DateTime
) ENGINE = MergeTree() ORDER BY updated_at;
"""

create_table_sql3 = """
CREATE TABLE stripe.subscriptions (
    user_id String,
    sub_id String,
    created String,
    current_period_start String,
    current_period_end String,
    cancel_at String,
    cancel_at_period_end UInt8,
    trial_start String,
    status String,
    plan String,
    product String,
    updated_at DateTime
) ENGINE = MergeTree() ORDER BY created;
"""

create_table_sql4 = """
CREATE TABLE stripe.charges (
    charge_id String,
    user_id String,
    sub_id String,
    created String,
    amount_paid Float64,
    percent_off Float64,
    status String,
    updated_at DateTime
) ENGINE = MergeTree() ORDER BY created;
"""

# Define the list of create table queries
create_table_queries = [
    create_table_sql1,
    create_table_sql2,
    create_table_sql3,
    create_table_sql4
]

# Execute the ClickHouse queries to create the tables
for query in create_table_queries:
    try:
        clickhouse_client.execute(query)
    except Exception as e:
        print("Error:", e)

# GET DATA FROM STRIPE API

# TABLE 1: ALL CUSTOMERS (BASIC)
data_customers = []  # Initialize list to hold dictionaries of customer data
# Get all customers (static)
customers = stripe.Customer.list(limit=100, expand=['data.subscriptions'])
for customer in customers.auto_paging_iter():
    data_customers.append({
        'user_id': customer.id,
        'created': datetime.fromtimestamp(customer.created).strftime('%Y-%m-%d %H:%M:%S'),
        'email': customer.email
    })

# Convert the list to a Pandas DataFrame
df_customers = pd.DataFrame(data_customers)
df_customers.fillna('', inplace=True)

# Add a column with the current date and time
df_customers['updated_at'] = pd.to_datetime(datetime.now())


# TABLE 2: ALL CUSTOMERS (HISTORICAL)
data_customers_h = []  # Initialize list to hold dictionaries of customer data

# Get all customers
customers_h = stripe.Customer.list(limit=100, expand=['data.subscriptions'])
for customer in customers_h.auto_paging_iter():
    data_customers_h.append({
        'user_id': customer.id,
        'sub_id': customer.subscriptions.data[0].id if customer.subscriptions.data else None,
        'sub_status': customer.subscriptions.data[0].status if customer.subscriptions.data else None
    })

# Convert the list to a Pandas DataFrame
df_customers_h = pd.DataFrame(data_customers_h)
df_customers_h.fillna('', inplace=True)

# Add a column with the current date and time
df_customers_h['updated_at'] = pd.to_datetime(datetime.now())


# TABLE 3: ALL SUBSCRIPTIONS
data_subs = []
# Get all subscriptions
subs = stripe.Subscription.list(limit=100, status='all', expand=['data.latest_invoice'])

# Extract information about each subscription
for sub in subs.auto_paging_iter():
    data_subs.append({
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
    })

# Convert the list to a Pandas DataFrame
df_subs = pd.DataFrame(data_subs)
df_subs.fillna('', inplace=True)

# Add a column with the current date and time
df_subs['updated_at'] = pd.to_datetime(datetime.now())


# TABLE 4:ALL CHARGES
data_charges = []
# Get all charges
charges = stripe.Charge.list(limit=100, expand=['data.invoice'])

# Extract information about each charge
for charge in charges.auto_paging_iter():
    data_charges.append({
        'charge_id': charge.id,
        'user_id': charge.customer,
        'sub_id': charge.invoice.subscription if charge.invoice else None,
        'created': datetime.fromtimestamp(charge.created).strftime('%Y-%m-%d %H:%M:%S'),
        'amount_paid': charge.invoice.amount_paid if charge.invoice else None,
        'percent_off': charge.invoice.discount.coupon.percent_off if charge.invoice and charge.invoice.discount else None,
        'status': charge.status
    })

# Convert the list to a Pandas DataFrame
df_charges = pd.DataFrame(data_charges)
df_charges.fillna('', inplace=True)

# Change the data types of the columns
df_charges['percent_off'] = pd.to_numeric(df_charges['percent_off'], errors='coerce')
df_charges['amount_paid'] = pd.to_numeric(df_charges['amount_paid'], errors='coerce')

# Add a column with the current date and time
df_charges['updated_at'] = pd.to_datetime(datetime.now())


# INGEST DATA INTO THE CLICKHOUSE TABLE
# Clickhouse names are case sensitive, 
# so we need to make sure that the table names are the same 
# as the ones in the Clickhouse database.

clickhouse_client.execute("INSERT INTO customers (user_id, created, email, updated_at) VALUES", df_customers.to_dict('records'))
clickhouse_client.execute("INSERT INTO customers_h (user_id, sub_id, sub_status, updated_at) VALUES", df_customers_h.to_dict('records'))
clickhouse_client.execute("INSERT INTO subscriptions (user_id, sub_id, created, current_period_start, current_period_end, cancel_at, cancel_at_period_end, trial_start, status, plan, product, updated_at) VALUES", df_subs.to_dict('records'), types_check=True)
clickhouse_client.execute("INSERT INTO charges (charge_id, user_id, sub_id, created, amount_paid, percent_off, status, updated_at) VALUES", df_charges.to_dict('records'))
