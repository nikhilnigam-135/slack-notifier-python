# IMPORTING LIBRARIES
import mysql.connector
import pywhatkit
import time
import slack
import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv

# DEFINING FUNCTIONS
def load_environment():
    try:
        env_path = Path('C:/Desktop/python/.env')
        load_dotenv(dotenv_path=env_path)
    except Exception as e: 
        logging.error(f"Error loading .env file: {e}")
        raise

def setup_logs():
    logging.getLogger('mysql.connector').setLevel(logging.WARNING)
    logging.basicConfig(
        filename=os.getenv('LOG_LOCATION'), 
        level=logging.INFO, 
        format="%(asctime)s - %(levelname)s - %(message)s",)

def get_db_connection():
    try:
        load_environment()
        host = os.getenv('MYSQL_HOST')
        user = os.getenv('MYSQL_USER')
        password = os.getenv('MYSQL_PASSWORD')
        if not host or not user or not password:
            raise ValueError("Missing MySQL connection details in .env file")
        conn = mysql.connector.connect(host=host, user=user, password=password)
        return conn
    except mysql.connector.Error as err:
        logging.error(f"MySQL error: {err}")
    except ValueError as e:
        logging.error(f"ValueError: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

def extracting_data_from_json():
    try:
        with open(os.getenv('JSON_LOCATION'), "r") as json_file:
            loaded_data = json.load(json_file)
        return loaded_data
    except FileNotFoundError:
        print("Error: The file 'data.json' was not found.")
        logging.error("Error: The file 'data.json' was not found.")
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON from the file.")
        logging.error("Error: Failed to decode JSON from the file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        logging.error(f"An unexpected error occurred: {e}")

def required_field_json(i, loaded_data):
    database_name = loaded_data[i]['queries']['database_name']
    table_name = loaded_data[i]['queries']['table_name']
    target_coloumn = loaded_data[i]['queries']['conditions']['target_coloumn']
    condition = loaded_data[i]['queries']['conditions']['condition']
    target_value = loaded_data[i]['queries']['conditions']['target_value']
    time_column = loaded_data[i]['queries']['time_column']
    return database_name, table_name, time_column, target_coloumn, condition, target_value

def open_database(database_name, cursor):
    query = f"USE {database_name};"
    cursor.execute(query)

def where_statement(where_column, where_operation, where_condition, time_column):
    where_clause = f"""WHERE {where_column} {where_operation} '{where_condition}' 
                   AND HOUR({time_column}) 
                   BETWEEN %s AND %s"""
    return where_clause

def building_of_query(column_name, table_name, where_clause):
    query = f"SELECT COUNT({column_name}) FROM {table_name} {where_clause};"
    return query

# Execute query and return No. of transactions in last hour
def execute_query(mycursor, query, t):
    mycursor.execute(query, (t, t))
    result = mycursor.fetchall()[0][0]
    return result

def if_threshold_reached(result, threshold):
    if result >= threshold:
        logging.error("ERROR CONFIRMED")
        send_notifications()
    else:
        logging.info("Threshold not reached")

# Read data from another file which is to be sent as a notification
def message_extraction():
    with open('SLACK Template', 'r') as f:
        read = f.readlines()
    return read

# If transactions are more than threshold, send notifications
def send_notifications():
    send_slack_message(message_extraction())
    send_whatsapp_message(message_extraction(), time.localtime().tm_hour, time.localtime().tm_min)

# Slack client setup and sending message
def send_slack_message(message):
    client = slack.WebClient(token=os.environ.get('slack_t'))  # slack_t is the token of the bot
    try:
        client.chat_postMessage(channel='C085M7392TB', text=''.join(message))
        logging.info("SLACK MESSAGE IS SENT")
    except slack.errors.SlackApiError as e:
        logging.error(f"Error sending Slack message: {e.response['error']}")

# Sending WhatsApp message
def send_whatsapp_message(message, th, tm):
    # number = os.getenv('MOBILE_NUMBER')
    # pywhatkit.sendwhatmsg(number, message, th, tm + 1, 15, True, 2)
    # logging.info("WhatsApp message is sent.")
    return

def main():
    load_environment()
    setup_logs()
    conn = get_db_connection()
    mycursor = conn.cursor()
    data = extracting_data_from_json()
    for i in range(len(data)):
        database_name,table_name,time_column,target_coloumn,condition,target_value=required_field_json(i, data)
        open_database(database_name, mycursor)
        where_clause = where_statement(target_coloumn, condition, target_value, time_column)
        query = building_of_query(target_coloumn, table_name, where_clause)
        t = time.localtime().tm_hour
        result = execute_query(mycursor, query, t)
        if data[i]['type']=='persistant' :
            logging.info(f"No. of transactions in last hour are: {result}")
        if data[i]['type']=='threshold':
            threshold = data[i]['queries']['threshold']
            if_threshold_reached(result, threshold)
    conn.close()

# CALLING MAIN FUNCTION
if __name__ == "__main__":
    main()
