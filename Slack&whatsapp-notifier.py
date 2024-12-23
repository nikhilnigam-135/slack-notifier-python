# IMPORTING LIBRARIES

# To connnect my code with SQL database
import mysql.connector

# To send whatsapp message through Whatsapp application
import pywhatkit

# To extract current time
import time

# To send message through slack API
import slack

# To load environment variables
import os

# To extract data from json file
import json

# To extract path
from pathlib import Path

# To load environment variables
from dotenv import load_dotenv

# DEFINING FUNCTIONS

# Load environment variables
def load_environment():
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)

# Connect to the SQL
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            password='Nikhil#135',
            user='root'
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error: {err}")


# Extracting data from json file
def extracting_data_from_json():
    with open("data.json", "r") as json_file:
        loaded_data = json.load(json_file)
    return (loaded_data)

# If the type of data is persistant
def if_type_persistant(i,loaded_data):
    database_name = (loaded_data[i]['queries']['database_name'])
    table_name = loaded_data[i]['queries']['table_name']
    target_coloumn = loaded_data[i]['queries']['conditions']['target_coloumn']
    condition = loaded_data[i]['queries']['conditions']['condition']
    target_value = loaded_data[i]['queries']['conditions']['target_value']
    time_column = loaded_data[i]['queries']['time_column']
    return database_name, table_name, target_coloumn, condition, target_value, time_column

# If the type of data is threshold
def if_type_threshold(i, loaded_data):
    database_name = loaded_data[i]['queries']['database_name']
    table_name = loaded_data[i]['queries']['table_name']
    threshold = loaded_data[i]['queries']['threshold']
    time_column = loaded_data[i]['queries']['time_column']
    return database_name, table_name, threshold , time_column


# Opening database
def open_database(database_name, cursor):
    query = f"USE {database_name};"
    cursor.execute(query)

# If type is Persistant, Building of query
def Persistant_building_of_query(column_name, table_name, time_column):
    query = f"SELECT COUNT({column_name}) FROM {table_name} WHERE HOUR({time_column}) BETWEEN %s AND %s;;"
    return query

# If type is Threshold, Building of where statement 
def Threshold_WHERE_statement(where_column, where_operation, where_condition, time_column):
    WHERE = f"WHERE {where_column} {where_operation} '{where_condition}' AND HOUR({time_column}) BETWEEN %s AND %s"
    return WHERE


# Create query
def Threshold_building_of_query(column_name, table_name, WHERE):
    query = f"SELECT COUNT({column_name}) FROM {table_name} {WHERE} ;"
    return query

# Execute query and return No. of transactions in last hour
def execute_query(mycursor, query, t):
    mycursor.execute(query, (t, t))
    result = mycursor.fetchall()[0][0]
    return result

# Checking if failed transactions are more than threshold
def process_failed_transactions(result,y):
    if result >= y:
        print("ERROR CONFIRMED")
        send_failed_transaction_notifications()
    else:
        print("EVERYTHING LOOKS GOOD")

# Read data from another file which is to be sent as a notification
def message_extraction():
    f=open('SLACK Template','r')
    read=f.readlines()
    return read

# If transactions are more than threshold, send notifications
def send_failed_transaction_notifications():
    send_slack_message(message_extraction())
    send_whatsapp_message("MULTIPLE FAILED TRANSACTIONS ERROR!", time.localtime().tm_hour, time.localtime().tm_min)

# Slack client setup and sending message
def send_slack_message(message):
    client = slack.WebClient(token=os.environ.get('slack_t'))# slack_t is the token of the bot
    try:
        client.chat_postMessage(channel='C085M7392TB', text=''.join(message))
        print("SLACK MESSAGE IS SENT")

    except slack.errors.SlackApiError as e:
        print(f"Error sending Slack message: {e.response['error']}")

# Sending WhatsApp message
def send_whatsapp_message(message, th, tm):
    '''pywhatkit.sendwhatmsg("+91 9310830655", message, th, tm + 1, 15, True, 2)
    print("WhatsApp message is sent.")
    return'''

def main():
    while True:

        load_environment() #To load envirounment variables
        conn = get_db_connection() # Connect to the database
        mycursor = conn.cursor()
        data= extracting_data_from_json() # Extracting data from json file
        for i in range (0,len(data)):
            if (data[i]['type']=="persistant"):
                database_name, table_name, target_coloumn, condition, target_value, time_column = if_type_persistant(i, data)
                open_database(database_name, mycursor) # Opening of database
                query = Persistant_building_of_query(target_coloumn, table_name, time_column)
                t=time.localtime().tm_hour #To get current time
                result = execute_query(mycursor, query,t)
                print("NO. of transactions in last hour are:", result)
                
            elif (data[i]['type']=='threshold'):
                database_name, table_name, threshold, time_column = if_type_threshold(i, data)
                open_database(database_name, mycursor) # Opening of database   
                WHERE = Threshold_WHERE_statement(target_coloumn, condition, target_value, time_column)
                query = Threshold_building_of_query(target_coloumn, table_name, WHERE)
                t=time.localtime().tm_hour #To get current time
                result = execute_query(mycursor, query,t)
                process_failed_transactions(result, threshold)

        # Close the connection
        conn.close()

        # Wait for an hour before the next cycle
        time.sleep(3600)


# CALLING MAIN FUNCTION
if __name__ == "__main__":
    main()