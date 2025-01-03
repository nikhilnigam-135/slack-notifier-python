import mysql.connector
import pywhatkit
import time
import slack
import os
import json
from pathlib import Path
from dotenv import load_dotenv

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

client = slack.WebClient(token=os.environ.get('slack_t'))  # slack_t is the token of the bot

def load_environment():
    """Load the environment variables from the .env file."""
    load_dotenv(dotenv_path=env_path)

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
        return None

def get_failed_transactions(cursor, t):
    query = """
        SELECT br.id, br.request_time, bt.status 
        FROM bill_request AS br 
        JOIN bbps_transactions AS bt 
        ON br.id = bt.request_id 
        WHERE bt.status = 'failed' 
        AND HOUR(request_time) BETWEEN %s AND %s
        ORDER BY br.id;
    """
    cursor.execute(query, (t, t))
    return cursor.fetchall()

def send_slack_message(message):
    try:
        result = client.chat_postMessage(channel='C085M7392TB', text=message)
        print("SLACK MESSAGE IS SENT")
    except slack.errors.SlackApiError as e:
        print(f"Error sending Slack message: {e.response['error']}")

def send_whatsapp_message(message, th, tm):
    """Send a WhatsApp message."""
    pywhatkit.sendwhatmsg("+91 9310830655", message, th, tm + 1, 15, True, 2)
    print("WhatsApp message is sent.")

def get_transaction_count(cursor):
    """Get the total count of transactions in the last hour."""
    query = "SELECT COUNT(*) FROM bbps_transactions;"
    cursor.execute(query)
    return cursor.fetchall()[0][0]

def process_failed_transactions(conn, t, y):
    count = 0
    mycursor = conn.cursor()
    mycursor.execute("USE service_provided;")
    result = get_failed_transactions(mycursor, t)

    for _ in result:
        count += 1

    if count >= y:
        send_slack_message("MULTIPLE FAILED TRANSACTIONS ERROR!")
        send_whatsapp_message("MULTIPLE FAILED TRANSACTIONS ERROR!", time.localtime().tm_hour, time.localtime().tm_min)
    
    else:
        transaction_count = get_transaction_count(mycursor)
        print(f"NO. of transactions in last hour are: {transaction_count}")

def main():
    while True:
        # Get user input for time and acceptable failed transaction threshold
        t = int(input("Time: "))  # User input for time (this is manual entry)
        y = int(input("NO. of failed requests which are non-acceptable: "))  # Threshold for failed transactions

        # Connect to the database
        conn = get_db_connection()

        process_failed_transactions(conn, t, y)

        conn.close()

        time.sleep(3600)

if __name__ == "__main__":
    load_environment()
    main()
