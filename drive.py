import os
import datetime
import logging
import pandas as pd
import pymysql
import sshtunnel
from openpyxl import Workbook
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import requests
import json
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def open_ssh_tunnel(verbose=False):
    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

    tunnel = sshtunnel.SSHTunnelForwarder(
        ssh_address=(os.getenv("SSH_HOST"), int(os.getenv("SSH_PORT"))),
        ssh_username=os.getenv("SSH_USERNAME"),
        ssh_password=os.getenv("SSH_PASSWORD"),
        remote_bind_address=(os.getenv("MYSQL_HOST"), int(os.getenv("MYSQL_PORT")))
    )

    tunnel.start()
    return tunnel

def mysql_connect(tunnel):
    connection = pymysql.connect(
        host='127.0.0.1',  # Localhost because we're connecting through the SSH tunnel
        user=os.getenv("MYSQL_USER"),
        passwd=os.getenv("MYSQL_PASSWORD"),
        db=os.getenv("MYSQL_DB"),
        port=tunnel.local_bind_port
    )
    return connection

def run_query(sql, connection):
    return pd.read_sql_query(sql, connection)

def mysql_disconnect(connection):
    connection.close()

def close_ssh_tunnel(tunnel):
    tunnel.close()

def send_message_to_google_chat(link):
    message = {
        "text": f"Dữ liệu trong bảng đã được upload lên Google Drive. [Xem file]({link})"
    }
    requests.post(os.getenv("GOOGLE_CHAT_WEBHOOK"), json=message)

def read_credentials_from_file():
    credentials_path = os.getenv("CREDENTIALS_JSON_PATH")
    with open(credentials_path, 'r') as credentials_file:
        credentials = json.load(credentials_file)
    return credentials

def export_to_excel_and_drive(dataframe):
    if dataframe.empty:
        # Send a notification if there's no data
        message = {"text": "Không có dữ liệu"}
        requests.post(os.getenv("GOOGLE_CHAT_WEBHOOK"), json=message)
        return  # End function if no data

    current_time = datetime.datetime.now()
    file_name = f"data_{current_time.strftime('%d-%m-%Y')}.xlsx"
    excel_file_path = os.path.abspath(file_name)

    workbook = Workbook()
    sheet = workbook.active

    # Write headers to the first row
    headers = list(dataframe.columns)
    sheet.append(headers)

    # Write data rows
    for row in dataframe.itertuples(index=False, name=None):
        sheet.append(row)

    # Adjust column widths based on maximum length of each column's content
    for column in sheet.columns:
        max_length = 0
        column_letter = column[0].column_letter  # Get the column letter
        for cell in column:
            try:
                max_length = max(max_length, len(str(cell.value)))
            except:
                pass
        adjusted_width = max_length + 2  # Add some padding
        sheet.column_dimensions[column_letter].width = adjusted_width

    # Optionally, adjust row heights (if needed)
    for row in sheet.iter_rows():
        max_height = max(len(str(cell.value)) for cell in row if cell.value) // 20  # Adjust based on average char width
        for cell in row:
            sheet.row_dimensions[cell.row].height = max(15, max_height)

    workbook.save(excel_file_path)

    # Load Google Drive API credentials
    credentials = read_credentials_from_file()

    gauth = GoogleAuth()
    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        credentials, ['https://www.googleapis.com/auth/drive']
    )

    folder_id = os.getenv("DRIVE_FOLDER_ID")
    drive = GoogleDrive(gauth)

    # Delete existing file with the same name in the folder
    file_list = drive.ListFile({'q': f"title='{file_name}' and '{folder_id}' in parents and trashed=false"}).GetList()
    if file_list:
        existing_file = file_list[0]
        existing_file.Delete()

    # Upload new file to Google Drive
    gdrive_file = drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}]})
    gdrive_file.SetContentFile(excel_file_path)
    gdrive_file.Upload()

    uploaded_file_link = gdrive_file['alternateLink']
    print(f"File uploaded to Google Drive: {uploaded_file_link}")

    # Send Google Drive link to Google Chat
    send_message_to_google_chat(uploaded_file_link)

if __name__ == "__main__":
    # Open SSH tunnel and connect to MySQL
    tunnel = open_ssh_tunnel()
    connection = mysql_connect(tunnel)

    try:
        # Run the query and get the data
        query = os.getenv("MYSQL_QUERY")
        df = run_query(query, connection)
        print(df.head())  # Print the first few rows for verification

        # Export data to Excel and upload to Google Drive
        export_to_excel_and_drive(df)

    finally:
        # Close database connection and SSH tunnel
        mysql_disconnect(connection)
        close_ssh_tunnel(tunnel)
