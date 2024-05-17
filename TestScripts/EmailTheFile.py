import time
import json
import yagmail
import csv
import mqtt_config as config

device_id = config.pumpName.split("_")[1]
email_sender = "3pcsimulator@gmail.com"
email_password = "khojhwxtqcnbvvej"

# Define a function to read a CSV file and extract data from the 'Email' column
def extract_email_data(csv_file_path):
    emails = []
    with open(csv_file_path, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            emails.append(row['Email'])
    return emails
	

def compose_email():
    mailing_list = extract_email_data('MailingList.csv')
    yag = yagmail.SMTP(email_sender, email_password)
    yag.set_logging(log_level=1)
    subject = device_id
    body = "Please find the attached data."
    attachments = f'{device_id}.csv'
    yag.send(
        to=mailing_list,
        subject=subject,
        contents=body,
        attachments=attachments,
    )
    # print(content)
	
while True:
	compose_email()
	time.wait(3600)

