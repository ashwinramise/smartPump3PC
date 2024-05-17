import time
from datetime import datetime
import paho.mqtt.client as paho
from paho import mqtt
import json
import mqtt_config as config
import socket
import csv
import random
import yagmail

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

content_headers = [device_id, "Date,PumpPowerON,ActualSetpointManualHI,ActualSetpointManualLO,ActualPulseVolumeHI,"
                                  "ActualPulseVolumeLO,ActualBatchDosingVolumeHI,ActualBatchDosingVolumeLO,"
                                  "ActualBatchDosingTimeHI,ActualBatchDosingTimeLO,ActualPressureMax,"
                                  "ControlSourceStates,FaultCode,WarningCode,WarningBits,DosingPressureMax,"
                                  "DosingCapacityMaxHI,DosingCapacityMaxLO,DosingCapacityReferenceHI,"
                                  "DosingCapacityReferenceLO,MeasuredDosingCapacityHI,MeasuredDosingCapacityLO,"
                                  "MeasuredPressure,PulseInputFrequency,RemainingDosingVolumeHI,RemainingDosingVolumeLO,"
                                  "VolumeTotalHI,VolumeTotalLO,VolumeTripCounterHI,VolumeTripCounterLO,NumberOfPowerOns,"
                                  "RunTimeHI,RunTimeLO,OperatingHoursHI,OperatingHoursLO,StrokeCounterHI,StrokeCounterLO,"
                                  "TimeToNextDosingHI,TimeToNextDosingLO",
			"DD/MM/YYYY hh:mm:ss,,0.1 ml/h,,1 nl,,0.001 ml,,"
            "0.1 s,,0.1 bar,Bits,Enum,Enum,Bits,Bar,0.1 ml/h,,"
            "0.1 ml/h,,0.1 ml/h,,0.1 bar,1 Hz,0.001 ml,,0.001 l,,0.001 l,,-,1,,1 s,,-,,1 s,"]  # list to store the data

# Define a function to read a CSV file and extract data from the 'Email' column
def extract_email_data(csv_file_path):
    emails = []
    with open(csv_file_path, mode='r', newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            emails.append(row['Email'])
    return emails
	
	
def writeHistory(content):
    with open(f'{device_id}.csv', 'w') as file:
        for line in content:
            file.write(line)
            file.write('\n')
			

k = []
with open("RegisterData.csv", "r") as csvfile:
    reader_variable = csv.reader(csvfile, delimiter=",")
    for row in reader_variable:
        print(row)
        k.append(row)
        
# regs = pd.read_csv('/root/smartPumpEdge/RegisterData.csv')  # 7970
# regs = pd.read_csv('RegisterData.csv') # windows
# holding = regs['Address'].tolist()
holding = [int(i[0]) for i in k[1:]]

last_message = None

print('Connected to Pump!')
content = content_headers.copy()
while True:
    # read holding registers from device number 27 formulate data dictionary define data in SparkPlugB structure
        register_readout = ""
        for reg in holding:
            read = random.randint(0, 10)
            if register_readout == "":
                register_readout = register_readout + str(datetime.now().strftime("%d/%m/%y %H:%M:%S")) + str(read) + ","
                # print(register_readout)
            else:
                register_readout = register_readout + str(read) + ","
                # print(register_readout)
        content.append(register_readout)
        print(len(content))
        if len(content) > 9:
            writeHistory(content)
            compose_email()
        else:
            continue
        time.sleep(10)  # repeat