from pymodbus.pdu import ModbusRequest
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.transaction import ModbusRtuFramer
import time
from datetime import datetime
import paho.mqtt.client as paho
from paho import mqtt
import json
import mqtt_config as config
import socket
import ssl
import os
import csv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

device_id = config.pumpName.split("_")[1]
email_sender = "3pcsimulator@gmail.com"
email_password = "khojhwxtqcnbvvej"
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


# def compose_email():
#     mailing_list = extract_email_data('MailingList.csv')
#     yag = yagmail.SMTP(email_sender, email_password)
#     yag.set_logging(log_level=1)
#     subject = device_id
#     body = "Please find the attached data."
#     attachments = f'{device_id}.csv'
#     yag.send(
#         to=mailing_list,
#         subject=subject,
#         contents=body,
#         attachments=attachments,
#     )

def compose_email():
    mailing_list = extract_email_data('/root/smartPumpG/MailingList.csv')
    subject = device_id
    body = "Please find the attached data."
    attachments = f'{device_id}.csv'

    msg = MIMEMultipart()
    msg['From'] = email_sender
    msg['To'] = ', '.join(mailing_list)
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        with open(attachments, "rb") as attachment_file:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment_file.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename= {attachments}", )
        msg.attach(part)
    except Exception as e:
        print(f"Unable to open attachment. Error: {str(e)}")

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(email_sender, email_password)
        text = msg.as_string()
        server.sendmail(email_sender, mailing_list, text)
        server.quit()
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email. Error: {str(e)}")


def writeHistory(content):
    with open(f'{device_id}.csv', 'w') as file:
        commacount = content[1].count(",")
        for line in content:
            if line==device_id:
                file.write(line + commacount*",")
                file.write('\n')
            else:
                file.write(line)
                file.write('\n')


mqtt_client = paho.Client(config.pumpName, clean_session=True)
topic = config.domain + 'rawdata/' + config.Customer + '/' + config.Plant + '/' + config.pumpName
broker = config.mqtt_broker
mqtt_topic = config.domain + 'edits/' + config.Customer + '/' + config.Plant + '/' + config.pumpName

k = []
with open("/root/smartPumpG/RegisterData.csv", "r") as csvfile:
    reader_variable = csv.reader(csvfile, delimiter=",")
    for row in reader_variable:
        print(row)
        k.append(row)

# regs = pd.read_csv('/root/smartPumpEdge/RegisterData.csv')  # 7970
# regs = pd.read_csv('RegisterData.csv') # windows
# holding = regs['Address'].tolist()
holding = [int(i[0]) for i in k[1:]]

last_message = None


### Status Checks and starts


def restartService():
    try:
        # stop apache2 service
        os.popen("sudo systemctl restart smartpump.service")
        print("smart-pump service restarted successfully...")

    except OSError as ose:
        print("Error while running the command", ose)


def writeReg(register, bit):
    try:
        conn = client.connect()
        if conn:
            print('Connected to pump')
            try:
                client.write_register(address=register - config.register_offset, value=bit, unit=1)
                print("Write Success")
            except Exception as e:
                print(e)
    except Exception as k:
        print(k)


def getRegData(holds, t=topic):
    mets = []
    for val in holds:
        out = client.read_holding_registers(address=val - config.register_offset, count=1,
                                            unit=1)
        mets.append({str(val): str(out.registers[0])})
    pingD = {
        'site': config.Plant,
        'pump': config.pumpName,
        'timestamp': str(datetime.now()),
        'metrics': mets
    }
    pingR = json.dumps(pingD)
    try:
        mqtt_client.publish(t, pingR, qos=1)
    except Exception as exep:
        print(f'There was an issue sending data because {r}.. Reconnecting')
        mqtt_client.connect(broker)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        print(f"Listening on topic: {mqtt_topic}")
    else:
        print(f"Failed to connect, return code {rc}", "Error\t")


def on_disconnect(client, userdata, rc):
    print(f"Unexpected disconnection due to {rc}")
    try:
        print("Reconnecting...")
        mqtt_client.reconnect()
    except socket.error:
        time.sleep(5)
        # mqtt_client.reconnect()
        restartService()


def on_message(client, userdata, msg):
    x = msg.payload
    command = json.loads(x)
    if command['change'] == 'change':
        # print(f"Recieved write command {command}")
        registers, bits = command['register'], command['bit']
        for i in range(len(registers)):
            writeReg(registers[i], bits[i])
    elif command['change'] == 'ping':
        getRegData(holding)
    elif command['change'] == 'req':
        getRegData(command['register'], t=f'{config.pumpName}/requested')
    elif command['change'] == 'restart':
        restartService()
    # writeReg(command['register'][0], command['bit'][0])
    # writeReg(command['register'][1], command['bit'][1])


# Connect To Client and Get Data
client = ModbusClient(method='rtu', port='/dev/ttymxc3', parity='N', baudrate=9600, stopbits=2, auto_open=True,
                      timeout=3)  # 7970
# client = ModbusClient(method='rtu', port='com8', parity='N', baudrate=9600, stopbits=2, auto_open=True)  # windows
try:
    conn = client.connect()
    # enable TLS
    mqtt_client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS, cert_reqs=ssl.CERT_NONE)
    mqtt_client.tls_insecure_set(True)
    # set username and password
    mqtt_client.username_pw_set(config.mqtt_username, config.mqtt_pass)
    # connect to HiveMQ Cloud on port 8883
    mqtt_client.connect(broker, 8883, keepalive=60)
    if conn:
        print('Connected to Pump!')
        content = content_headers.copy()
        while True:
            try:
                mqtt_client.loop_start()
                mqtt_client.on_connect = on_connect
                mqtt_client.on_message = on_message
                mqtt_client.on_disconnect = on_disconnect
                mqtt_client.subscribe(mqtt_topic, qos=1)
                mqtt_client.loop_stop()
            except Exception as r:
                print(f'There was an issue sending data because {r}.. Reconnecting')
            # read holding registers from device number 27 formulate data dictionary define data in SparkPlugB structure
            register_readout = ""
            metrics = []
            current = {}
            for reg in holding:
                read = client.read_holding_registers(address=reg - config.register_offset, count=1,
                                                     unit=1)
                if register_readout == "":
                    register_readout = register_readout + str(datetime.now().replace(microsecond=0).strftime("%d/%m/%Y %H:%M:%S")) + "," + str(
                        read.registers[0]) + ","
                else:
                    register_readout = register_readout + str(read.registers[0]) + ","
                metrics.append({str(reg): str(read.registers[0])})
                current.update({str(reg): str(read.registers[0])})
            content.append(register_readout)
            pub_data = {
                'site': config.Plant,
                'pump': config.pumpName,
                'timestamp': str(datetime.now()),
                'metrics': metrics
            }
            if last_message is None or current != last_message:
                message = json.dumps(pub_data)
                last_message = current
                try:
                    mqtt_client.publish(topic, message, qos=1)
                    print(f'{datetime.now()}: published {message} to {topic}')
                except Exception as r:
                    print(f'There was an issue sending data because {r}.. Reconnecting')
                    connection = mqtt_client.connect(broker)

#            print(f'{str(datetime.now().strftime("%d/%m/%y %H:%M:%S"))}: {len(content)}')
            if len(content) > 363:  # Headers + 30 mins data @ 10 seconds frequency.
                writeHistory(content)
                compose_email()
                content = content_headers.copy()
            else:
                print(f'{datetime.now().replace(microsecond=0).strftime("%d/%m/%Y %H:%M:%S")}:{len(content)}')
                time.sleep(10)
                continue
            # repeat
    else:
        print("Error Connecting to Pump")
except Exception as e:
    print(e)
