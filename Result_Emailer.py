# This file emails the results of the deck interrogation

#!/usr/bin/python3
#This is the script that has socket issues/errors

import smtplib, ssl

smtp_server = 'stmp.gmail.com'
port = 465

sender = 'emailtester2500@gmail.com'
password = input('Maxarcodingproject')

receiver = 'cain.acosta@maxar.com'
message = """\

Python Test 

"""


context = ssl.create_default_context()

with smtplib.SMTP_SSL(smtp_server, port, context = context) as server:
    server.login(sender,password)
    
    #Send Email
    server.sendmail(sender, receiver, message)


######################################################################################


# This is the start of the second script, but it has the invalid login errors even though they are correct

import smtplib
from email.mime.text import MIMEText

subject = "Email Subject"
body = "This is the body of the text message"
sender = "emailtester2500@gmail.com"
recipients = ["cain.acosta@maxar.com"]
password = "Maxarcodingproject"


def send_email(subject, body, sender, recipients, password):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
       smtp_server.login(sender, password)
       smtp_server.sendmail(sender, recipients, msg.as_string())
    print("Message sent!")


send_email(subject, body, sender, recipients, password)
