"""
This file will be run by GCloud functions every hour to update the data.
Make sure it runs smoothly and reports any errors clearly.
"""


from confirmed_cases import script
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

RECIPIENTS = ['dynosa.ead@icloud.com']
def send_mail(recip, subject, content):
    ##TODO make this less ghetto and maybe slightly secure
    sender_address = 'throwawaydev2020@gmail.com'
    sender_pass = 'devtesting123'
    #Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = recip
    message['Subject'] = subject
    #The body and the attachments for the mail
    message.attach(MIMEText(content, 'plain'))
    #Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
    session.starttls() #enable security
    session.login(sender_address, sender_pass) #login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, recip, text)
    session.quit()
    print('Mail Sent')

def notify_error(errnum):
    msg = ""
    if errnum == 1:
        msg = "FAILED GETTING SPREADSHEETDATA"
    elif errnum == 2:
        msg = "FAILED GEOLOCATING"
    elif errnum == 3:
        msg = "FAILED JSON OUTPUT"
    for recip in RECIPIENTS:
        send_mail(recip, 'FLATTEN ERROR', msg)

def main():
    print("Hello world")
    try:
        script.get_spreadsheet_data()
    except:
        notify_error(1)
    for i in range(5):
        try:
            script.geocode_sheet()
            break
        except:
            if i < 4:
                continue
            else:
                notify_error(2)
    try:
        script.output_json()
    except:
        notify_error(3)

if __name__ == '__main__':
    main()