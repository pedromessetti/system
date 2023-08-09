import smtplib
from email.mime.text import MIMEText


def send_alert(error):
    # Define sender's email address, password, recipient's email address, subject, and message
    sender_email = 'pedromessetti@gmail.com'
    sender_password = 'gcbh usqg wljq sqzq'
    recipient_email = 'pedromessetti@gmail.com'
    subject = 'ALERT'
    message = error

    # Create an instance of the MIMEText class and set the message and content type
    msg = MIMEText(message)
    msg['Content-Type'] = 'text/plain'

    # Set the sender, recipient, and subject of the email message
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject

    # Create an SMTP server instance and login to the sender's email account
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender_email, sender_password)

    # Send the email message using the sendmail() method of the SMTP server instance
    server.sendmail(sender_email, recipient_email, msg.as_string())

    # Close the SMTP server connection
    server.quit()
    print("ALERT SENDED")
