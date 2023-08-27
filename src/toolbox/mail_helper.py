# references :
# https://docs.python.org/3/library/email.examples.html
# https://realpython.com/python-send-email/
# https://www.quennec.fr/trucs-astuces/langages/python/python-envoyer-un-mail-tout-simplement
import smtplib, ssl
from email.message import EmailMessage
import mimetypes  # For guessing MIME type based on file name extension
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from . import settings_helper

def send_mail(receiver, subject, message, attachments=None):
    mailbot = settings_helper.get_mailbot_info("default")
    smtp_server = mailbot.get("smtpserver", None)
    port = mailbot.get("port", 587)
    if isinstance(port, str):
        port = int(port)
    sender_email = mailbot.get("sender", None)
    receiver_email = receiver
    password = mailbot.get("password", None)
    if not smtp_server or not sender_email or not password:
        return 1

    # Create a secure SSL context
    context = ssl.create_default_context()

    # Try to log in to server and send email
    server = smtplib.SMTP(smtp_server, port)
    try:
        server.ehlo()  # Can be omitted
        server.starttls(context=context)  # Secure the connection
        server.ehlo()  # Can be omitted
        server.login(sender_email, password)

        if not attachments or (isinstance(attachments, list) and len(attachments) == 0):
            message = "From: {}\nTo: {}\nSubject: {}\n{}".format(sender_email, receiver_email, subject, message)
            server.sendmail(sender_email, receiver_email, message)
        else:
            msg = EmailMessage()
            msg['Subject'] = subject
            msg['From'] = sender_email
            msg['To'] = receiver_email
            for filepath in attachments:
                ctype, encoding = mimetypes.guess_type(filepath)
                if ctype is None or encoding is not None:
                    ctype = 'application/octet-stream'
                maintype, subtype = ctype.split('/', 1)
                with open(filepath, 'rb') as fp:
                    msg.add_attachment(fp.read(),
                                       maintype=maintype,
                                       subtype=subtype,
                                       filename=filepath)

            msg.attach(MIMEText(message, "plain"))
            server.send_message(msg)

    except Exception as e:
        # Print any error messages to stdout
        print(e)
    finally:
        server.quit()

    return 0
