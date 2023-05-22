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
import os
import xml.etree.cElementTree as ET

def import_mailbots(filename="mail_bots.xml"):
    path = "./conf"
    mailbots_filename = os.path.join(path, filename)
    if not os.path.isfile(mailbots_filename):
        print("!!! {} not found".format(mailbots_filename))
        return {}

    tree = ET.parse(mailbots_filename)
    root = tree.getroot()
    if root.tag != "bots":
        print("!!! tag {} encountered. expecting bots".format(root.tag))
        return {}

    mailbots = {}
    mailbots_nodes = list(root)
    for mailbot_node in mailbots_nodes:
        if mailbot_node.tag != "bot":
            continue

        mailbot = {}
        for name, value in mailbot_node.attrib.items():
            mailbot[name] = value
        if "id" in mailbot:
            mailbots[mailbot["id"]] = mailbot

    return mailbots

def get_mailbot_info(botId, filename="mail_bots.xml"):
    mailbots = import_mailbots(filename)
    if botId in mailbots:
        return mailbots[botId]
    return {}


def send_mail(receiver, subject, message, attachments=None):
    mailbot = get_mailbot_info("default")
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

        if attachments == None:
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
