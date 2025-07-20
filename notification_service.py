import smtplib
from email.mime.text import MIMEText
from twilio.rest import Client

class NotificationService:
    def __init__(self, config):
        self.email_config = config.get("email")
        self.sms_config = config.get("sms")
        self.twilio_client = Client(
            self.sms_config["account_sid"],
            self.sms_config["auth_token"]
        )
    
    def send_email(self, to: str, subject: str, body: str):
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = self.email_config["from_address"]
        msg['To'] = to
        
        with smtplib.SMTP(self.email_config["smtp_server"], self.email_config["smtp_port"]) as server:
            server.login(self.email_config["username"], self.email_config["password"])
            server.send_message(msg)
    
    def send_sms(self, to: str, message: str):
        self.twilio_client.messages.create(
            body=message,
            from_=self.sms_config["from_number"],
            to=to
        )