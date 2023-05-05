import os
import logging
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

"""
Sends email notifications on the status of scheduled tasks.
"""

class Notifications:

    def __init__(self):
        pass

    def _send_mail(self, msg):
        if os.getenv("WB_EMAIL_SMTP") == "smtp.epa.gov":
            return self._send_mail_epa(msg)
        else:
            return self._send_mail_gmail(msg)

    def _send_mail_gmail(self, msg):
        """
        Sends email using Gmail SMTP.
        """
        try:
            server = smtplib.SMTP_SSL(os.getenv("WB_EMAIL_SMTP"), os.getenv("WB_EMAIL_PORT"))
            server.ehlo()
            server.login(os.getenv("WB_EMAIL_HOST"), os.getenv("WB_EMAIL_PASS"))
            server.sendmail(os.getenv("WB_EMAIL_HOST"), os.getenv("WB_EMAIL_TO"), msg)
            server.close()
            return {"success": "Email sent."}
        except Exception as e:
            logging.error("Error sending reset email: {}".format(e))
            return {"error": "Unable to send email."}

    def _send_mail_epa(self, msg):
        """
        Sends email using EPA SMTP.
        """
        try:
            server = smtplib.SMTP(os.getenv("WB_EMAIL_SMTP"), os.getenv("WB_EMAIL_PORT"))
            server.ehlo()
            server.sendmail(os.getenv("WB_EMAIL_HOST"), os.getenv("WB_EMAIL_TO"), msg)
            server.close()
            return {"success": "Email sent."}
        except Exception as e:
            logging.error("Error sending reset email: {}".format(e))
            return {"error": "Unable to send email."}

    def send_aggregation_status_email(self, agg_status):
        """
        Sends notification of automated aggregation status.
        """
        message_body = """\n
        Aggregation Status: {}\n
        Conus Image Generation Status: {}
        """.format(agg_status["aggregation"], agg_status["conus"])
        subject = "Aggregation and conus image generation status for {}".format(os.getenv("WB_SERVER_NAME"))
        msg = "\r\n".join(
            [
                "From: {}".format(os.getenv("WB_EMAIL_SMTP")),
                "To: {}".format(os.getenv("WB_EMAIL_TO")),
                "Subject: {}".format(subject),
                "",
                message_body
            ]
        )
        email_status = self._send_mail(msg)
        return email_status

    def send_monthly_report_status_email(self, report_status):
        """
        Sends notification of scheduled state and alpine monthly reports status.
        {
            "year": year,
            "day": day,
            "type": "",  # "state" or "alpine"
            "status": ""
        } 
        """
        message_body = "{} report status for year {}, day {}: {}\n".format(
            report_status["type"].capitalize(),
            report_status["year"],
            report_status["day"],
            report_status["status"]
        )
        subject = "Monthly {} report generation status from server: {}".format(report_status["type"], os.getenv("WB_SERVER_NAME"))
        msg = "\r\n".join(
            [
                "From: {}".format(os.getenv("WB_EMAIL_SMTP")),
                "To: {}".format(os.getenv("WB_EMAIL_TO")),
                "Subject: {}".format(subject),
                "",
                message_body
            ]
        )
        email_status = self._send_mail(msg)
        return email_status
