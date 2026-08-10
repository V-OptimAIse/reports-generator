from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from reports.common.email_sender import EmailSender


def _cfg():
    return {
        "email": {
            "sender": "sender@example.com",
            "password": "secret",
            "recipients": ["recipient@example.com"],
            "smtp_server": "smtp.example.com",
            "smtp_port": 587,
        },
    }


class FakeSmtp:
    sent_messages = []

    def __init__(self, server, port):
        self.server = server
        self.port = port

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def starttls(self):
        pass

    def login(self, sender, password):
        pass

    def send_message(self, message):
        self.sent_messages.append(message)


class EmailSenderTest(TestCase):
    def setUp(self):
        FakeSmtp.sent_messages = []

    def test_plain_text_send_remains_backward_compatible(self):
        with patch("reports.common.email_sender.smtplib.SMTP", FakeSmtp):
            EmailSender(cfg=_cfg()).send("Subject", "Plain report")

        message = FakeSmtp.sent_messages[0]
        self.assertEqual(message.get_content_type(), "text/plain")
        self.assertEqual(message.get_content().strip(), "Plain report")

    def test_html_body_creates_multipart_alternative_and_keeps_attachment(self):
        with TemporaryDirectory() as tmp:
            attachment = Path(tmp) / "verified.csv"
            attachment.write_text("Pipe Number\n1\n")

            with patch("reports.common.email_sender.smtplib.SMTP", FakeSmtp):
                EmailSender(cfg=_cfg()).send(
                    "HTML Subject",
                    "Plain fallback",
                    attachments=[str(attachment)],
                    html_body="<html><body><table><tr><td>1</td></tr></table></body></html>",
                )

        message = FakeSmtp.sent_messages[0]
        plain_part = message.get_body(preferencelist=("plain",))
        html_part = message.get_body(preferencelist=("html",))
        self.assertEqual(message.get_content_type(), "multipart/mixed")
        self.assertEqual(plain_part.get_content().strip(), "Plain fallback")
        self.assertEqual(html_part.get_content_type(), "text/html")
        self.assertIn("<table>", html_part.get_content())
        self.assertEqual(len(list(message.iter_attachments())), 1)
