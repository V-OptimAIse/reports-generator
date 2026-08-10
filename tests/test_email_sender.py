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
    instances = []

    def __init__(self, server, port, timeout=None):
        self.server = server
        self.port = port
        self.timeout = timeout
        self.ehlo_calls = 0
        self.login_calls = 0
        self.quit_calls = 0
        self.close_calls = 0
        self.instances.append(self)

    def ehlo(self):
        self.ehlo_calls += 1

    def starttls(self):
        pass

    def login(self, sender, password):
        self.login_calls += 1

    def send_message(self, message):
        self.sent_messages.append(message)

    def quit(self):
        self.quit_calls += 1

    def close(self):
        self.close_calls += 1


class EmailSenderTest(TestCase):
    def setUp(self):
        FakeSmtp.sent_messages = []
        FakeSmtp.instances = []

    def test_plain_text_send_remains_backward_compatible(self):
        with (
            patch("reports.common.email_sender.smtplib.SMTP", FakeSmtp),
            self.assertLogs("reports-generator.email", level="INFO") as logs,
        ):
            EmailSender(cfg=_cfg()).send("Subject", "Plain report")

        message = FakeSmtp.sent_messages[0]
        self.assertEqual(message.get_content_type(), "text/plain")
        self.assertEqual(message.get_content().strip(), "Plain report")
        self.assertEqual(FakeSmtp.instances[0].timeout, 15)
        self.assertTrue(any("smtp_connect=" in line for line in logs.output))
        self.assertTrue(any("smtp_send=" in line for line in logs.output))
        self.assertTrue(any("smtp_quit=" in line for line in logs.output))

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

    def test_context_reuses_one_authenticated_connection_for_multiple_messages(self):
        cfg = _cfg()
        cfg["email"]["smtp_timeout"] = 7

        with patch("reports.common.email_sender.smtplib.SMTP", FakeSmtp):
            with EmailSender(cfg=cfg) as sender:
                sender.send("Raw", "Raw body")
                sender.send("Verified", "Verified body")

        self.assertEqual(len(FakeSmtp.instances), 1)
        smtp = FakeSmtp.instances[0]
        self.assertEqual(smtp.timeout, 7)
        self.assertEqual(smtp.ehlo_calls, 2)
        self.assertEqual(smtp.login_calls, 1)
        self.assertEqual(smtp.quit_calls, 1)
        self.assertEqual(len(FakeSmtp.sent_messages), 2)
