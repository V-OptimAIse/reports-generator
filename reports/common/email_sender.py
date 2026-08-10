import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

from reports.common.config_loader import load_runtime_config


class EmailSender:
    def __init__(self, cfg: dict | None = None):
        cfg = cfg or load_runtime_config()
        self.email_cfg = cfg["email"]
        self.sender = self.email_cfg["sender"]
        self.recipients = self.email_cfg["recipients"]

        password_env = self.email_cfg.get("password_env")
        if password_env:
            self.password = os.getenv(password_env)
        else:
            # fallback (not recommended, but keeps backward compatibility)
            self.password = self.email_cfg.get("password")

        if not self.password:
            raise RuntimeError(
                "Email password not configured. "
                "Set email.password_env in runtime.yaml and export that env var."
            )

    def send(
        self,
        subject: str,
        body: str,
        attachments: list[str] | None = None,
        recipients: list[str] | None = None,
        html_body: str | None = None,
    ):
        to_recipients = recipients if recipients is not None else self.recipients
        if not to_recipients:
            raise RuntimeError("No email recipients configured.")

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.sender
        msg["To"] = ", ".join(to_recipients)
        msg.set_content(body)
        if html_body is not None:
            msg.add_alternative(html_body, subtype="html")

        attachments = attachments or []
        for p in attachments:
            fp = Path(p)
            if not fp.exists():
                continue
            with open(fp, "rb") as f:
                msg.add_attachment(
                    f.read(),
                    maintype="application",
                    subtype="octet-stream",
                    filename=fp.name,
                )

        with smtplib.SMTP(self.email_cfg["smtp_server"], self.email_cfg["smtp_port"]) as server:
            server.starttls()
            server.login(self.sender, self.password)
            server.send_message(msg)

    def send_text(self, subject: str, body: str, recipients: list[str] | None = None):
        self.send(subject=subject, body=body, attachments=[], recipients=recipients)

    def send_csv(
        self,
        subject: str,
        body: str,
        csv_path: str,
        recipients: list[str] | None = None,
    ):
        self.send(subject=subject, body=body, attachments=[csv_path], recipients=recipients)
