import logging
import os
import smtplib
import time
from email.message import EmailMessage
from pathlib import Path

from reports.common.config_loader import load_runtime_config


logger = logging.getLogger("reports-generator.email")


class EmailSender:
    def __init__(self, cfg: dict | None = None):
        started = time.perf_counter()
        cfg = cfg or load_runtime_config()
        self.email_cfg = cfg["email"]
        self.sender = self.email_cfg["sender"]
        self.recipients = self.email_cfg["recipients"]
        self.smtp_timeout = float(self.email_cfg.get("smtp_timeout", 15))
        if self.smtp_timeout <= 0:
            raise ValueError("email.smtp_timeout must be greater than zero")
        self._smtp: smtplib.SMTP | None = None
        self._keep_connection = False

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
        logger.info(
            "Email timing | initialization=%.3fs | smtp_timeout=%.1fs",
            time.perf_counter() - started,
            self.smtp_timeout,
        )

    def __enter__(self):
        self._keep_connection = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        self._keep_connection = False
        return False

    def _connect(self) -> smtplib.SMTP:
        if self._smtp is not None:
            logger.info("Email timing | smtp_reused=true")
            return self._smtp

        total_started = time.perf_counter()
        smtp = None
        try:
            started = time.perf_counter()
            smtp = smtplib.SMTP(
                self.email_cfg["smtp_server"],
                self.email_cfg["smtp_port"],
                timeout=self.smtp_timeout,
            )
            logger.info("Email timing | smtp_connect=%.3fs", time.perf_counter() - started)

            started = time.perf_counter()
            smtp.ehlo()
            logger.info("Email timing | smtp_ehlo=%.3fs", time.perf_counter() - started)

            started = time.perf_counter()
            smtp.starttls()
            logger.info("Email timing | smtp_starttls=%.3fs", time.perf_counter() - started)

            started = time.perf_counter()
            smtp.ehlo()
            logger.info("Email timing | smtp_ehlo_tls=%.3fs", time.perf_counter() - started)

            started = time.perf_counter()
            smtp.login(self.sender, self.password)
            logger.info("Email timing | smtp_login=%.3fs", time.perf_counter() - started)
            self._smtp = smtp
            logger.info(
                "Email timing | smtp_session_setup=%.3fs",
                time.perf_counter() - total_started,
            )
            return smtp
        except Exception:
            if smtp is not None:
                try:
                    smtp.close()
                except Exception:
                    pass
            logger.exception(
                "Email timing | smtp_session_setup_failed=%.3fs",
                time.perf_counter() - total_started,
            )
            raise

    def _discard_connection(self):
        smtp, self._smtp = self._smtp, None
        if smtp is not None:
            try:
                smtp.close()
            except Exception:
                pass

    def close(self):
        smtp, self._smtp = self._smtp, None
        if smtp is None:
            return
        started = time.perf_counter()
        try:
            smtp.quit()
        except Exception as exc:
            logger.warning("SMTP quit failed; closing socket directly | error=%s", exc)
            try:
                smtp.close()
            except Exception:
                pass
        finally:
            logger.info("Email timing | smtp_quit=%.3fs", time.perf_counter() - started)

    def send(
        self,
        subject: str,
        body: str,
        attachments: list[str] | None = None,
        recipients: list[str] | None = None,
        html_body: str | None = None,
    ):
        total_started = time.perf_counter()
        to_recipients = recipients if recipients is not None else self.recipients
        if not to_recipients:
            raise RuntimeError("No email recipients configured.")

        started = time.perf_counter()
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.sender
        msg["To"] = ", ".join(to_recipients)
        msg.set_content(body)
        if html_body is not None:
            msg.add_alternative(html_body, subtype="html")
        logger.info("Email timing | build_message=%.3fs", time.perf_counter() - started)

        started = time.perf_counter()
        attachments = attachments or []
        attached_count = 0
        for p in attachments:
            fp = Path(p)
            if not fp.exists():
                continue
            msg.add_attachment(
                fp.read_bytes(),
                maintype="application",
                subtype="octet-stream",
                filename=fp.name,
            )
            attached_count += 1
        logger.info(
            "Email timing | attachments=%.3fs | attachment_count=%s",
            time.perf_counter() - started,
            attached_count,
        )

        try:
            smtp = self._connect()
            started = time.perf_counter()
            smtp.send_message(msg)
            logger.info("Email timing | smtp_send=%.3fs", time.perf_counter() - started)
        except Exception:
            self._discard_connection()
            raise
        finally:
            if not self._keep_connection:
                self.close()
            logger.info("Email timing | total=%.3fs", time.perf_counter() - total_started)

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
