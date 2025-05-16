#!/usr/bin/env python3
"""
email_processor.py

This module connects to one or more mailboxes (IMAP or POP3), fetches new emails,
and for each mailbox either:
  - saves PDF attachments only,
  - converts the entire email to PDF,
  - or both (default behavior).

Each mailbox can override the storage backend, choosing between:
  - Dropbox
  - OneDrive
  - Local filesystem

Rules in config.json still control sub-folders under the chosen base.
If no rule matches, the appropriate default folder is used.
OneDrive integration is only executed when storage type is 'onedrive' and
credentials are provided; otherwise, it is skipped.
"""

import os
import json
import imaplib
import poplib
import email
from email import policy
from email.parser import BytesParser
import pdfkit
import dropbox
from dropbox.files import WriteMode
from datetime import datetime

# Path to your configuration file (do NOT hardcode credentials here)
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")


def load_config(config_path):
    """Load JSON configuration from given path.

    Args:
        config_path (str): Path to JSON config file.

    Returns:
        dict: Parsed configuration.
    """
    print(f"Loading configuration from {config_path}")
    with open(config_path, "r") as f:
        cfg = json.load(f)
    print(f"Loaded configuration for {len(cfg.get('mailboxes', []))} mailbox(es)")
    return cfg


def fetch_emails(mail_config):
    """Fetch new emails via IMAP or POP3 according to mailbox config.

    Args:
        mail_config (dict): Mailbox settings with keys:
            protocol (str): "imap" or "pop3"
            host (str), port (int), username (str), password (str)

    Returns:
        List[bytes]: Raw email byte strings.
    """
    protocol = mail_config["protocol"].lower()
    user = mail_config.get("username")
    host = mail_config.get("host")
    print(
        f"Connecting to {protocol.upper()} mailbox {user}@{host}:{mail_config.get('port')}"
    )
    raw_emails = []

    if protocol == "imap":
        imap = imaplib.IMAP4_SSL(host, mail_config["port"])
        imap.login(user, mail_config["password"])
        imap.select("INBOX")
        status, messages = imap.search(None, "UNSEEN")
        ids = messages[0].split()
        print(f"Found {len(ids)} new email(s) in IMAP inbox")
        for num in ids:
            status, data = imap.fetch(num, "(RFC822)")
            for part in data:
                if isinstance(part, tuple):
                    raw_emails.append(part[1])
            imap.store(num, "+FLAGS", "\\Seen")  # mark as read
        imap.logout()

    elif protocol == "pop3":
        pop = poplib.POP3_SSL(host, mail_config["port"])
        pop.user(user)
        pop.pass_(mail_config["password"])
        uid_file = f"processed_uids_{user}.json"
        if os.path.exists(uid_file):
            with open(uid_file, "r") as f:
                processed = set(json.load(f))
        else:
            processed = set()

        resp, listings, _ = pop.list()
        print(f"Found {len(listings)} total email(s) in POP3 inbox")
        for listing in listings:
            num, _ = listing.decode().split()
            resp, uidl_resp, _ = pop.uidl(num)
            uid = uidl_resp.decode().split()[2]
            if uid in processed:
                continue
            resp, lines, _ = pop.retr(num)
            raw_emails.append(b"\r\n".join(lines))
            processed.add(uid)
        pop.quit()
        with open(uid_file, "w") as f:
            json.dump(list(processed), f)
        print(f"Fetched {len(raw_emails)} new email(s) via POP3")

    else:
        raise ValueError(f"Unsupported protocol: {protocol}")

    if not raw_emails:
        print("No new emails to process.")
    return raw_emails


def get_pdf_attachments(msg):
    """Extract PDF attachments from an EmailMessage.

    Args:
        msg (email.message.EmailMessage): Parsed email message.

    Returns:
        List[dict]: Each dict has 'filename' and 'content' (bytes).
    """
    attachments = []
    for part in msg.iter_attachments():
        if part.get_content_type() == "application/pdf":
            filename = part.get_filename()
            data = part.get_content()
            attachments.append({"filename": filename, "content": data})
    return attachments


def convert_email_to_pdf(msg, wkhtml_bin=None):
    """Convert entire email (HTML or text) to PDF bytes.

    Args:
        msg (email.message.EmailMessage): Parsed message.
        wkhtml_bin (str, optional): Path to wkhtmltopdf executable.

    Returns:
        bytes: PDF file content.
    """
    html = None
    text = None
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                html = part.get_content()
                break
            if part.get_content_type() == "text/plain":
                text = part.get_content()
    else:
        if msg.get_content_type() == "text/html":
            html = msg.get_content()
        else:
            text = msg.get_content()

    content = html if html else f"<pre>{text or ''}</pre>"
    config = pdfkit.configuration(wkhtmltopdf=wkhtml_bin) if wkhtml_bin else None
    return pdfkit.from_string(content, False, configuration=config)


def get_applicable_rule(from_addr, subject, rules):
    """Find first matching rule by sender domain or subject prefix.

    Args:
        from_addr (str): 'Name <user@domain>' or 'user@domain'.
        subject (str): Email subject.
        rules (List[dict]): Each rule has:
            type: "domain" or "prefix"
            value: e.g. "example.com" or "INV"
            target_folder: relative path under storage base folder

    Returns:
        dict or None: Matching rule or None.
    """
    domain = from_addr.split("@")[-1].split(">")[0].lower()
    for rule in rules:
        if rule["type"] == "domain" and domain == rule["value"].lower():
            return rule
        if rule["type"] == "prefix" and subject.startswith(rule["value"]):
            return rule
    return None


def save_to_local(filename, data, folder_path):
    """Save a file to local filesystem.

    Args:
        filename (str): Name of the file.
        data (bytes): File content.
        folder_path (str): Absolute path to target folder.
    """
    os.makedirs(folder_path, exist_ok=True)
    path = os.path.join(folder_path, filename)
    with open(path, "wb") as f:
        f.write(data)
    print(f"Local save: {path}")  # Log file save


def upload_to_dropbox(filename, data, folder_path, dbx_config):
    """Upload a file to Dropbox.

    Args:
        filename (str): Name of the file.
        data (bytes): File content.
        folder_path (str): Path under Dropbox root.
        dbx_config (dict): {access_token: str}.
    """
    dbx = dropbox.Dropbox(dbx_config["access_token"])
    dest = f"{folder_path}/{filename}"
    dbx.files_upload(data, dest, mode=WriteMode("overwrite"))
    print(f"Dropbox upload: {dest}")  # Log file upload


def upload_to_onedrive(filename, data, folder_path, od_config):
    """Upload a file to OneDrive via Microsoft Graph.

    Args:
        filename (str): File name.
        data (bytes): File content.
        folder_path (str): Path under OneDrive root.
        od_config (dict): {client_id, client_secret, tenant_id}.
    """
    # Skip if not configured
    if not all(od_config.get(k) for k in ("client_id", "client_secret", "tenant_id")):
        print("OneDrive credentials not configured; skipping OneDrive upload.")
        return
    import msal
    import requests

    authority = f"https://login.microsoftonline.com/{od_config['tenant_id']}"
    app = msal.ConfidentialClientApplication(
        od_config["client_id"],
        client_credential=od_config["client_secret"],
        authority=authority,
    )
    token = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    if "access_token" not in token:
        print("Failed to acquire OneDrive token; skipping upload.")
        return
    headers = {
        "Authorization": f"Bearer {token['access_token']}",
        "Content-Type": "application/octet-stream",
    }
    url = f"https://graph.microsoft.com/v1.0/me/drive/root:/{folder_path}/{filename}:/content"
    requests.put(url, headers=headers, data=data)
    print(f"OneDrive upload: {folder_path}/{filename}")


def save_file(filename, data, subfolder, storage_config):
    """Dispatch upload to chosen storage backend.

    Args:
        filename (str): File name.
        data (bytes): Content.
        subfolder (str): Relative folder under storage base.
        storage_config (dict): {
            type: "dropbox"|"onedrive"|"local",
            base_folder: str,
            dropbox: {...}, onedrive: {...},
            default_attachments_folder: str,
            default_convert_folder: str
        }
    """
    stype = storage_config["type"].lower()
    # Determine target path based on storage type
    if stype == "local":
        folder = os.path.join(storage_config["base_folder"], subfolder)
        save_to_local(filename, data, folder)
    elif stype == "dropbox":
        folder = f"{storage_config['base_folder']}/{subfolder}"
        upload_to_dropbox(filename, data, folder, storage_config["dropbox"])
    elif stype == "onedrive":
        folder = f"{storage_config['base_folder']}/{subfolder}"
        upload_to_onedrive(filename, data, folder, storage_config["onedrive"])
    else:
        raise ValueError(f"Unknown storage type: {storage_config['type']}")


def process_message(msg, rules, mailbox_storage, action, wkhtml_bin=None):
    """Process one email according to mailbox action and storage.

    Args:
        msg (EmailMessage): Parsed message.
        rules (List[dict]): Rule definitions.
        mailbox_storage (dict): Storage settings for this mailbox.
        action (str): "attachments", "convert", or "default".
        wkhtml_bin (str, optional): Path to wkhtmltopdf executable.
    """
    sender = msg.get("From", "<unknown>")
    subject = msg.get("Subject", "<no subject>")
    print(f"Processing message from {sender} | Subject: '{subject}'")
    rule = get_applicable_rule(sender, subject, rules)

    if action == "attachments":
        sub = (
            rule["target_folder"]
            if rule
            else mailbox_storage.get("default_attachments_folder")
        )
        pdfs = get_pdf_attachments(msg)
        print(f"Found {len(pdfs)} PDF attachment(s)")
        for att in pdfs:
            save_file(att["filename"], att["content"], sub, mailbox_storage)

    elif action == "convert":
        sub = (
            rule["target_folder"]
            if rule
            else mailbox_storage.get("default_convert_folder")
        )
        print("Converting email to PDF...")
        pdf_data = convert_email_to_pdf(msg, wkhtml_bin)
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"email_{ts}.pdf"
        save_file(filename, pdf_data, sub, mailbox_storage)

    else:  # default behavior
        pdfs = get_pdf_attachments(msg)
        if pdfs:
            sub = (
                rule["target_folder"]
                if rule
                else mailbox_storage.get("default_attachments_folder")
            )
            print(f"Default action: saving {len(pdfs)} attachment(s)")
            for att in pdfs:
                save_file(att["filename"], att["content"], sub, mailbox_storage)
        else:
            sub = (
                rule["target_folder"]
                if rule
                else mailbox_storage.get("default_convert_folder")
            )
            print("Default action: no attachments, converting email to PDF...")
            pdf_data = convert_email_to_pdf(msg, wkhtml_bin)
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"email_{ts}.pdf"
            save_file(filename, pdf_data, sub, mailbox_storage)


def main():
    """Main entrypoint: load config, iterate mailboxes, fetch & process emails."""
    cfg = load_config(CONFIG_PATH)
    wkhtml_bin = cfg.get("wkhtmltopdf_path")

    for mbox in cfg.get("mailboxes", []):
        user = mbox.get("username")
        host = mbox.get("host")
        action = mbox.get("action", "default")
        print(f"\n=== Processing mailbox {user}@{host} | Action: {action} ===")
        raws = fetch_emails(mbox)
        if not raws:
            continue
        storage_cfg = mbox.get("storage", cfg.get("storage", {}))
        for idx, raw in enumerate(raws, start=1):
            print(f"-- Message {idx} of {len(raws)} --")
            msg = BytesParser(policy=policy.default).parsebytes(raw)
            process_message(msg, cfg.get("rules", []), storage_cfg, action, wkhtml_bin)
        print(f"Finished processing {len(raws)} message(s) for {user}@{host}")


if __name__ == "__main__":
    main()
