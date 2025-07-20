import shutil
import os
import json
from datetime import datetime
import requests
import time


def load_config(config_path="config.json"):
    with open(config_path, "r") as f:
        return json.load(f)
    
config = load_config()
WEBHOOK_URL = config['External']['Discord_Webhook']
MESSAGE_ON_SUCCEED = config['External']['Message_On_Succeed']
MESSAGE_ON_FAILED = config['External']['Message_On_Failed']
FILE_NAME_PREFIX = config['Internal']['File_Name_Prefix']
TIMESTAMP_FORMAT = config['Internal']['Timestamp_Format']
BACKUP_KEEP_LIMIT = config['Internal']['Backup_Keep_Limit']
SOURCE_FOLDER = config['Task']['Source_Folder']
DESTINATION_BASE = config['Task']['Destination_Base']
RETRY_ATTEMPTS = config['Internal']['Retry_On_Operation_Failed_Attempts']
DELAY_ON_RETRY = config['Internal']['Delay_Between_Each_Retry_In_Seconds']

def send_discord_webhook(webhook_url, message):
    if not webhook_url:
        return

    payload = {
        "content": message
    }

    try:
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 204:
            print("Discord notification sent.")
        else:
            print(f"Failed to send Discord notification. Status code: {response.status_code}")
    except Exception as e:
        print(f"Exception when sending Discord webhook: {e}")


def copy_and_rename_after(source_folder, destination_base, file_name_prefix):
    original_folder_name = os.path.basename(os.path.normpath(source_folder))
    temp_destination = os.path.join(destination_base, original_folder_name)

    # ลบ temp folder ถ้ายังค้างอยู่
    if os.path.exists(temp_destination):
        try:
            retry(shutil.rmtree, RETRY_ATTEMPTS, DELAY_ON_RETRY, temp_destination)
        except Exception as e:
            print(f"Failed to remove existing temp folder: {e}")
            send_discord_webhook(WEBHOOK_URL, f"{MESSAGE_ON_FAILED}\nCannot clean up temp folder:\n{e}")
            return

    # Copy ด้วยชื่อเดิมก่อน
    try:
        retry(shutil.copytree, RETRY_ATTEMPTS, DELAY_ON_RETRY, source_folder, temp_destination)
    except Exception as e:
        print(f"Failed to copytree: {e}")
        send_discord_webhook(WEBHOOK_URL, f"{MESSAGE_ON_FAILED}\nError copying folder:\n{e}")
        return

    # Rename → เปลี่ยนชื่อให้มี timestamp ใหม่ทุกรอบ retry
    for attempt in range(RETRY_ATTEMPTS):
        now = datetime.now()
        try:
            timestamp = now.strftime(TIMESTAMP_FORMAT)
        except Exception as e:
            print(f"Invalid timestamp format: {e}")
            send_discord_webhook(WEBHOOK_URL, f"{MESSAGE_ON_FAILED}\nInvalid timestamp format:\n{e}")
            return

        new_folder_name = f"{file_name_prefix}{timestamp}"
        final_destination = os.path.join(destination_base, new_folder_name)

        try:
            if os.path.exists(final_destination):
                raise FileExistsError(f"Folder '{final_destination}' already exists.")
            os.rename(temp_destination, final_destination)
            return final_destination
        except Exception as e:
            print(f"Rename attempt {attempt + 1} failed: {e}")
            if attempt == RETRY_ATTEMPTS - 1:
                try:
                    retry(shutil.rmtree, 3, 1, temp_destination)
                except:
                    pass
                send_discord_webhook(WEBHOOK_URL, f"{MESSAGE_ON_FAILED}\nError renaming folder:\n{e}")
                return
            time.sleep(DELAY_ON_RETRY)



def cleanup_old_backups_by_count(destination_base, file_name_prefix, timestamp_format, keep_limit=BACKUP_KEEP_LIMIT):
    backups = []

    for folder in os.listdir(destination_base):
        if not folder.startswith(file_name_prefix):
            continue

        folder_path = os.path.join(destination_base, folder)
        if not os.path.isdir(folder_path):
            continue

        try:
            timestamp_str = folder[len(file_name_prefix):]
            folder_time = datetime.strptime(timestamp_str, timestamp_format)
            backups.append((folder_time, folder_path))
        except ValueError:
            continue

    backups.sort(key=lambda x: x[0])

    to_delete = backups[:-keep_limit] if len(backups) > keep_limit else []

    deleted = []
    for _, path in to_delete:
        try:
            shutil.rmtree(path)
            deleted.append(path)
        except Exception as e:
            print(f"Failed to delete {path}: {e}")

    return deleted

def retry(func, max_attempts=RETRY_ATTEMPTS, delay=DELAY_ON_RETRY, *args, **kwargs):
    for attempt in range(max_attempts):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")
            if attempt < max_attempts - 1:
                time.sleep(delay)
            else:
                raise e


def main():
    if not SOURCE_FOLDER or not DESTINATION_BASE:
        print("Invalid configuration: missing source_folder or destination_base.")
        send_discord_webhook(WEBHOOK_URL, MESSAGE_ON_FAILED)
        return

    try:
        result_folder = copy_and_rename_after(SOURCE_FOLDER, DESTINATION_BASE, FILE_NAME_PREFIX)
        print(f"[{datetime.now()}] Copied source to: {result_folder}")
        send_discord_webhook(WEBHOOK_URL, MESSAGE_ON_SUCCEED)
        deleted = cleanup_old_backups_by_count(
            DESTINATION_BASE, FILE_NAME_PREFIX, TIMESTAMP_FORMAT, keep_limit=BACKUP_KEEP_LIMIT
        )
        if deleted:
            print(f"Deleted old backups: {deleted}")
    except Exception as e:
        print(f"Failed to copy: {e}")
        send_discord_webhook(WEBHOOK_URL, f"{MESSAGE_ON_FAILED}\nError: {e}")


if __name__ == "__main__":
    main()