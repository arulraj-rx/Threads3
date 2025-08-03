import os
import time
import json
import logging
import requests
import dropbox
from telegram import Bot
from datetime import datetime
from pytz import timezone
import random

class DropboxToThreadsUploader:
    DROPBOX_TOKEN_URL = "https://api.dropbox.com/oauth2/token"
    THREADS_API_BASE = "https://graph.threads.net/v1.0"

    def __init__(self, account_name, threads_user_id, threads_access_token, dropbox_app_key, dropbox_app_secret, dropbox_refresh_token, dropbox_folder, telegram_bot_token=None, telegram_chat_id=None, schedule_file="caption/config.json"):
        self.account_name = account_name
        self.script_name = f"{account_name}_threads_post.py"
        self.ist = timezone('Asia/Kolkata')
        self.account_key = account_name
        self.schedule_file = schedule_file

        # Logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger()

        # Account-specific secrets
        self.threads_access_token = threads_access_token.strip() if threads_access_token else None
        self.threads_user_id = threads_user_id
        self.dropbox_app_key = dropbox_app_key
        self.dropbox_app_secret = dropbox_app_secret
        self.dropbox_refresh_token = dropbox_refresh_token
        self.dropbox_folder = dropbox_folder
        self.telegram_bot_token = telegram_bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = telegram_chat_id or os.getenv("TELEGRAM_CHAT_ID")

        if self.telegram_bot_token is not None:
            self.telegram_bot = Bot(token=self.telegram_bot_token)
        else:
            self.telegram_bot = None
        self.start_time = time.time()
        self.log_buffer = []  # Buffer for log messages

    def send_message(self, msg, level=logging.INFO, immediate=False):
        full_msg = f"[{self.account_name}] [{self.script_name}]\n" + msg
        self.log_buffer.append(full_msg)
        self.logger.log(level, full_msg)
        # Only send immediately if requested (e.g., for critical errors)
        if immediate:
            if self.telegram_bot is not None:
                try:
                    self.telegram_bot.send_message(chat_id=self.telegram_chat_id, text=full_msg)
                except Exception as e:
                    self.logger.error(f"Telegram send error: {e}")
            else:
                self.logger.warning("Telegram bot is not configured. Message not sent to Telegram.")

    def send_log_summary(self):
        if self.telegram_bot is not None and self.log_buffer:
            summary = '\n'.join(self.log_buffer)
            # Telegram messages have a max length; split if needed
            max_len = 4000
            for i in range(0, len(summary), max_len):
                try:
                    self.telegram_bot.send_message(chat_id=self.telegram_chat_id, text=summary[i:i+max_len])
                except Exception as e:
                    self.logger.error(f"Telegram send error: {e}")
        self.log_buffer = []

    def refresh_dropbox_token(self):
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.dropbox_refresh_token,
            "client_id": self.dropbox_app_key,
            "client_secret": self.dropbox_app_secret,
        }
        r = requests.post(self.DROPBOX_TOKEN_URL, data=data)
        r.raise_for_status()
        return r.json().get("access_token")

    def list_dropbox_files(self, dbx):
        files = dbx.files_list_folder(self.dropbox_folder).entries
        valid_exts = ('.mp4', '.mov', '.jpg', '.jpeg', '.png')
        return [f for f in files if f.name.lower().endswith(valid_exts)]

    def get_caption_from_config(self):
        try:
            with open(self.schedule_file, 'r') as f:
                config = json.load(f)
            today = datetime.now(self.ist).strftime("%A")
            return config.get(self.account_key, {}).get(today, {}).get("caption", f"✨ #{self.account_name} ✨")
        except Exception as e:
            self.logger.warning(f"Could not load caption from config: {e}")
            return f"✨ #{self.account_name} ✨"

    def post_to_threads(self, dbx, file, caption):
        name = file.name.lower()
        media_type = "VIDEO" if name.endswith((".mp4", ".mov")) else "IMAGE"

        temp_link = dbx.files_get_temporary_link(file.path_lower).link
        total_files = len(self.list_dropbox_files(dbx))

        self.send_message(f"🚀 Uploading to Threads: {file.name}\n📐 Type: {media_type}\n📦 Remaining: {total_files}")

        post_url = f"{self.THREADS_API_BASE}/{self.threads_user_id}/threads"
        data = {
            "access_token": self.threads_access_token,
            "text": caption,
        }

        if temp_link:
            if media_type == "VIDEO":
                data["video_url"] = temp_link
                data["media_type"] = "VIDEO"
            else:
                data["image_url"] = temp_link
                data["media_type"] = "IMAGE"
            # Step 1: Create media container
            res = requests.post(post_url, data=data)
            if res.status_code != 200:
                self.send_message(f"❌ Threads media container creation failed: {file.name}\n{res.text}", level=logging.ERROR)
                return False
            creation_id = res.json().get("id")
            if not creation_id:
                self.send_message(f"❌ No creation_id returned for {file.name}", level=logging.ERROR)
                return False
            # Step 2: Poll status until fully processed
            max_retries = 20
            for _ in range(max_retries):
                poll_res = requests.get(f"{self.THREADS_API_BASE}/{creation_id}", params={"access_token": self.threads_access_token})
                if poll_res.status_code != 200:
                    self.send_message(f"❌ Polling failed for {file.name}: {poll_res.text}", level=logging.ERROR)
                    return False
                status = poll_res.json().get("status")
                if status == "FINISHED":
                    time.sleep(3)  # Give time for backend to finalize video
                    break
                elif status == "ERROR":
                    self.send_message(f"❌ Transcode failed for {file.name}: {poll_res.text}", level=logging.ERROR)
                    return False
                time.sleep(1)
            # Step 3: Publish
            publish_url = f"{self.THREADS_API_BASE}/{self.threads_user_id}/threads_publish"
            publish_data = {
                "access_token": self.threads_access_token,
                "creation_id": creation_id
            }

            # Optional: Extra wait for first account
            if self.account_name == "eclipsed.by.you":
                self.send_message("⏳ Extra wait before publishing for first account...", level=logging.INFO)
                time.sleep(5)

            # Safe retry-publish block
            for attempt in range(3):
                pub_res = requests.post(publish_url, data=publish_data)
                if pub_res.status_code == 200:
                    self.send_message(f"✅ Successfully posted to Threads: {file.name}")
                    return True
                else:
                    # Check for specific error code (e.g. Threads backend delay)
                    try:
                        err_json = pub_res.json()
                        error_code = err_json.get("error", {}).get("error_subcode")
                        if error_code == 2207032:
                            self.send_message(f"⚠️ Threads backend not ready yet, retrying... ({attempt + 1}/3)", level=logging.WARNING)
                            time.sleep(5)
                            continue
                    except Exception:
                        pass
                    # Unknown error or not a retryable one
                    self.send_message(f"❌ Threads publish failed: {file.name}\n{pub_res.text}", level=logging.ERROR)
                    return False
            # If all retries failed
            self.send_message(f"❌ Threads publish failed after retries: {file.name}", level=logging.ERROR)
            return False
        else:
            data["media_type"] = "TEXT_POST"
            res = requests.post(post_url, data=data)
            if res.status_code == 200:
                self.send_message(f"✅ Successfully posted to Threads: {file.name}")
                return True
            else:
                self.send_message(f"❌ Threads post failed: {file.name}\n{res.text}", level=logging.ERROR)
                return False

    def authenticate_dropbox(self):
        access_token = self.refresh_dropbox_token()
        return dropbox.Dropbox(oauth2_access_token=access_token)

    def run(self):
        self.send_message(f"📡 Threads Run started at: {datetime.now(self.ist).strftime('%Y-%m-%d %H:%M:%S')}")
        try:
            caption = self.get_caption_from_config()
            dbx = self.authenticate_dropbox()
            files = self.list_dropbox_files(dbx)
            if not files:
                self.send_message("📭 No files found in Dropbox folder.")
                return

            file = random.choice(files)  # Pick a random file to post
            success = self.post_to_threads(dbx, file, caption)
            try:
                dbx.files_delete_v2(file.path_lower)
                self.send_message(f"🗑️ Deleted file after attempt: {file.name}")
            except Exception as e:
                self.send_message(f"⚠️ Failed to delete file {file.name}: {e}", level=logging.WARNING)

        except Exception as e:
            self.send_message(f"❌ Script crashed: {e}", level=logging.ERROR)
        finally:
            duration = time.time() - self.start_time
            self.send_message(f"🏁 Run complete in {duration:.1f} seconds")
            self.send_log_summary()

# --- Multi-account logic ---

ACCOUNTS = [
    {
        "account_name": "eclipsed.by.you",
        "threads_user_id": os.getenv("THREADS_USER_ID_1"),
        "threads_access_token": os.getenv("THREADS_ACCESS_TOKEN_1"),
        "dropbox_app_key": os.getenv("DROPBOX_APP_KEY_1"),
        "dropbox_app_secret": os.getenv("DROPBOX_APP_SECRET_1"),
        "dropbox_refresh_token": os.getenv("DROPBOX_REFRESH_TOKEN_1"),
        "dropbox_folder": "/Threads_1",
    },
    {
        "account_name": "inkwisp",
        "threads_user_id": os.getenv("THREADS_USER_ID_2"),
        "threads_access_token": os.getenv("THREADS_ACCESS_TOKEN_2"),
        "dropbox_app_key": os.getenv("DROPBOX_APP_KEY_2"),
        "dropbox_app_secret": os.getenv("DROPBOX_APP_SECRET_2"),
        "dropbox_refresh_token": os.getenv("DROPBOX_REFRESH_TOKEN_2"),
        "dropbox_folder": "/threads_2",
    },
    {
        "account_name": "ink_wisps",
        "threads_user_id": os.getenv("THREADS_USER_ID_3"),
        "threads_access_token": os.getenv("THREADS_ACCESS_TOKEN_3"),
        "dropbox_app_key": os.getenv("DROPBOX_APP_KEY_3"),
        "dropbox_app_secret": os.getenv("DROPBOX_APP_SECRET_3"),
        "dropbox_refresh_token": os.getenv("DROPBOX_REFRESH_TOKEN_3"),
        "dropbox_folder": "/Threads_3",
    }
]

def send_overall_summary(summary_lines, telegram_bot_token, telegram_chat_id):
    logger = logging.getLogger()
    summary = '[Threads Multi-Account Summary]\n' + '\n'.join(summary_lines)
    logger.info(summary)
    try:
        bot = Bot(token=telegram_bot_token)
        max_len = 4000
        for i in range(0, len(summary), max_len):
            bot.send_message(chat_id=telegram_chat_id, text=summary[i:i+max_len])
    except Exception as e:
        logger.error(f"Telegram send error (overall summary): {e}")

if __name__ == "__main__":
    overall_summary = []
    telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
    for account in ACCOUNTS:
        uploader = DropboxToThreadsUploader(**account)
        # Get initial file count
        try:
            dbx = uploader.authenticate_dropbox()
            files = uploader.list_dropbox_files(dbx)
            initial_count = len(files)
        except Exception as e:
            initial_count = 'ERR'
            files = []
        posted_file = None
        post_success = False
        if files:
            # Pick the file that will be posted (randomly, as in run)
            posted_file = random.choice(files)
            # Monkey-patch uploader.run to use the chosen file
            def run_with_file(self, chosen_file):
                self.send_message(f"📡 Threads Run started at: {datetime.now(self.ist).strftime('%Y-%m-%d %H:%M:%S')}")
                try:
                    caption = self.get_caption_from_config()
                    dbx = self.authenticate_dropbox()
                    success = self.post_to_threads(dbx, chosen_file, caption)
                    try:
                        dbx.files_delete_v2(chosen_file.path_lower)
                        self.send_message(f"🗑️ Deleted file after attempt: {chosen_file.name}")
                    except Exception as e:
                        self.send_message(f"⚠️ Failed to delete file {chosen_file.name}: {e}", level=logging.WARNING)
                    return success, chosen_file.name
                except Exception as e:
                    self.send_message(f"❌ Script crashed: {e}", level=logging.ERROR)
                    return False, None
                finally:
                    duration = time.time() - self.start_time
                    self.send_message(f"🏁 Run complete in {duration:.1f} seconds")
                    self.send_log_summary()
            # Run and get result
            post_success, posted_file_name = run_with_file(uploader, posted_file)
        else:
            uploader.run()  # Will log no files found
            posted_file_name = None
        # Build summary line
        if initial_count == 'ERR':
            summary_line = f"{account['account_name']}: Dropbox error, could not count files."
        elif posted_file_name:
            status = '✅ Success' if post_success else '❌ Failed'
            summary_line = f"{account['account_name']}: {initial_count} files, posted: {posted_file_name}, {status}"
        else:
            summary_line = f"{account['account_name']}: {initial_count} files, no file posted."
        overall_summary.append(summary_line)
    # Send overall summary
    send_overall_summary(overall_summary, telegram_bot_token, telegram_chat_id)
