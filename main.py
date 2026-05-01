import os
import zipfile
import subprocess
import sys
import shutil
import asyncio
import logging
import time
import signal
import platform
import threading
import queue
from threading import Thread
from flask import Flask, jsonify
from telegram import ReplyKeyboardMarkup, KeyboardButton, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# --- [ᴄᴏɴꜰɪɢᴜʀᴀᴛɪᴏɴ] ---
TOKEN = os.environ.get('BOT_TOKEN', '8787262370:AAHi_dainHlYIARnqbzQSBRKGEAbIal6a-Y')

ADMIN_IDS = [
    int(os.environ.get('ADMIN_ID_1', '7559289812')),
    int(os.environ.get('ADMIN_ID_2', '7559289812')),
    int(os.environ.get('ADMIN_ID_3', '0')),
    int(os.environ.get('ADMIN_ID_4', '0')),
    int(os.environ.get('ADMIN_ID_5', '0')),
    int(os.environ.get('OWNER_ID', '7559289812')),
]
ADMIN_IDS = [aid for aid in ADMIN_IDS if aid != 0]

PRIMARY_ADMIN_ID = ADMIN_IDS[0] if ADMIN_IDS else 7559289812
ADMIN_USERNAME = "RED_EAGLE888"
ADMIN_DISPLAY_NAME = "💞 **SENUxCHEATS** 💞"

# 🔴 Channel Mandatory Settings
REQUIRED_CHANNEL = "HTTPS://T.ME/EAGLE_SRC"
REQUIRED_CHANNEL_ID = -1003634522050

BASE_DIR = os.path.join(os.getcwd(), "hosted_projects")
PORT = int(os.environ.get('PORT', 8080))

# ʟᴏɢɢɪɴɢ ꜱᴇᴛᴜᴘ
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ᴄʀᴇᴀᴛᴇ ᴅɪʀᴇᴄᴛᴏʀɪᴇꜱ
if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR)

# --- [ɢʟᴏʙᴀʟ ᴅᴀᴛᴀ] ---
running_processes = {}
bot_locked = False
auto_restart_mode = False
user_upload_state = {}
project_owners = {}
recovery_enabled = True  # ᴀᴜᴛᴏ ʀᴇᴄᴏᴠᴇʀʏ ꜱᴡɪᴛᴄʜ
live_logs_enabled = True
user_log_sessions = {}

# --- [PSUTIL CHECK] ---
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not available, system health will use basic info")

# --- [ᴀᴜᴛᴏ ᴘᴀᴄᴋᴀɢᴇ ɪɴꜱᴛᴀʟʟᴇʀ] ---
def auto_install_packages():
    """Automatic Package Installer"""
    required_packages = [
        'flask', 'python-telegram-bot', 'psutil', 'aiohttp'
    ]
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            logger.info(f"✅ {package} already installed")
        except ImportError:
            logger.info(f"📦 Installing {package}...")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", package, "--quiet"])
                logger.info(f"✅ {package} installed successfully")
            except Exception as e:
                logger.error(f"❌ Failed to install {package}: {e}")

auto_install_packages()

# --- [ʟᴏɢ ꜱᴛʀᴇᴀᴍᴇʀ ᴄʟᴀꜱꜱ] ---
class LogStreamer:
    """Real-time Log Streaming System"""
    
    def __init__(self):
        self.active_streams = {}  # {project_name: {"queue": Queue(), "subscribers": set()}}
        self.monitor_threads = {}
    
    def start_stream(self, project_name, process):
        """Start log stream for new project"""
        if project_name in self.active_streams:
            return
        
        log_queue = queue.Queue()
        self.active_streams[project_name] = {
            "queue": log_queue,
            "subscribers": set(),
            "process": process,
            "last_lines": [],
            "running": True
        }
        
        stdout_thread = threading.Thread(
            target=self._read_output,
            args=(project_name, process.stdout, "stdout"),
            daemon=True
        )
        stderr_thread = threading.Thread(
            target=self._read_output,
            args=(project_name, process.stderr, "stderr"),
            daemon=True
        )
        
        stdout_thread.start()
        stderr_thread.start()
        
        self.monitor_threads[project_name] = (stdout_thread, stderr_thread)
        logger.info(f"📝 Log stream started for {project_name}")
    
    def _read_output(self, project_name, pipe, pipe_type):
        """Read logs from pipe and put in queue"""
        stream_data = self.active_streams.get(project_name)
        if not stream_data:
            return
        
        try:
            for line in iter(pipe.readline, ''):
                if not stream_data["running"]:
                    break
                
                timestamp = time.strftime("%H:%M:%S")
                log_entry = f"[{timestamp}] [{pipe_type.upper()}] {line.rstrip()}"
                
                stream_data["queue"].put(log_entry)
                
                stream_data["last_lines"].append(log_entry)
                if len(stream_data["last_lines"]) > 50:
                    stream_data["last_lines"].pop(0)
                
                for user_id in list(stream_data["subscribers"]):
                    try:
                        if user_id in user_log_sessions and user_log_sessions[user_id]["active"]:
                            user_log_sessions[user_id]["buffer"].append(log_entry)
                    except:
                        pass
                        
        except Exception as e:
            logger.error(f"Log read error for {project_name}: {e}")
        finally:
            pipe.close()
    
    def subscribe(self, project_name, user_id, chat_id, message_id):
        """Add user to log stream"""
        if project_name not in self.active_streams:
            return False
        
        stream_data = self.active_streams[project_name]
        stream_data["subscribers"].add(user_id)
        
        user_log_sessions[user_id] = {
            "project": project_name,
            "chat_id": chat_id,
            "message_id": message_id,
            "buffer": list(stream_data["last_lines"]),
            "active": True,
            "last_update": time.time()
        }
        return True
    
    def unsubscribe(self, user_id):
        """Remove user from log stream"""
        if user_id in user_log_sessions:
            project_name = user_log_sessions[user_id]["project"]
            if project_name in self.active_streams:
                self.active_streams[project_name]["subscribers"].discard(user_id)
            user_log_sessions[user_id]["active"] = False
            return True
        return False
    
    def stop_stream(self, project_name):
        """Stop log stream for project"""
        if project_name in self.active_streams:
            self.active_streams[project_name]["running"] = False
            if project_name in self.monitor_threads:
                for thread in self.monitor_threads[project_name]:
                    thread.join(timeout=2)
            del self.active_streams[project_name]
            if project_name in self.monitor_threads:
                del self.monitor_threads[project_name]
    
    def get_recent_logs(self, project_name, lines=20):
        """Get last few lines"""
        if project_name in self.active_streams:
            return self.active_streams[project_name]["last_lines"][-lines:]
        return []
    
    def is_streaming(self, project_name):
        """Check if stream is running"""
        return project_name in self.active_streams and self.active_streams[project_name]["running"]

log_streamer = LogStreamer()

# --- [ʜᴇʟᴘᴇʀ ꜰᴜɴᴄᴛɪᴏɴ] ---
def is_admin(user_id):
    """Check if user is admin"""
    return user_id in ADMIN_IDS

# --- [ᴄʜᴀɴɴᴇʟ ᴍᴇᴍʙᴇʀꜱʜɪᴘ ᴄʜᴇᴄᴋ] ---
async def check_channel_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not REQUIRED_CHANNEL_ID: return True
    if is_admin(user_id): return True
    try:
        member = await context.bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID, user_id=user_id)
        return member.status not in ['left', 'kicked', 'banned']
    except Exception as e:
        logger.error(f"Membership check error: {e}")
        return False

async def require_channel_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_channel_membership(user_id, context):
        keyboard = [
            [InlineKeyboardButton("📢 Join Channel", url=REQUIRED_CHANNEL)],
            [InlineKeyboardButton("✅ I have joined", callback_data="check_join")]
        ]
        msg = "⚠️ **You must join our official channel to use this bot!**\n\n1. Click the button below to join.\n2. After joining, click 'I have joined'."
        if update.message:
            await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        elif getattr(update, 'callback_query', None):
            await update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return False
    return True

# --- [ʟᴏᴀᴅɪɴɢ ᴀɴɪᴍᴀᴛɪᴏɴꜱ] ---
class Loading:
    @staticmethod
    def executing():
        return [
            "🌺 ᴇxᴇᴄᴜᴛɪɴɢ: [▱▱▱▱▱▱▱▱▱▱] 0%",
            "🌼 ᴇxᴇᴄᴜᴛɪɴɗ: [▰▱▱▱▱▱▱▱▱▱] 10%",
            "🌻 ᴇxᴇᴄᴜᴛɪɴɗ: [▰▰▱▱▱▱▱▱▱▱] 20%",
            "🌸 ᴇxᴇᴄᴜᴛɪɴɗ: [▰▰▰▱▱▱▱▱▱▱] 30%",
            "🌹 ᴇxᴇᴄᴜᴛɪɴɗ: [▰▰▰▰▱▱▱▱▱▱] 40%",
            "🍁 ᴇxᴇᴄᴜᴛɪɴɗ: [▰▰▰▰▰▱▱▱▱▱] 50%",
            "🌿 ᴇxᴇᴄᴜᴛɪɴɗ: [▰▰▰▰▰▰▱▱▱▱] 60%",
            "🌳 ᴇxᴇᴄᴜᴛɪɴɗ: [▰▰▰▰▰▰▰▱▱▱] 70%",
            "🌲 ᴇxᴇᴄᴜᴛɪɴɗ: [▰▰▰▰▰▰▰▰▱▱] 80%",
            "🪷 ᴇxᴇᴄᴜᴛɪɴɗ: [▰▰▰▰▰▰▰▰▰▱] 90%",
            "✅ ᴄᴏᴍᴘʟᴇᴛᴇ: [▰▰▰▰▰▰▰▰▰▰] 100%"
        ]
    
    @staticmethod
    def uploading():
        return [
            "🗳️ ᴜᴘʟᴏᴀᴅɪɴɗ: [▱▱▱▱▱▱▱▱▱▱] 0%",
            "🗳️ ᴜᴘʟᴏᴀᴅɪɴɗ: [▰▱▱▱▱▱▱▱▱▱] 25%",
            "🗳️ ᴜᴘʟᴏᴀᴅɪɴɗ: [▰▰▰▱▱▱▱▱▱▱] 50%",
            "🗳️ ᴜᴘʟᴏᴀᴅɪɴɗ: [▰▰▰▰▰▰▱▱▱▱] 75%",
            "✅ ᴜᴘʟᴏᴀᴅ ᴄᴏᴍᴘʟᴇᴛᴇ: [▰▰▰▰▰▰▰▰▰▰] 100%"
        ]
    
    @staticmethod
    def installing():
        return [
            "📦 ɪɴꜱᴛᴀʟʟɪɴɗ: [▱▱▱▱▱▱▱▱▱▱] 0%",
            "📦 ɪɴꜱᴛᴀʟʟɪɴɗ: [▰▰▱▱▱▱▱▱▱▱] 20%",
            "📦 ɪɴꜱᴛᴀʟʟɪɴɗ: [▰▰▰▰▱▱▱▱▱▱] 40%",
            "📦 ɪɴꜱᴛᴀʟʟɪɴɗ: [▰▰▰▰▰▰▱▱▱▱] 60%",
            "📦 ɪɴꜱᴛᴀʟʟɪɴɗ: [▰▰▰▰▰▰▰▰▱▱] 80%",
            "✅ ɪɴꜱᴛᴀʟʟᴇᴅ: [▰▰▰▰▰▰▰▰▰▰] 100%"
        ]
    
    @staticmethod
    def deleting():
        return [
            "🗑️ ᴅᴇʟᴇᴛɪɴɗ: [▱▱▱▱▱▱▱▱▱▱] 0%",
            "🗑️ ᴅᴇʟᴇᴛɪɴɗ: [▰▰▰▱▱▱▱▱▱▱] 30%",
            "🗑️ ᴅᴇʟᴇᴛɪɴɗ: [▰▰▰▰▰▰▱▱▱▱] 60%",
            "✅ ᴅᴇʟᴇᴛᴇᴅ: [▰▰▰▰▰▰▰▰▰▰] 100%"
        ]
    
    @staticmethod
    def restarting():
        return [
            "🇵🇰 ʀᴇꜱᴛᴀʀᴛɪɴɗ: [▱▱▱▱▱▱▱▱▱▱] 0%",
            "🇵🇰 ʀᴇꜱᴛᴀʀᴛɪɴɗ: [▰▰▱▱▱▱▱▱▱▱] 20%",
            "🇵🇰 ʀᴇꜱᴛᴀʀᴛɪɴɗ: [▰▰▰▰▱▱▱▱▱▱] 40%",
            "🇵🇰 ʀᴇꜱᴛᴀʀᴛɪɴɗ: [▰▰▰▰▰▰▱▱▱▱] 60%",
            "🇵🇰 ʀᴇꜱᴛᴀʀᴛɪɴɗ: [▰▰▰▰▰▰▰▰▱▱] 80%",
            "✅ ʀᴇꜱᴛᴀʀᴛᴇᴅ: [▰▰▰▰▰▰▰▰▰▰] 100%"
        ]
    
    @staticmethod
    def recovering():
        return [
            "🔄 ʀᴇᴄᴏᴠᴇʀɪɴɗ: [▱▱▱▱▱▱▱▱▱▱] 0%",
            "🔄 ʀᴇᴄᴏᴠᴇʀɪɴɗ: [▰▰▰▱▱▱▱▱▱▱] 30%",
            "🔄 ʀᴇᴄᴏᴠᴇʀɪɴɗ: [▰▰▰▰▰▰▱▱▱▱] 60%",
            "✅ ʀᴇᴄᴏᴠᴇʀᴇᴅ: [▰▰▰▰▰▰▰▰▰▰] 100%"
        ]
    
    @staticmethod
    def logs_on():
        return [
            "📺 ʟɪᴠᴇ ʟᴏɢꜱ: [▱▱▱▱▱▱▱▱▱▱] ᴏꜰꜰ",
            "📺 ʟɪᴠᴇ ʟᴏɢꜱ: [▰▰▰▱▱▱▱▱▱▱] ꜱᴛᴀʀᴛɪɴɗ...",
            "📺 ʟɪᴠᴇ ʟᴏɢꜱ: [▰▰▰▰▰▰▱▱▱▱] ᴄᴏɴɴᴇᴄᴛɪɴɗ...",
            "✅ ʟɪᴠᴇ ʟᴏɢꜱ: [▰▰▰▰▰▰▰▰▰▰] ᴏɴʟɪɴᴇ"
        ]
    
    @staticmethod
    def logs_off():
        return [
            "📺 ʟɪᴠᴇ ʟᴏɢꜱ: [▰▰▰▰▰▰▰▰▰▰] ᴏɴʟɪɴᴇ",
            "📺 ʟɪᴠᴇ ʟᴏɢꜱ: [▰▰▰▰▰▰▱▱▱▱] ᴅɪꜱᴄᴏɴɴᴇᴄᴛɪɴɗ...",
            "📺 ʟɪᴠᴇ ʟᴏɢꜱ: [▰▰▰▱▱▱▱▱▱▱] ᴄʟᴏꜱɪɴɗ...",
            "❌ ʟɪᴠᴇ ʟᴏɢꜱ: [▱▱▱▱▱▱▱▱▱▱] ᴏꜰꜰ"
        ]

# ʜᴇʟᴘᴇʀ ꜰᴜɴᴄᴛɪᴏɴ ꜰᴏʀ ʟᴏᴀᴅɪɴɗ ᴀɴɪᴍᴀᴛɪᴏɴ
async def animate(update, context, frames, delay=0.5, final_text=None):
    msg = await update.message.reply_text(frames[0]) if hasattr(update, 'message') else await update.edit_message_text(frames[0])
    for frame in frames[1:]:
        await asyncio.sleep(delay)
        try:
            msg = await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text=frame)
        except:
            pass
    if final_text:
        await asyncio.sleep(0.3)
        try:
            await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text=final_text, parse_mode='Markdown')
        except:
            pass
    return msg

# --- [ꜰʟᴀꜱᴋ ᴡᴇʙ ꜱᴇʀᴠᴇʀ] ---
app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({
        "status": "online",
        "service": "ᴀᴘᴏɴ ᴘʀᴇᴍɪᴜᴍ ʜᴏꜱᴛɪɴɗ ᴠ1",
        "projects": len(project_owners),
        "running": len([p for p in running_processes.values() if p.poll() is None]),
        "recovery": recovery_enabled,
        "live_logs": live_logs_enabled
    })

@app.route('/health')
def health():
    return jsonify({"status": "healthy"}), 200

def run_web():
    app.run(host='0.0.0.0', port=PORT, debug=False)
# --- [ᴋᴇʏʙᴏᴀʀᴅ ꜱᴇᴛᴜᴘ] ---
def get_main_keyboard(user_id):
    lock_status = "🔓 ᴜɴʟᴏᴄᴋ ꜱʏꜱᴛᴇᴍ" if bot_locked else "🔒 ʟᴏᴄᴋ ꜱʏꜱᴛᴇᴍ"
    restart_status = "🔄 ᴀᴜᴛᴏ ʀᴇꜱᴛᴀʀᴛ: ᴏꜰꜰ" if auto_restart_mode else "🔄 ᴀᴜᴛᴏ ʀᴇꜱᴛᴀʀᴛ: ᴏɴ"
    recovery_status = "🛡️ ʀᴇᴄᴏᴠᴇʀʏ: ᴏꜰꜰ" if recovery_enabled else "🛡️ ʀᴇᴄᴏᴠᴇʀʏ: ᴏɴ"
    logs_status = "📺 ʟɪᴠᴇ ʟᴏɢꜱ: ᴏꜰꜰ" if live_logs_enabled else "📺 ʟɪᴠᴇ ʟᴏɢꜱ: ᴏɴ"
    
    if is_admin(user_id):
        layout = [
            [KeyboardButton("🗳️ ᴜᴘʟᴏᴀᴅ ᴍᴀɴᴀɢᴇʀ"), KeyboardButton("📮 ꜰɪʟᴇ ᴍᴀɴᴀɢᴇʀ")],
            [KeyboardButton("🗑️ ᴅᴇʟᴇᴛᴇ ᴍᴀɴᴀɢᴇʀ"), KeyboardButton("🏩 ꜱʏꜱᴛᴇᴍ ʜᴇᴀʟᴛʜ")],
            [KeyboardButton("🌎 ꜱᴇʀᴠᴇʀ ɪɴꜰᴏ"), KeyboardButton("📠 ᴄᴏɴᴛᴀᴄᴛ ᴀᴅᴍɪɴ")],
            [KeyboardButton(lock_status), KeyboardButton(restart_status)],
            [KeyboardButton(recovery_status), KeyboardButton("🎬 ᴘʀᴏᴊᴇᴄᴛ ꜰɪʟᴇ")],
            [KeyboardButton(logs_status)]
        ]
    else:
        layout = [
            [KeyboardButton("🗳️ ᴜᴘʟᴏᴀᴅ ᴍᴀɴᴀɢᴇʀ"), KeyboardButton("📮 ꜰɪʟᴇ ᴍᴀɴᴀɢᴇʀ")],
            [KeyboardButton("🗑️ ᴅᴇʟᴇᴛᴇ ᴍᴀɴᴀɢᴇʀ"), KeyboardButton("🏩 ꜱʏꜱᴛᴇᴍ ʜᴇᴀʟᴛʜ")],
            [KeyboardButton("🌎 ꜱᴇʀᴠᴇʀ ɪɴꜰᴏ"), KeyboardButton("📠 ᴄᴏɴᴛᴀᴄᴛ ᴀᴅᴍɪɴ")],
            [KeyboardButton(logs_status)]
        ]
    return ReplyKeyboardMarkup(layout, resize_keyboard=True)

# --- [ʟɪᴠᴇ ʟᴏɢꜱ ᴠɪᴇᴡᴇʀ ᴛᴀꜱᴋ] ---
async def log_viewer_task(context: ContextTypes.DEFAULT_TYPE):
    """Background task that updates user log messages"""
    logger.info("📝 Log viewer task started")
    while True:
        try:
            if not live_logs_enabled:
                await asyncio.sleep(2)
                continue
            
            current_time = time.time()
            
            for user_id, session in list(user_log_sessions.items()):
                if not session["active"]:
                    continue
                
                if current_time - session["last_update"] < 2:
                    continue
                
                logs = session["buffer"][-20:]
                session["buffer"] = []
                
                if not logs and not session.get("has_content"):
                    continue
                
                log_text = "\n".join(logs) if logs else "⏳ Waiting for logs..."
                
                terminal_text = (
                    f"📺 **ʟɪᴠᴇ ᴄᴏɴꜱᴏʟᴇ - {session['project']}**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"```\n"
                    f"{log_text[-3500:]}\n"
                    f"```\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🟢 ᴏɴʟɪɴᴇ | 🔄 ᴀᴜᴛᴏ-ᴜᴘᴅᴀᴛᴇ: 2ꜱ"
                )
                
                try:
                    await context.bot.edit_message_text(
                        chat_id=session["chat_id"],
                        message_id=session["message_id"],
                        text=terminal_text,
                        parse_mode='Markdown'
                    )
                    session["last_update"] = current_time
                    session["has_content"] = True
                except Exception as e:
                    if "message is not modified" not in str(e).lower():
                        logger.debug(f"Log update error for user {user_id}: {e}")
                        if "message to edit not found" in str(e).lower():
                            session["active"] = False
            
            await asyncio.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Log viewer task error: {e}")
            await asyncio.sleep(2)

# --- [ꜱʏꜱᴛᴇᴍ ʜᴇᴀʟᴛʜ ꜰᴜɴᴄᴛɪᴏɴ] ---
async def get_system_health():
    """Collects system health data"""
    try:
        if PSUTIL_AVAILABLE:
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = psutil.cpu_count()
            
            ram = psutil.virtual_memory()
            ram_used_gb = ram.used / (1024**3)
            ram_total_gb = ram.total / (1024**3)
            ram_percent = ram.percent
            
            disk = psutil.disk_usage('/')
            disk_used_gb = disk.used / (1024**3)
            disk_total_gb = disk.total / (1024**3)
            disk_percent = disk.percent
            
            boot_time = psutil.boot_time()
            uptime = time.time() - boot_time
            
            return {
                "status": "ok",
                "cpu": f"{cpu_percent}%",
                "cpu_cores": cpu_count,
                "ram": f"{ram_percent}%",
                "ram_used": f"{ram_used_gb:.1f}GB",
                "ram_total": f"{ram_total_gb:.1f}GB",
                "disk": f"{disk_percent}%",
                "disk_used": f"{disk_used_gb:.1f}GB",
                "disk_total": f"{disk_total_gb:.1f}GB",
                "uptime": f"{int(uptime//3600)}h {int((uptime%3600)//60)}m"
            }
        else:
            return {
                "status": "basic",
                "platform": platform.system(),
                "machine": platform.machine(),
                "processor": platform.processor() or "Unknown",
                "python_version": platform.python_version()
            }
    except Exception as e:
        logger.error(f"System health error: {e}")
        return {"status": "error", "error": str(e)}

# --- [ᴄᴏʀᴇ ꜰᴜɴᴄᴛɪᴏɴꜱ] ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not await require_channel_join(update, context):
        return
    
    if bot_locked and not is_admin(user_id):
        await update.message.reply_text("🔒 **ꜱʏꜱᴛᴇᴍ ɪꜱ ᴄᴜʀʀᴇɴᴛʟʏ ʟᴏᴄᴋᴇᴅ ʙʏ ᴀᴅᴍɪɴ**", parse_mode='Markdown')
        return
    
    msg = (
        "🌍 **ʟᴀᴍ ᴘʀᴇᴍɪᴜᴍ ʜᴏꜱᴛɪɴɗ ᴠ1** 🌸\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "💙 **ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ ᴛʜᴇ ᴇʟɪᴛᴇ ᴘᴀɴᴇʟ**\n"
        "🔮 **Welcome! This is the most powerful premium server in Pakistan.**\n\n"
        f"🇵🇰 **ᴏᴡɴᴇʀ:** `{ADMIN_USERNAME}`\n"
        f"📢 **ᴄʜᴀɴɴᴇʟ:** {'Not Set' if not REQUIRED_CHANNEL else REQUIRED_CHANNEL}\n"
        "━━━━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(msg, reply_markup=get_main_keyboard(user_id), parse_mode='Markdown')

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text
    global bot_locked, auto_restart_mode, recovery_enabled, live_logs_enabled

    if not await require_channel_join(update, context):
        return

    if bot_locked and not is_admin(user_id):
        await update.message.reply_text("🔒 **System is currently locked.**", parse_mode='Markdown')
        return

    # ʟɪᴠᴇ ʟᴏɢꜱ ᴛᴏɢɢʟᴇ 🔴
    if "📺 ʟɪᴠᴇ ʟᴏɢꜱ:" in text:
        if "ᴏɴ" in text:
            live_logs_enabled = True
            await animate(update, context, Loading.logs_on(), delay=0.5, final_text="📺 **ʟɪᴠᴇ ʟᴏɢꜱ: ᴇɴᴀʙʟᴇᴅ**")
        else:
            live_logs_enabled = False
            for uid in list(user_log_sessions.keys()):
                log_streamer.unsubscribe(uid)
            await animate(update, context, Loading.logs_off(), delay=0.5, final_text="❌ **ʟɪᴠᴇ ʟᴏɢꜱ: ᴅɪꜱᴀʙʟᴇᴅ**")
        
        await update.message.reply_text("ᴍᴇɴᴜ ᴜᴘᴅᴀᴛᴇᴅ!", reply_markup=get_main_keyboard(user_id), parse_mode='Markdown')
        return

    # ᴘʀᴏᴊᴇᴄᴛ ɴᴀᴍɪɴɗ
    if user_id in user_upload_state and "path" in user_upload_state[user_id]:
        p_name = text.replace(" ", "_").replace("/", "_")
        state = user_upload_state[user_id]
        extract_path = os.path.join(BASE_DIR, p_name)
        
        try:
            # ʟᴏᴀᴅɪɴɗ ᴀɴɪᴍᴀᴛɪᴏɴ ꜰᴏʀ ᴇxᴛʀᴀᴄᴛɪɴɗ
            msg = await animate(update, context, Loading.executing(), delay=0.4)
            
            os.makedirs(extract_path, exist_ok=True)
            with zipfile.ZipFile(state["path"], 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            
            # ᴄʜᴇᴄᴋ ꜰᴏʀ ᴍᴀɪɴ.ᴘʏ ᴀɴᴅ ʀᴇǫᴜɪʀᴇᴍᴇɴᴛꜱ.ᴛxᴛ
            main_py = os.path.join(extract_path, "main.py")
            req_txt = os.path.join(extract_path, "requirements.txt")
            
            if not os.path.exists(main_py):
                await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text="❌ **ᴇʀʀᴏʀ: ᴍᴀɪɴ.ᴘʏ ɴᴏᴛ ꜰᴏᴜɴᴅ ɪɴ ᴢɪᴘ!**", parse_mode='Markdown')
                shutil.rmtree(extract_path)
                return
            
            # ɪɴꜱᴛᴀʟʟ ʀᴇǫᴜɪʀᴇᴍᴇɴᴛꜱ ɪꜰ ᴇxɪꜱᴛꜱ
            if os.path.exists(req_txt):
                for frame in Loading.installing():
                    await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text=frame)
                    await asyncio.sleep(1.0)
                
                try:
                    subprocess.run([sys.executable, "-m", "pip", "install", "-r", req_txt], check=True, capture_output=True, text=True, cwd=extract_path)
                except subprocess.CalledProcessError as e:
                    logger.error(f"ʀᴇǫᴜɪʀᴇᴍᴇɴᴛꜱ ɪɴꜱᴛᴀʟʟ ꜰᴀɪʟᴇᴅ: {e}")
                    await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text="⚠️ **ᴡᴀʀɴɪɴɗ: ꜱᴏᴍᴇ ʀᴇǫᴜɪʀᴇᴍᴇɴᴛꜱ ꜰᴀɪʟᴇᴅ ᴛᴏ ɪɴꜱᴛᴀʟʟ**", parse_mode='Markdown')
                    await asyncio.sleep(1)
            
            # ꜱᴀᴠᴇ ᴘʀᴏᴊᴇᴄᴛ ᴅᴀᴛᴀ
            project_owners[p_name] = {
                "u_id": user_id,
                "u_name": state["u_name"],
                "u_username": update.effective_user.username or "ɴᴏ_ᴜꜱᴇʀɴᴀᴍᴇ",
                "zip": state["path"],
                "original_name": state["original_name"],
                "path": extract_path
            }
            del user_upload_state[user_id]
            
            final_text = (
                f"✅ **ᴘʀᴏᴊᴇᴄᴛ `{p_name}` ꜱᴀᴠᴇᴅ!**\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"🚀 **Now go to '📮 FILE MANAGER' and run it.**\n"
                f"━━━━━━━━━━━━━━━━━━━━━"
            )
            await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text=final_text, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"ᴜᴘʟᴏᴀᴅ ᴇʀʀᴏʀ: {e}")
            await update.message.reply_text(f"❌ **ᴇʀʀᴏʀ:** `{str(e)}`", parse_mode='Markdown')
        return

    # ʙᴜᴛᴛᴏɴ ʜᴀɴᴅʟᴇʀꜱ
    if text == "🗳️ ᴜᴘʟᴏᴀᴅ ᴍᴀɴᴀɢᴇʀ":
        await update.message.reply_text(
            "🗳️ **ᴜᴘʟᴏᴀᴅ ᴍᴀɴᴀɢᴇʀ**\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "📪 **ꜱᴇɴᴅ ʏᴏᴜʀ .ᴢɪᴘ ꜰɪʟᴇ ᴄᴏɴᴛᴀɪɴɪɴɗ:**\n"
            "• `ᴍᴀɪɴ.ᴘʏ` (ʏᴏᴜʀ ʙᴏᴛ ᴄᴏᴅᴇ)\n"
            "• `ʀᴇǫᴜɪʀᴇᴍᴇɴᴛꜱ.ᴛxᴛ` (ᴅᴇᴘᴇɴᴅᴇɴᴄɪᴇꜱ)\n"
            "━━━━━━━━━━━━━━━━━━━━━", parse_mode='Markdown')

    elif text == "📮 ꜰɪʟᴇ ᴍᴀɴᴀɢᴇʀ":
        user_projects = [p for p, d in project_owners.items() if d["u_id"] == user_id]
        if not user_projects:
            await update.message.reply_text("📮 **ɴᴏ ᴘʀᴏᴊᴇᴄᴛꜱ ꜰᴏᴜɴᴅ**", parse_mode='Markdown')
            return
        keyboard = []
        for p in user_projects:
            status = "💚 ᴏɴʟɪɴᴇ" if (p in running_processes and running_processes[p].poll() is None) else "💔 ᴏꜰꜰʟɪɴᴇ"
            keyboard.append([InlineKeyboardButton(f"{status} | {p}", callback_data=f"manage_{p}")])
        
        await update.message.reply_text(
            "📮 **ᴍʏ ꜰɪʟᴇ ᴍᴀɴᴀɢᴇʀ**\n"
            "━━━━━━━━━━━━━━━━━━━━━", 
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    elif text == "🗑️ ᴅᴇʟᴇᴛᴇ ᴍᴀɴᴀɢᴇʀ":
        user_projects = [p for p, d in project_owners.items() if d["u_id"] == user_id]
        if not user_projects:
            await update.message.reply_text("🗑️ **ɴᴏ ᴘʀᴏᴊᴇᴄᴛꜱ**", parse_mode='Markdown')
            return
        keyboard = [[InlineKeyboardButton(f"🗑️ {p}", callback_data=f"del_{p}")] for p in user_projects]
        await update.message.reply_text("🗑️ **ꜱᴇʟᴇᴄᴛ ᴘʀᴏᴊᴇᴄᴛ ᴛᴏ ᴅᴇʟᴇᴛᴇ:**", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    # ᴀᴅᴍɪɴ ᴄᴏɴᴛʀᴏʟꜱ ᴡɪᴛʜ ᴛᴏɢɢʟᴇ ʙᴜᴛᴛᴏɴꜱ
    elif "🔄 ᴀᴜᴛᴏ ʀᴇꜱᴛᴀʀᴛ:" in text and is_admin(user_id):
        if "ᴏɴ" in text:
            auto_restart_mode = True
            await animate(update, context, Loading.restarting(), delay=0.5, final_text="🔄 **ᴀᴜᴛᴏ ʀᴇꜱᴛᴀʀᴛ: ᴀᴄᴛɪᴠᴀᴛᴇᴅ**")
        else:
            auto_restart_mode = False
            await animate(update, context, Loading.restarting(), delay=0.5, final_text="🔄 **ᴀᴜᴛᴏ ʀᴇꜱᴛᴀʀᴛ: ᴅᴇᴀᴄᴛɪᴠᴀᴛᴇᴅ**")
        await update.message.reply_text("ᴍᴇɴᴜ ᴜᴘᴅᴀᴛᴇᴅ!", reply_markup=get_main_keyboard(user_id), parse_mode='Markdown')

    elif text in ["🔒 ʟᴏᴄᴋ ꜱʏꜱᴛᴇᴍ", "🔓 ᴜɴʟᴏᴄᴋ ꜱʏꜱᴛᴇᴍ"] and is_admin(user_id):
        if "ʟᴏᴄᴋ" in text and "ᴜɴʟᴏᴄᴋ" not in text:
            bot_locked = True
            await animate(update, context, Loading.executing(), delay=0.3, final_text="🔒 **ꜱʏꜱᴛᴇᴍ ʟᴏᴄᴋᴇᴅ**")
        else:
            bot_locked = False
            await animate(update, context, Loading.executing(), delay=0.3, final_text="🔓 **ꜱʏꜱᴛᴇᴍ ᴜɴʟᴏᴄᴋᴇᴅ**")
        await update.message.reply_text("ᴍᴇɴᴜ ᴜᴘᴅᴀᴛᴇᴅ!", reply_markup=get_main_keyboard(user_id), parse_mode='Markdown')
    
    elif "🛡️ ʀᴇᴄᴏᴠᴇʀʏ:" in text and is_admin(user_id):
        if "ᴏɴ" in text:
            recovery_enabled = True
            await animate(update, context, Loading.recovering(), delay=0.5, final_text="🛡️ **ᴀᴜᴛᴏ ʀᴇᴄᴏᴠᴇʀʏ: ᴇɴᴀʙʟᴇᴅ**")
        else:
            recovery_enabled = False
            await animate(update, context, Loading.recovering(), delay=0.5, final_text="🛡️ **ᴀᴜᴛᴏ ʀᴇᴄᴏᴠᴇʀʏ: ᴅɪꜱᴀʙʟᴇᴅ**")
        await update.message.reply_text("ᴍᴇɴᴜ ᴜᴘᴅᴀᴛᴇᴅ!", reply_markup=get_main_keyboard(user_id), parse_mode='Markdown')

    elif text == "🎬 ᴘʀᴏᴊᴇᴄᴛ ꜰɪʟᴇ" and is_admin(user_id):
        total_projects = len(project_owners)
        running_count = len([p for p in running_processes.values() if p.poll() is None])
        offline_count = total_projects - running_count
        
        status_text = (
            "🎬 **ᴘʀᴏᴊᴇᴄᴛ ꜱᴛᴀᴛᴜꜱ**\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 **ᴛᴏᴛᴀʟ ᴘʀᴏᴊᴇᴄᴛꜱ:** `{total_projects}`\n"
            f"💚 **ᴏɴʟɪɴᴇ:** `{running_count}`\n"
            f"💔 **ᴏꜰꜰʟɪɴᴇ:** `{offline_count}`\n"
            f"📺 **ʟɪᴠᴇ ʟᴏɢꜱ:** `{'ᴏɴ' if live_logs_enabled else 'ᴏꜰꜰ'}`\n"
            "━━━━━━━━━━━━━━━━━━━━━"
        )
        await update.message.reply_text(status_text, parse_mode='Markdown')

    elif text == "🏩 ꜱʏꜱᴛᴇᴍ ʜᴇᴀʟᴛʜ":
        msg = await update.message.reply_text("🏩 **ᴄʜᴇᴄᴋɪɴɗ ꜱʏꜱᴛᴇᴍ ʜᴇᴀʟᴛʜ...**")
        
        try:
            health_data = await get_system_health()
            
            if health_data["status"] == "ok":
                msg_text = (
                    "🏩 **ꜱʏꜱᴛᴇᴍ ʜᴇᴀʟᴛʜ**\n"
                    "━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🖥️ **ᴄᴘᴜ:** {health_data['cpu']} ({health_data['cpu_cores']} ᴄᴏʀᴇꜱ)\n"
                    f"🧠 **ʀᴀᴍ:** {health_data['ram']} ({health_data['ram_used']}/{health_data['ram_total']})\n"
                    f"💾 **ᴅɪꜱᴋ:** {health_data['disk']} ({health_data['disk_used']}/{health_data['disk_total']})\n"
                    f"⏱️ **ᴜᴘᴛɪᴍᴇ:** {health_data['uptime']}\n"
                    f"📮 **ᴘʀᴏᴊᴇᴄᴛꜱ:** {len(project_owners)}\n"
                    f"💚 **ʀᴜɴɴɪɴɗ:** {len([p for p in running_processes.values() if p.poll() is None])}\n"
                    f"🛡️ **ʀᴇᴄᴏᴠᴇʀʏ:** {'ᴏɴ' if recovery_enabled else 'ᴏꜰꜰ'}\n"
                    f"📺 **ʟɪᴠᴇ ʟᴏɢꜱ:** {'ᴏɴ' if live_logs_enabled else 'ᴏꜰꜰ'}\n"
                    "━━━━━━━━━━━━━━━━━━━━━\n"
                    "✅ **ꜱʏꜱᴛᴇᴍ ɪꜱ ʜᴇᴀʟᴛʜʏ**"
                )
            elif health_data["status"] == "basic":
                msg_text = (
                    "🏩 **ꜱʏꜱᴛᴇᴍ ʜᴇᴀʟᴛʜ** (ʙᴀꜱɪᴄ)\n"
                    "━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🖥️ **ᴘʟᴀᴛꜰᴏʀᴍ:** {health_data['platform']}\n"
                    f"⚙️ **ᴍᴀᴄʜɪɴᴇ:** {health_data['machine']}\n"
                    f"🔧 **ᴘʀᴏᴄᴇꜱꜱᴏʀ:** {health_data['processor']}\n"
                    f"🐍 **ᴘʏᴛʜᴏɴ:** {health_data['python_version']}\n"
                    f"📮 **ᴘʀᴏᴊᴇᴄᴛꜱ:** {len(project_owners)}\n"
                    f"💚 **ʀᴜɴɴɪɴɗ:** {len([p for p in running_processes.values() if p.poll() is None])}\n"
                    f"🛡️ **ʀᴇᴄᴏᴠᴇʀʏ:** {'ᴏɴ' if recovery_enabled else 'ᴏꜰꜰ'}\n"
                    f"📺 **ʟɪᴠᴇ ʟᴏɢꜱ:** {'ᴏɴ' if live_logs_enabled else 'ᴏꜰꜰ'}\n"
                    "━━━━━━━━━━━━━━━━━━━━━\n"
                    "⚠️ **ɪɴꜱᴛᴀʟʟ `psutil` ꜰᴏʀ ᴅᴇᴛᴀɪʟᴇᴅ ꜱᴛᴀᴛꜱ**"
                )
            else:
                msg_text = (
                    "🏩 **ꜱʏꜱᴛᴇᴍ ʜᴇᴀʟᴛʜ**\n"
                    "━━━━━━━━━━━━━━━━━━━━━\n"
                    "💞 ʜɪ ᴇᴠᴇʀʏᴏɴᴇ ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ🔸ᴢᴇɴᴏɴ-ᴀᴘᴏɴ ʙᴏᴛ ᴀʟʟ ꜱᴇʀᴠᴇʀ 💞\n\n"
                    "ꜰʀᴇᴇ ꜰɪʀᴇ\n\n"
                    f"📮 **ᴘʀᴏᴊᴇᴄᴛꜱ:** {len(project_owners)}\n"
                    f"💚 **ʀᴜɴɴɪɴɗ:** {len([p for p in running_processes.values() if p.poll() is None])}\n"
                    f"🛡️ **ʀᴇᴄᴏᴠᴇʀʏ:** {'ᴏɴ' if recovery_enabled else 'ᴏꜰꜰ'}\n"
                    f"📺 **ʟɪᴠᴇ ʟᴏɢꜱ:** {'ᴏɴ' if live_logs_enabled else 'ᴏꜰꜰ'}"
                )
            
            await msg.edit_text(msg_text, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"ꜱʏꜱᴛᴇᴍ ʜᴇᴀʟᴛʜ ᴇʀʀᴏʀ: {e}")
            await msg.edit_text(
                "🏩 **ꜱʏꜱᴛᴇᴍ ʜᴇᴀʟᴛʜ**\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "💞 **ᴜɴᴀʙʟᴇ ᴛᴏ ꜰᴇᴛᴄʜ ꜱʏꜱᴛᴇᴍ ɪɴꜰᴏ**\n"
                f"📮 **ᴘʀᴏᴊᴇᴄᴛꜱ:** {len(project_owners)}\n"
                f"💚 **ʀᴜɴɴɪɴɗ:** {len([p for p in running_processes.values() if p.poll() is None])}\n"
                f"🛡️ **ʀᴇᴄᴏᴠᴇʀʏ:** {'ᴏɴ' if recovery_enabled else 'ᴏꜰꜰ'}\n"
                f"📺 **ʟɪᴠᴇ ʟᴏɢꜱ:** {'ᴏɴ' if live_logs_enabled else 'ᴏꜰꜰ'}",
                parse_mode='Markdown'
            )

    elif text == "🌎 ꜱᴇʀᴠᴇʀ ɪɴꜰᴏ":
        await update.message.reply_text(
            "🌎 **ꜱᴇʀᴠᴇʀ ɪɴꜰᴏ**\n"
            f"🚀 **ᴘᴏʀᴛ:** {PORT}\n"
            f"🛡️ **ᴘʟᴀᴛꜰᴏʀᴍ:** {os.environ.get('PLATFORM', 'ᴜɴᴋɴᴏᴡɴ')}\n"
            f"🔄 **ᴀᴜᴛᴏ-ʀᴇꜱᴛᴀʀᴛ:** {'ᴏɴ' if auto_restart_mode else 'ᴏꜰꜰ'}\n"
            f"🛡️ **ᴀᴜᴛᴏ-ʀᴇᴄᴏᴠᴇʀʏ:** {'ᴏɴ' if recovery_enabled else 'ᴏꜰꜰ'}\n"
            f"📺 **ʟɪᴠᴇ ʟᴏɢꜱ:** {'ᴏɴ' if live_logs_enabled else 'ᴏꜰꜰ'}\n"
            f"📢 **ʀᴇǫᴜɪʀᴇᴅ ᴄʜᴀɴɴᴇʟ:** {'Not Set' if not REQUIRED_CHANNEL else REQUIRED_CHANNEL}",
            parse_mode='Markdown'
        )

    elif text == "📠 ᴄᴏɴᴛᴀᴄᴛ ᴀᴅᴍɪɴ":
        contact_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📠  ᴄᴏɴᴛᴀᴄᴛ ᴏᴡɴᴇʀ", url=f"tg://user?id={PRIMARY_ADMIN_ID}")]
        ])
        
        await update.message.reply_text(
            f"{ADMIN_DISPLAY_NAME}\n"
            f"📠 ᴄᴏɴᴛᴀᴄᴛ ᴏᴡɴᴇʀ",
            reply_markup=contact_keyboard,
            parse_mode='Markdown'
        )

async def handle_docs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not await require_channel_join(update, context):
        return
    
    if bot_locked and not is_admin(user_id):
        return
    
    doc = update.message.document
    if not doc.file_name.endswith('.zip'):
        await update.message.reply_text("❌ **ᴘʟᴇᴀꜱᴇ ꜱᴇɴᴅ ᴀ .ᴢɪᴘ ꜰɪʟᴇ ᴏɴʟʏ!**", parse_mode='Markdown')
        return
    
    # ᴜᴘʟᴏᴀᴅ ʟᴏᴀᴅɪɴɗ ᴀɴɪᴍᴀᴛɪᴏɴ
    msg = await update.message.reply_text(Loading.uploading()[0])
    for frame in Loading.uploading()[1:]:
        await asyncio.sleep(0.8)
        try:
            msg = await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text=frame)
        except:
            pass
    
    temp_dir = os.path.join(BASE_DIR, f"tmp_{user_id}")
    os.makedirs(temp_dir, exist_ok=True)
    zip_path = os.path.join(temp_dir, doc.file_name)
    
    try:
        file = await doc.get_file()
        await file.download_to_drive(zip_path)
        
        user_upload_state[user_id] = {
            "path": zip_path,
            "u_name": update.effective_user.full_name,
            "original_name": doc.file_name
        }
        
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=msg.message_id,
            text="🖋️ **ɴᴀᴍᴇ ʏᴏᴜʀ ᴘʀᴏᴊᴇᴄᴛ**\n━━━━━━━━━━━━━━━━━━━━━\n💬 **Send a name for your project (ꜱᴘᴀᴄᴇ ᴀʟʟᴏᴡᴇᴅ):**",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"ᴅᴏᴡɴʟᴏᴀᴅ ᴇʀʀᴏʀ: {e}")
        await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text="❌ **ᴅᴏᴡɴʟᴏᴀᴅ ꜰᴀɪʟᴇᴅ!**", parse_mode='Markdown')
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split('_')
    action, p_name = data[0], "_".join(data[1:])
    user_id = update.effective_user.id

    if query.data == "check_join":
        is_member = await check_channel_membership(user_id, context)
        if is_member:
            await query.edit_message_text("✅ **Verification successful! You can now use the bot. please Again /start**", parse_mode='Markdown')
            await start(update, context)
        else:
            await query.answer("❌ You haven't joined the channel yet!", show_alert=True)
        return

    if action == "run":
        if p_name in running_processes and running_processes[p_name].poll() is None:
            await query.edit_message_text(f"⚠️ **`{p_name}` ɪꜱ ᴀʟʀᴇᴀᴅʏ ʀᴜɴɴɪɴɗ!**", parse_mode='Markdown')
            return
            
        folder = os.path.join(BASE_DIR, p_name)
        main_file = os.path.join(folder, "main.py")
        
        if os.path.exists(main_file):
            try:
                # ʀᴜɴ ʟᴏᴀᴅɪɴɗ ᴀɴɪᴍᴀᴛɪᴏɴ
                msg = await query.edit_message_text(Loading.executing()[0])
                for frame in Loading.executing()[1:]:
                    await asyncio.sleep(0.4)
                    try:
                        msg = await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text=frame)
                    except:
                        pass
                
                # ʀᴜɴ ᴘʀᴏᴄᴇꜱꜱ
                proc = subprocess.Popen(
                    [sys.executable, "-u", main_file],
                    cwd=folder,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1
                )
                running_processes[p_name] = proc
                
                if live_logs_enabled:
                    log_streamer.start_stream(p_name, proc)
                
                # ᴀᴜᴛᴏ-ʀᴇꜱᴛᴀʀᴛ ᴍᴏɴɪᴛᴏʀ
                if auto_restart_mode:
                    asyncio.create_task(monitor_process(p_name, folder))
                
                keyboard = [
                    [
                        InlineKeyboardButton("▶️ ʀᴜɴ", callback_data=f"run_{p_name}"),
                        InlineKeyboardButton("🛑 ꜱᴛᴏᴘ", callback_data=f"stop_{p_name}")
                    ],
                    [
                        InlineKeyboardButton("📺 ᴠɪᴇᴡ ʟɪᴠᴇ ʟᴏɢꜱ", callback_data=f"viewlogs_{p_name}")
                    ],
                    [
                        InlineKeyboardButton("🗑️ ᴅᴇʟᴇᴛᴇ", callback_data=f"del_{p_name}")
                    ]
                ]
                
                await context.bot.edit_message_text(
                    chat_id=update.effective_chat.id,
                    message_id=msg.message_id,
                    text=f"🚀 **`{p_name}` ɪꜱ ɴᴏᴡ ᴏɴʟɪɴᴇ! 💚**\n\n📺 Click **View Live Logs** to see live output.",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='Markdown'
                )
            except Exception as e:
                await query.edit_message_text(f"❌ **ꜰᴀɪʟᴇᴅ ᴛᴏ ꜱᴛᴀʀᴛ:** `{str(e)}`", parse_mode='Markdown')
        else:
            await query.edit_message_text(f"❌ **ᴍᴀɪɴ.ᴘʏ ɴᴏᴛ ꜰᴏᴜɴᴅ!**", parse_mode='Markdown')
    
    elif action == "stop":
        if p_name in running_processes:
            # ʀᴇᴠᴇʀꜱᴇ ᴀɴɪᴍᴀᴛɪᴏɴ ꜰᴏʀ ꜱᴛᴏᴘ
            msg = await query.edit_message_text("🛑 ꜱᴛᴏᴘᴘɪɴɗ: [▰▰▰▰▰▰▰▰▰▰] 100%")
            await asyncio.sleep(0.3)
            await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text="🛑 ꜱᴛᴏᴘᴘɪɴɗ: [▰▰▰▰▰▰▰▰▱▱] 80%")
            await asyncio.sleep(0.3)
            await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text="🛑 ꜱᴛᴏᴘᴘɪɴɗ: [▰▰▰▰▰▰▰▱▱▱] 60%")
            await asyncio.sleep(0.3)
            await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text="🛑 ꜱᴛᴏᴘᴘɪɴɗ: [▰▰▰▰▰▰▱▱▱▱] 40%")
            await asyncio.sleep(0.3)
            
            try:
                log_streamer.stop_stream(p_name)
                
                running_processes[p_name].terminate()
                running_processes[p_name].wait(timeout=5)
            except:
                running_processes[p_name].kill()
            del running_processes[p_name]
            
            for uid, session in list(user_log_sessions.items()):
                if session["project"] == p_name:
                    session["active"] = False
            
            await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text=f"🛑 **`{p_name}` ɪꜱ ɴᴏᴡ ᴏꜰꜰʟɪɴᴇ! 💔**", parse_mode='Markdown')
        else:
            await query.edit_message_text(f"⚠️ **`{p_name}` ᴡᴀꜱ ɴᴏᴛ ʀᴜɴɴɪɴɗ**", parse_mode='Markdown')
    
    elif action == "viewlogs":
        if not live_logs_enabled:
            await query.answer("❌ Live logs are currently turned off!", show_alert=True)
            return
        
        if p_name not in running_processes or running_processes[p_name].poll() is not None:
            await query.answer("❌ This project is not currently running!", show_alert=True)
            return
        
        log_msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="📺 **ɪɴɪᴛɪᴀʟɪᴢɪɴɗ ʟɪᴠᴇ ᴄᴏɴꜱᴏʟᴇ...**",
            parse_mode='Markdown'
        )
        
        success = log_streamer.subscribe(p_name, user_id, update.effective_chat.id, log_msg.message_id)
        
        if success:
            await query.answer("✅ Live logs started!", show_alert=True)
        else:
            await log_msg.edit_text("❌ **Failed to start log stream!**", parse_mode='Markdown')
    
    elif action == "del":
        # ᴅᴇʟᴇᴛᴇ ʟᴏᴀᴅɪɴɗ ᴀɴɪᴍᴀᴛɪᴏɴ
        msg = await query.edit_message_text(Loading.deleting()[0])
        for frame in Loading.deleting()[1:]:
            await asyncio.sleep(0.5)
            try:
                msg = await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text=frame)
            except:
                pass
        
        # ꜱᴛᴏᴘ ɪꜰ ʀᴜɴɴɪɴɗ
        if p_name in running_processes:
            try:
                log_streamer.stop_stream(p_name)
                
                running_processes[p_name].terminate()
                running_processes[p_name].wait(timeout=5)
            except:
                pass
            del running_processes[p_name]
        
        for uid, session in list(user_log_sessions.items()):
            if session["project"] == p_name:
                session["active"] = False
        
        path = os.path.join(BASE_DIR, p_name)
        if os.path.exists(path):
            shutil.rmtree(path)
        if p_name in project_owners:
            del project_owners[p_name]
        
        await context.bot.edit_message_text(chat_id=update.effective_chat.id, message_id=msg.message_id, text=f"🗑️ **`{p_name}` ᴅᴇʟᴇᴛᴇᴅ!**", parse_mode='Markdown')

    elif action == "manage":
        status = "💚 ᴏɴʟɪɴᴇ" if (p_name in running_processes and running_processes[p_name].poll() is None) else "💔 ᴏꜰꜰʟɪɴᴇ"
        
        keyboard = [
            [
                InlineKeyboardButton("▶️ ʀᴜɴ", callback_data=f"run_{p_name}"),
                InlineKeyboardButton("🛑 ꜱᴛᴏᴘ", callback_data=f"stop_{p_name}")
            ],
            [
                InlineKeyboardButton("📺 ᴠɪᴇᴡ ʟɪᴠᴇ ʟᴏɢꜱ", callback_data=f"viewlogs_{p_name}")
            ],
            [
                InlineKeyboardButton("🗑️ ᴅᴇʟᴇᴛᴇ", callback_data=f"del_{p_name}")
            ]
        ]
        
        await query.edit_message_text(
            f"📦 **ᴘʀᴏᴊᴇᴄᴛ:** `{p_name}`\n"
            f"📡 **ꜱᴛᴀᴛᴜꜱ:** {status}\n"
            f"📺 **ʟɪᴠᴇ ʟᴏɢꜱ:** {'ᴀᴠᴀɪʟᴀʙʟᴇ' if live_logs_enabled else 'ᴅɪꜱᴀʙʟᴇᴅ'}", 
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

async def monitor_process(p_name, folder):
    """ᴀᴜᴛᴏ-ʀᴇꜱᴛᴀʀᴛ ᴍᴏɴɪᴛᴏʀ"""
    while auto_restart_mode and p_name in running_processes:
        proc = running_processes.get(p_name)
        if proc and proc.poll() is not None:
            await asyncio.sleep(2)
            main_file = os.path.join(folder, "main.py")
            if os.path.exists(main_file):
                new_proc = subprocess.Popen(
                    [sys.executable, "-u", main_file],
                    cwd=folder,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1
                )
                running_processes[p_name] = new_proc
                
                if live_logs_enabled:
                    log_streamer.stop_stream(p_name)
                    log_streamer.start_stream(p_name, new_proc)
                
                logger.info(f"ᴀᴜᴛᴏ-ʀᴇꜱᴛᴀʀᴛᴇᴅ {p_name}")
        await asyncio.sleep(5)

# --- [ᴀᴜᴛᴏ ʀᴇᴄᴏᴠᴇʀʏ ꜱʏꜱᴛᴇᴍ] ---
class BotRecovery:
    def __init__(self):
        self.running = True
        self.restart_count = 0
        self.max_restarts = 100  # ɪɴꜰɪɴɪᴛᴇ ʟᴏᴏᴘ ᴇꜱꜱᴇɴᴛɪᴀʟʟʏ
        self.crash_log = []
    
    async def start_recovery_monitor(self, application):
        """ᴍᴀɪɴ ʀᴇᴄᴏᴠᴇʀʏ ʟᴏᴏᴘ"""
        while self.running and recovery_enabled:
            try:
                # ᴄʜᴇᴄᴋ ɪꜰ ʙᴏᴛ ɪꜱ ʀᴇꜱᴘᴏɴᴅɪɴɗ
                await self.check_bot_health(application)
                
                # ʀᴇᴄᴏᴠᴇʀ ᴀɴʏ ᴄʀᴀꜱʜᴇᴅ ᴘʀᴏᴊᴇᴄᴛꜱ
                await self.recover_projects()
                
                await asyncio.sleep(10)  # ᴄʜᴇᴄᴋ ᴇᴠᴇʀʏ 10 ꜱᴇᴄᴏɴᴅꜱ
                
            except Exception as e:
                logger.error(f"ʀᴇᴄᴏᴠᴇʀʏ ᴇʀʀᴏʀ: {e}")
                self.crash_log.append({"time": time.time(), "error": str(e)})
                await asyncio.sleep(5)
    
    async def check_bot_health(self, application):
        """ᴄʜᴇᴄᴋ ɪꜰ ʙᴏᴛ ɪꜱ ʀᴜɴɴɪɴɗ ᴄᴏʀʀᴇᴄᴛʟʏ"""
        try:
            # ᴛʀʏ ᴛᴏ ɢᴇᴛ ʙᴏᴛ ɪɴꜰᴏ - ɪꜰ ᴛʜɪꜱ ꜰᴀɪʟꜱ, ʙᴏᴛ ɪꜱ ᴅᴏᴡɴ
            await application.bot.get_me()
        except Exception as e:
            logger.critical(f"ʙᴏᴛ ʜᴇᴀʟᴛʜ ᴄʜᴇᴄᴋ ꜰᴀɪʟᴇᴅ: {e}")
            await self.emergency_restart(application)
    
    async def recover_projects(self):
        """ᴀᴜᴛᴏ-ʀᴇꜱᴛᴀʀᴛ ᴄʀᴀꜱʜᴇᴅ ᴘʀᴏᴊᴇᴄᴛꜱ"""
        for p_name, proc in list(running_processes.items()):
            if proc.poll() is not None:  # ᴘʀᴏᴄᴇꜱꜱ ᴄʀᴀꜱʜᴇᴅ
                if recovery_enabled and p_name in project_owners:
                    logger.info(f"ʀᴇᴄᴏᴠᴇʀɪɴɗ ᴄʀᴀꜱʜᴇᴅ ᴘʀᴏᴊᴇᴄᴛ: {p_name}")
                    folder = project_owners[p_name]["path"]
                    main_file = os.path.join(folder, "main.py")
                    
                    if os.path.exists(main_file):
                        try:
                            log_streamer.stop_stream(p_name)
                            
                            new_proc = subprocess.Popen(
                                [sys.executable, "-u", main_file],
                                cwd=folder,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                text=True,
                                bufsize=1
                            )
                            running_processes[p_name] = new_proc
                            
                            if live_logs_enabled:
                                log_streamer.start_stream(p_name, new_proc)
                            
                            logger.info(f"ᴘʀᴏᴊᴇᴄᴛ {p_name} ʀᴇᴄᴏᴠᴇʀᴇᴅ")
                        except Exception as e:
                            logger.error(f"ꜰᴀɪʟᴇᴅ ᴛᴏ ʀᴇᴄᴏᴠᴇʀ {p_name}: {e}")
    
    async def emergency_restart(self, application):
        """ᴇᴍᴇʀɢᴇɴᴄʏ ʀᴇꜱᴛᴀʀᴛ ᴡʜᴇɴ ʙᴏᴛ ᴄʀᴀꜱʜᴇꜱ"""
        if self.restart_count < self.max_restarts:
            self.restart_count += 1
            logger.critical(f"ᴇᴍᴇʀɢᴇɴᴄʏ ʀᴇꜱᴛᴀʀᴛ #{self.restart_count}")
            
            # ᴡᴀɪᴛ ᴀ ʙɪᴛ ʙᴇꜰᴏʀᴇ ʀᴇꜱᴛᴀʀᴛɪɴɗ
            await asyncio.sleep(5)
            
            try:
                # ꜱᴛᴏᴘ ᴄᴜʀʀᴇɴᴛ ᴀᴘᴘʟɪᴄᴀᴛɪᴏɴ
                await application.stop()
                await asyncio.sleep(2)
                
                # ʀᴇꜱᴛᴀʀᴛ
                await application.start()
                await application.updater.start_polling(drop_pending_updates=True)
                
                logger.info("ᴇᴍᴇʀɢᴇɴᴄʏ ʀᴇꜱᴛᴀʀᴛ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟ")
                
            except Exception as e:
                logger.critical(f"ᴇᴍᴇʀɢᴇɴᴄʏ ʀᴇꜱᴛᴀʀᴛ ꜰᴀɪʟᴇᴅ: {e}")
    
    def stop(self):
        self.running = False

recovery_system = BotRecovery()

# ꜱɪɢɴᴀʟ ʜᴀɴᴅʟᴇʀ ꜰᴏʀ ɢʀᴀᴄᴇꜰᴜʟ ꜱʜᴜᴛᴅᴏᴡɴ
def signal_handler(signum, frame):
    logger.info("ꜱʜᴜᴛᴅᴏᴡɴ ꜱɪɢɴᴀʟ ʀᴇᴄᴇɪᴠᴇᴅ, ꜱᴛᴏᴘᴘɪɴɗ ʀᴇᴄᴏᴠᴇʀʏ...")
    recovery_system.stop()
    for p_name in list(log_streamer.active_streams.keys()):
        log_streamer.stop_stream(p_name)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# --- [ᴍᴀɪɴ] ---
def main():
    # ꜱᴛᴀʀᴛ ᴡᴇʙ ꜱᴇʀᴠᴇʀ ɪɴ ᴛʜʀᴇᴀᴅ
    web_thread = Thread(target=run_web, daemon=True)
    web_thread.start()
    
    # ꜱᴛᴀʀᴛ ʙᴏᴛ
    application = Application.builder().token(TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.Document.ZIP, handle_docs))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_handler(CallbackQueryHandler(button_callback))
    
    logger.info("ʙᴏᴛ ꜱᴛᴀʀᴛᴇᴅ!")
    
    async def post_init(app):
        asyncio.create_task(log_viewer_task(app))
        asyncio.create_task(recovery_system.start_recovery_monitor(app))
    
    application.post_init = post_init
    
    # ᴜꜱᴇ ᴡᴇʙʜᴏᴏᴋ ɪꜰ ᴡᴇʙʜᴏᴏᴋ_ᴜʀʟ ɪꜱ ꜱᴇᴛ, ᴇʟꜱᴇ ᴘᴏʟʟɪɴɗ
    webhook_url = os.environ.get('WEBHOOK_URL')
    if webhook_url:
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            webhook_url=webhook_url
        )
    else:
        application.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
