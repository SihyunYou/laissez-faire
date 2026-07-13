import requests
import time
import os

REMOTE_URL = "https://pastebin.com/raw/iGrXJ3yP"
LOCAL_FILE = "command.txt"
CHECK_INTERVAL = 5  # 초 단위

def fetch_remote_text():
    try:
        response = requests.get(REMOTE_URL)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print("🚫 에러:", e)
        return None

def load_local_text():
    if not os.path.exists(LOCAL_FILE):
        return ""
    with open(LOCAL_FILE, "r", encoding="utf-8") as f:
        return f.read()

def save_local_text(text):
    with open(LOCAL_FILE, "w", encoding="utf-8") as f:
        f.write(text)

def sync_loop():
    print("📡 Pastebin 텍스트 동기화 시작 (Ctrl+C로 종료)...")
    while True:
        remote_text = fetch_remote_text()
        if remote_text is None:
            print("⚠️ 텍스트 불러오기 실패, 재시도 중...")
        else:
            local_text = load_local_text()
            if remote_text != local_text:
                print("🔄 변경 감지! 로컬 파일 갱신 중...")
                save_local_text(remote_text)
            else:
                print("✅ 변경 없음.")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    sync_loop()
