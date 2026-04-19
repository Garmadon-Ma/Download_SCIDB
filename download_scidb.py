import requests
import os
import time
from urllib.parse import urlparse, parse_qs
from collections import defaultdict

# Config
USERNAME = ""
TOKEN = ""

HEADERS = {
    "User-Agent": "",
    "Authorization": TOKEN,
    "username": USERNAME,
    "traceid": USERNAME,
}

SAVE_DIR = 'E:/DRFF-R2-dataset2'

def parse_url(url):
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    return {
        'url': url,
        'file_id': params.get('fileId', [''])[0],
        'path': params.get('path', [''])[0],
        'file_name': params.get('fileName', [''])[0],
    }

def is_dataset2(info):
    return 'dataset2-drone_mixed' in info['path']

def get_subfolder(info):
    path = info['path']
    parts = path.split('/')
    if len(parts) >= 5:
        return parts[4]
    return 'root'

def download_file(info, save_dir):
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, info['file_name'])

    if os.path.exists(save_path):
        return True, os.path.getsize(save_path)

    try:
        response = requests.get(info['url'], headers=HEADERS, timeout=120, stream=True)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True, os.path.getsize(save_path)
        else:
            return False, 0
    except Exception:
        return False, 0

def print_progress(current, total, bar_len=30):
    percent = current / total
    filled = int(bar_len * percent)
    bar = '=' * filled + '-' * (bar_len - filled)
    pct = f"{percent*100:.1f}%"
    print(f"\r[{bar}] {pct} ({current}/{total})", end='', flush=True)

# Read URLs from file
with open('urls.txt', 'r', encoding='utf-8') as f:
    lines = [line.strip() for line in f if line.strip()]

print(f"Total URLs: {len(lines)}")

# Parse
all_files = [parse_url(u) for u in lines]
dataset2_files = [f for f in all_files if is_dataset2(f)]
print(f"Dataset2-drone_mixed files: {len(dataset2_files)}\n")

# Group by subfolder
groups = defaultdict(list)
for f in dataset2_files:
    subfolder = get_subfolder(f)
    groups[subfolder].append(f)

# Print subfolder summary
for subfolder, files in groups.items():
    print(f"  {subfolder}: {len(files)} files")

print()

# Download with progress bar
total_files = len(dataset2_files)
success = 0
failed = 0
total_size = 0

for subfolder, files in groups.items():
    safe_name = subfolder.replace('&', '_').replace(' ', '_')
    subdir = os.path.join(SAVE_DIR, safe_name)

    print(f"\n--- {subfolder} ---")

    for i, info in enumerate(files):
        ok, size = download_file(info, subdir)
        if ok:
            success += 1
            total_size += size
            status = "[OK]"
        else:
            failed += 1
            status = "[FAIL]"

        print(f"  {status} {info['file_name']}")
        print_progress(success + failed, total_files)
        time.sleep(0.2)

print()
print()
print("=" * 50)
print(f"Done! Success: {success}, Failed: {failed}")
print(f"Total downloaded: {total_size/1024/1024:.1f} MB")
