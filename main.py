from collections import defaultdict
import os
import shutil
import logging
import re

def setup_logging():
    formatter = logging.Formatter('%(asctime)s | %(levelname)s: %(message)s', "%H:%M:%S")
    log = logging.getLogger('')  ## define your logging name
    log.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    log.addHandler(ch)
    return log

log = setup_logging()

def store_file(in_file, out_path):
    basename = os.path.basename(in_file)
    match = re.match(r"^(.*?)_(\d{8})\.zip$", basename)
    if not match:
        log.error(f"Invalid file format: {basename}")
        return

    channel_name, date_str = match.groups()
    year, month, day = date_str[:4], date_str[4:6], date_str[6:8]

    channel_paths = defaultdict(
        lambda: f"{out_path}/unknown/{year}/{month}/{day}",
        {
            "okezone": f"{out_path}/okezone/{year}/{month}/{day}",  ## example subdir
            "trijaya": f"{out_path}/trijaya/{year}/{month}/{day}",  ## example subdir
            ## next
        }
    )
    channel_path = channel_paths[channel_name.lower()]
    os.makedirs(channel_path, exist_ok=True)

    try:
        temp_file = os.path.join(channel_path, f".tmp_{basename}")
        shutil.copy2(in_file, temp_file)
        os.rename(temp_file, os.path.join(channel_path, basename))
        os.remove(in_file)
        log.info(f"Success moving ({in_file}) to ({channel_path})")
    except shutil.Error as e:
        log.error(f"Failed moving {in_file} to {channel_path}: {str(e)}")
    except Exception as e:
        log.error(f"Unexpected error while moving {in_file}: {str(e)}")

def check_inpath(in_path, out_path, pattern):
    files = [file for file in os.listdir(in_path) if file.endswith(tuple(pattern))]
    
    for file in files:
        in_file = os.path.join(in_path, file)
        log.info(f"Scanning file: {file}")
        store_file(in_file, out_path)
    
    log.info("File checking done. Exiting script.")

if __name__ == "__main__":
    in_path = ""   ## define your path
    out_path = ""   ## define your path
    pattern = [""]   ## define your pattern, example .zip 
    
    os.makedirs(in_path, exist_ok=True)
    os.makedirs(out_path, exist_ok=True)
    
    check_inpath(in_path, out_path, pattern)
