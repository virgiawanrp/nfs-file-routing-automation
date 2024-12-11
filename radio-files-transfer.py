import os
import time
import shutil
import logging
import threading
from collections import defaultdict

formatter = logging.Formatter('%(asctime)s | %(levelname)s: %(message)s', "%H:%M:%S")

log = logging.getLogger('Radio-Archive')
log.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
log.addHandler(ch)

# fh = logging.FileHandler('/home/bino/logs/radio-files-transfer/radio-files-transfer.log')
# fh.setLevel(logging.DEBUG)
# fh.setFormatter(formatter)
# log.addHandler(fh)

file_lock = threading.Lock()
done_event = threading.Event()


def create_directory(path):
    try:
        os.makedirs(path, exist_ok=True)
        log.info(f"Directory created or already exists: {path}")
    except PermissionError:
        log.error(f"Permission denied creating directory: {path}")
    except Exception as e:
        log.error(f"Failed to create directory {path}: {e}")


def check_inpath(in_path, out_path, pattern):
    while True:
        files = [file for file in os.listdir(in_path) if file.endswith(tuple(pattern))]

        for file in files:
            in_file = os.path.join(in_path, file)
            out_file = os.path.join(out_path, file)

            log.info(f"Scanning size of file: {file}")
            check_size1 = os.path.getsize(in_file)
            retries = 0
            while retries < 3:
                time.sleep(0.5)
                check_size2 = os.path.getsize(in_file)
                if check_size1 == check_size2:
                    with file_lock:
                        try:
                            shutil.move(in_file, out_file)
                            log.info(f"Success moving ({file}) to ({out_path})")
                        except Exception as e:
                            log.error(f"Failed moving file {file}: {e}")
                    break
                else:
                    retries += 1
                    check_size1 = check_size2
                    log.warning(f"File '{file}' is still being written. Retry {retries}/3")
            else:
                log.error(f"Failed to move file '{file}' after 3 retries.")
                
        time.sleep(1)
        
        done_event.set()


def store_file(out_path, pattern):
    while True:
        files = [file for file in sorted(os.listdir(out_path)) if file.endswith(tuple(pattern))]

        if not files:
            time.sleep(1)
            continue

        for file in files:
            basename = file[:-13].lower()
            year, month, day = file[-12:-8], file[-8:-6], file[-6:-4]

            channel_paths = defaultdict(
                lambda: f"{out_path}/unknown",
                {
                    "okezone": f"{out_path}/okezone/{year}/{month}/{day}",
                    "trijaya": f"{out_path}/trijaya/{year}/{month}/{day}",
                    "brava": f"{out_path}/brava/{year}/{month}/{day}",
                    "female": f"{out_path}/female/{year}/{month}/{day}",
                    "gen": f"{out_path}/gen/{year}/{month}/{day}",
                    "hardrock": f"{out_path}/hardrock/{year}/{month}/{day}",
                    "jak": f"{out_path}/jak/{year}/{month}/{day}",
                    "most": f"{out_path}/most/{year}/{month}/{day}",
                    "motion": f"{out_path}/motion/{year}/{month}/{day}",
                    "pas": f"{out_path}/pas/{year}/{month}/{day}",
                    "prambors": f"{out_path}/prambors/{year}/{month}/{day}",
                    "prfm": f"{out_path}/prfm/{year}/{month}/{day}",
                    "rripro1_bandung": f"{out_path}/rripro1_bandung/{year}/{month}/{day}",
                    "rripro1_banten": f"{out_path}/rripro1_banten/{year}/{month}/{day}",
                    "rripro1_jakarta": f"{out_path}/rripro1_jakarta/{year}/{month}/{day}",
                    "rripro2_jakarta": f"{out_path}/rripro2_jakarta/{year}/{month}/{day}",
                    "rripro3": f"{out_path}/rripro3/{year}/{month}/{day}",
                    "elshinta": f"{out_path}/elshinta/{year}/{month}/{day}",
                    "sonora": f"{out_path}/sonora/{year}/{month}/{day}",
                    "smart": f"{out_path}/smart/{year}/{month}/{day}",
                    "trax": f"{out_path}/trax/{year}/{month}/{day}",
                    "yesradio": f"{out_path}/yesradio/{year}/{month}/{day}",
                }
            )

            channel_path = channel_paths[basename]
            create_directory(channel_path)


            with file_lock:
                try:
                    shutil.move(
                        os.path.join(out_path, file), os.path.join(channel_path, file)
                    )
                    log.info(f"Success moving ({file}) to ({channel_path})")
                except shutil.Error as e:
                    log.error(f'Failed moving {file} to {channel_path} : {str(e)}')
                except Exception as e:
                    log.error(f'Unexpected error while moving {file}: {str(e)}')


if __name__ == "__main__":
    in_path = "/mnt/radiouploads"
    out_path = "/mnt/radio"
    pattern = [".zip"]

    create_directory(in_path)
    create_directory(out_path)

    thread1 = threading.Thread(target=check_inpath, args=(in_path, out_path, pattern))
    thread2 = threading.Thread(target=store_file, args=(out_path, pattern))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

