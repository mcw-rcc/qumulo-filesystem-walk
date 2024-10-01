import argparse
import io
import os

from datetime import datetime

from typing import Sequence

from . import FileInfo, Worker


class ReportOldFiles:
    def __init__(self, in_args: Sequence[str]):
        parser = argparse.ArgumentParser(description="")
        parser.add_argument("--age", help="", dest="age")
        args = parser.parse_args(in_args)
        self.sample_age = 60
        if args.age:
            self.sample_age = float(args.age)

    def every_batch(self, file_list: Sequence[FileInfo], work_obj: Worker) -> None:
        action_count = 0
        of_res = []
        for file_obj in file_list:
            if 'conda_local' in file_obj["path"]:
                continue
            if file_obj["type"] == "FS_FILE_TYPE_DIRECTORY":
            #if file_obj["type"] == "FS_FILE_TYPE_FILE":
                continue
            if 'link' in file_obj["type"].lower():
                continue
            try:
                file_age_atime = datetime.utcnow() - datetime.strptime(file_obj["access_time"][:19], '%Y-%m-%dT%H:%M:%S')
                file_age_chtime = datetime.utcnow() - datetime.strptime(file_obj["change_time"][:19], '%Y-%m-%dT%H:%M:%S')
                file_age_crtime = datetime.utcnow() - datetime.strptime(file_obj["creation_time"][:19], '%Y-%m-%dT%H:%M:%S')
                file_age_mtime = datetime.utcnow() - datetime.strptime(file_obj["modification_time"][:19], '%Y-%m-%dT%H:%M:%S')
                file_age = min(file_age_atime,file_age_chtime,file_age_crtime,file_age_mtime)
                if file_age.days >= self.sample_age:
                    of_res.append("%(path)s" % file_obj)
            except:
                print(file_obj["path"])
                pass

        with work_obj.result_file_lock:
            with io.open(work_obj.LOG_FILE_NAME, "a", encoding="utf8") as f:
                for line in of_res:
                    f.write(line + "\n")
            work_obj.action_count.value += action_count

    @staticmethod
    def minimum_queue_length() -> int:
        return 10
    
    @staticmethod
    def work_start(work_obj: Worker) -> None:
        if os.path.exists(work_obj.LOG_FILE_NAME):
            os.remove(work_obj.LOG_FILE_NAME)

    @staticmethod
    def work_done(_work_obj: Worker) -> None:
        return