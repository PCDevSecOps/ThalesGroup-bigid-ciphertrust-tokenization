import datetime
import inspect
import os


def create_log_file(logfile_name = "log.txt"):
    if not os.path.exists(logfile_name):
        with open(logfile_name, "w") as _: 
            pass
    
    status = os.stat(logfile_name)
    if oct(status.st_mode)[-3:] != "666":
        os.chmod(logfile_name, 0o666)


def write_to_file(mode: str, message: str, filename: str):
    """
    Formats the log string and writes it to the log.txt file.
    """
    log_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    now = datetime.datetime.now()
    now.strftime("%Y/%m/%d %H:%M:%S.%f")

    fname = f" [{filename}]" if filename else ""
    formatted = f"[{now}] {mode}{fname} - {message}\n"

    with open(os.path.join(log_path, "log.txt"), "a", encoding="utf-8") as f:
        f.write(formatted)


def get_caller_fname():
    """
    Gets the name of the file that called the Log methods
    """
    caller_frame = inspect.stack()[2]
    caller_filename_full = caller_frame.filename
    return os.path.basename(caller_filename_full)


class Log:

    @staticmethod
    def info(message: str):
        write_to_file("INFO ", message, get_caller_fname())

    @staticmethod
    def warn(message: str):
        write_to_file("WARN ", message, get_caller_fname())

    @staticmethod
    def error(message: str):
        write_to_file("ERROR", message, get_caller_fname())
