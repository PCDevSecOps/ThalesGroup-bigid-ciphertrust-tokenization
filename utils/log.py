import datetime
import os
import inspect

debug = True


def write_to_file(mode: str, message: str, filename: str):
    log_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    now = datetime.datetime.now()
    now.strftime("%Y/%m/%d %H:%M:%S.%f")

    fname = f" [{filename}]" if filename else ""
    formatted = f"[{now}] {mode}{fname} - {message}\n"

    if debug:
        print(formatted)

    with open(os.path.join(log_path, "log.txt"), "a") as f:
        f.write(formatted)


def get_caller_fname():
    # print(inspect.stack()[2])
    caller_frame = inspect.stack()[2]
    caller_filename_full = caller_frame.filename
    return os.path.basename(caller_filename_full)


class Log:

    def info(message: str):
        write_to_file("INFO ", message, get_caller_fname())

    def warn(message: str):
        write_to_file("WARN ", message, get_caller_fname())

    def error(message: str):
        write_to_file("ERROR", message, get_caller_fname())