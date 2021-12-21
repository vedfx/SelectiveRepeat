import sys
from os import getcwd

workingDir = format(getcwd())


def read_args(file_name):
    args = {}
    with open(file_name) as input:
        for line in input:
            (key, val) = line.strip().split('=')
            args[key] = val
    return args


def print_line():
    print(f"{color.BLACK}{color.UNDERLINE}                                               {color.END}", file=sys.stderr)


def print_star_line():
    print(f"{color.BLACK}{color.BOLD}***********************************************{color.END}", file=sys.stderr)


class color:
    BLACK = '\u001b[30m'
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
