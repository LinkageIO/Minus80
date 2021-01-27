"""Utilities and tools for minus80."""

import gzip
import bz2
from subprocess import check_call


def install_apsw(method="pip", version="3.27.2", tag="-r1"):  # pragma: no cover
    if method == "pip":
        print("Installing apsw from GitHub using ")
        version = "3.27.2"
        tag = "-r1"
        check_call(
            f"""\
            pip install  \
            https://github.com/rogerbinns/apsw/releases/download/{version}{tag}/apsw-{version}{tag}.zip \
            --global-option=fetch \
            --global-option=--version \
            --global-option={version} \
            --global-option=--all \
            --global-option=build  \
            --global-option=--enable=rtree \
        """.split()
        )
    else:
        raise ValueError(f"{method} not supported to install apsw")


def human_sizeof(num, suffix="B"):  # pragma: no cover
    # Courtesy of:
    # https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


class rawFile(object):  # pragma no cover
    def __init__(self, filename):  # pragma no cover
        self.filename = filename
        if filename.endswith(".gz"):  # pragma no cover
            self.handle = gzip.open(filename, "rt")
        elif filename.endswith("bz2"):  # pragma no cover
            self.handle = bz2.open(filename, "rt")
        else:
            self.handle = open(filename, "r")

    def __enter__(self):  # pragma no cover
        return self.handle

    def __exit__(self, type, value, traceback):  # pragma no cover
        self.handle.close()
