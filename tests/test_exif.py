#!/usr/bin/env python3
import base64
import os
import subprocess
from pathlib import Path
from subprocess import CalledProcessError

import pytest

from src.exif import Exif

os.chdir(os.path.dirname(__file__))

pytestmark = pytest.mark.needs_exiftool

SAMPLE_JPEG = base64.b64decode(
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAP//////////////////////////////////////////////////////////////////////////////////////2wBDAf//////////////////////////////////////////////////////////////////////////////////////wAARCABkAGQDAREAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAb/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/8QAFQEBAQAAAAAAAAAAAAAAAAAAAwT/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIRAxEAPwCfAAf/2Q=="
)


def _prepare_files():
    base = Path("input")
    base.mkdir(exist_ok=True)
    targets = {
        "exif.jpg": "2017:01:01 01:01:01",
        "!#$%'+-.^_`~.jpg": "2017:01:01 01:01:01",
        "phockup's exif test.jpg": "2017:10:06 01:01:01",
    }
    for name, date in targets.items():
        path = base / name
        path.write_bytes(SAMPLE_JPEG + name.encode())
        subprocess.run(
            ["exiftool", "-overwrite_original", f"-CreateDate={date}", str(path)],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )


def test_exif_reads_valid_file():
    _prepare_files()
    exif = Exif("input/exif.jpg")
    assert exif.data()['CreateDate'] == '2017:01:01 01:01:01'


def test_exif_reads_files_with_illegal_characters():
    _prepare_files()
    exif = Exif("input/!#$%'+-.^_`~.jpg")
    assert exif.data()['CreateDate'] == '2017:01:01 01:01:01'


def test_exif_reads_file_with_spaces_punctuation():
    _prepare_files()
    exif = Exif("input/phockup's exif test.jpg")
    assert exif.data()['CreateDate'] == '2017:10:06 01:01:01'


def test_exif_handles_exception(mocker):
    mocker.patch('subprocess.check_output',
                 side_effect=CalledProcessError(2, 'cmd'))
    exif = Exif("not-existing.jpg")
    assert exif.data() is None
