import json
import os
import shlex
import subprocess
import sys
import threading
from subprocess import CalledProcessError, check_output

# Shared persistent exiftool session to amortize startup cost
_persistent_session = None
_session_lock = threading.Lock()


class _ExiftoolSession:
    """Lightweight wrapper around a single exiftool -stay_open process."""

    def __init__(self):
        self._lock = threading.Lock()
        self._token = 0
        try:
            self._process = subprocess.Popen(
                self._base_command(),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except OSError:
            self._process = None

    @staticmethod
    def _base_command():
        cmd = ['exiftool', '-stay_open', 'True', '-@', '-']
        # No shell, so no quoting required; exiftool handles newlines as separators
        return cmd

    def _build_payload(self, filename, token):
        filename = os.path.abspath(filename)
        return "\n".join([
            "-fast2",
            "-n",
            "-time:all",
            "-mimetype",
            "-j",
            self._quote_filename(filename),
            "-echo3",
            token,
            "-execute\n",
        ])

    @staticmethod
    def _quote_filename(filename):
        filename = os.path.abspath(filename)
        if sys.platform == 'win32':
            return f'"{filename}"'
        return shlex.quote(filename)

    def fetch(self, filename):
        if not self._process or self._process.stdin.closed:
            return None

        with self._lock:
            self._token += 1
            token = f"__PHOCKUP_END__{self._token}__"
            try:
                self._process.stdin.write(self._build_payload(filename, token))
                self._process.stdin.flush()
            except (BrokenPipeError, OSError):
                return None

            chunks = []
            for line in self._process.stdout:
                if line.strip() == token:
                    break
                chunks.append(line)

        data = ''.join(chunks).strip()
        if not data:
            return None

        try:
            return json.loads(data)[0]
        except (json.JSONDecodeError, IndexError):
            return None


def _get_persistent_session():
    global _persistent_session
    if _persistent_session is not None:
        return _persistent_session
    with _session_lock:
        if _persistent_session is None:
            _persistent_session = _ExiftoolSession()
    return _persistent_session


class Exif(object):
    def __init__(self, filename):
        self.filename = os.path.abspath(filename)

    def data(self):
        # Try persistent session first to avoid spawning per-file exiftool
        exif = self._data_from_session()
        if exif is not None:
            return exif
        return self._data_fallback()

    def _data_from_session(self):
        session = _get_persistent_session()
        if session is None:
            return None
        return session.fetch(self.filename)

    def _data_fallback(self):
        try:
            exif_command = self.get_exif_command(self.filename)
            if threading.current_thread() is threading.main_thread():
                data = check_output(exif_command, shell=True).decode('UTF-8')
            else:
                # Swallow stderr in the case that multiple threads are executing
                data = check_output(exif_command, shell=True, stderr=subprocess.DEVNULL).decode('UTF-8')
            exif = json.loads(data)[0]
        except (CalledProcessError, UnicodeDecodeError, json.JSONDecodeError):
            return None

        return exif

    @staticmethod
    def get_exif_command(filename):
        # Handle all platform variations
        abspath = os.path.abspath(filename)
        quoted = shlex.quote(abspath)
        if sys.platform == 'win32':
            quoted = f'"{abspath}"'
        # Fast, numeric output reduces parsing overhead
        return f'exiftool -fast2 -n -time:all -mimetype -j {quoted}'
