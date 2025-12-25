#!/usr/bin/env python3
import concurrent.futures
import filecmp
import logging
import os
import re
import shutil
import sys
import time

from tqdm import tqdm

from src.date import Date
from src.exif import Exif

logger = logging.getLogger('phockup')
ignored_files = ('.DS_Store', 'Thumbs.db')


class Phockup:
    DEFAULT_DIR_FORMAT = ['%Y', '%m', '%d']
    DEFAULT_NO_DATE_DIRECTORY = "unknown"
    MEDIA_EXTENSIONS = {
        '.jpg', '.jpeg', '.jpe', '.png', '.gif', '.bmp', '.tif', '.tiff',
        '.heic', '.heif', '.avif', '.jxl', '.dng', '.cr2', '.cr3', '.nef',
        '.arw', '.orf', '.raf', '.rw2', '.srw', '.pef', '.mp4', '.mov',
        '.m4v', '.avi', '.mts', '.m2ts', '.3gp', '.mkv'
    }
    PAIRED_VIDEO_MAP = {
        '.heic': ('.mov', '.mp4'),
        '.heif': ('.mov', '.mp4'),
        '.jpg': ('.mov', '.mp4'),
        '.jpeg': ('.mov', '.mp4'),
        '.dng': ('.mov', '.mp4'),
        '.raf': ('.mov', '.mp4'),
    }
    SIDECAR_EXTENSIONS = ('.xmp', '.aae', '.json')

    def __init__(self, input_dir, output_dir, **args):
        start_time = time.time()
        self.files_processed = 0
        self.duplicates_found = 0
        self.unknown_found = 0
        self.files_moved = 0
        self.files_copied = 0

        input_dir = os.path.abspath(os.path.expanduser(input_dir))
        output_dir = os.path.abspath(os.path.expanduser(output_dir))

        if input_dir.endswith(os.path.sep):
            input_dir = input_dir[:-1]
        if output_dir.endswith(os.path.sep):
            output_dir = output_dir[:-1]

        self.input_dir = input_dir
        self.output_dir = output_dir
        self.output_prefix = args.get('output_prefix' or None)
        self.output_suffix = args.get('output_suffix' or '')
        self.no_date_dir = args.get('no_date_dir') or Phockup.DEFAULT_NO_DATE_DIRECTORY
        self.dir_format = args.get('dir_format') or os.path.sep.join(Phockup.DEFAULT_DIR_FORMAT)
        self.move = args.get('move', False)
        self.link = args.get('link', False)
        self.original_filenames = args.get('original_filenames', False)
        self.date_regex = args.get('date_regex', None)
        self.timestamp = args.get('timestamp', False)
        self.date_field = args.get('date_field', False)
        self.skip_unknown = args.get("skip_unknown", False)
        self.movedel = bool(args.get("movedel", False))
        self.rmdirs = bool(args.get("rmdirs", False))
        self.dry_run = args.get('dry_run', False)
        self.progress = args.get('progress', False)
        self.max_depth = args.get('max_depth', -1)
        # default to concurrency of one to retain existing behavior
        self.max_concurrency = args.get("max_concurrency", 1)

        self.from_date = args.get("from_date", None)
        self.to_date = args.get("to_date", None)
        if self.from_date is not None:
            self.from_date = Date.strptime(f"{self.from_date} 00:00:00", "%Y-%m-%d %H:%M:%S")
        if self.to_date is not None:
            self.to_date = Date.strptime(f"{self.to_date} 23:59:59", "%Y-%m-%d %H:%M:%S")

        if self.max_concurrency > 1:
            logger.info(f"Using {self.max_concurrency} workers to process files.")

        self.stop_depth = self.input_dir.count(os.sep) + self.max_depth \
            if self.max_depth > -1 else sys.maxsize
        self.file_type = args.get('file_type', None)

        if self.dry_run:
            logger.warning("Dry-run phockup (does a trial run with no permanent changes)...")

        self.check_directories()
        self.pbar = tqdm(
            desc=f"Progressing: '{self.input_dir}' ",
            total=None,
            unit="file",
            position=0,
            leave=True,
            ascii=(sys.platform == 'win32'),
        ) if self.progress else None
        self.walk_directory()

        if self.move and self.rmdirs:
            self.rm_subdirs()

        run_time = time.time() - start_time
        if self.files_processed and run_time:
            self.print_action_report(run_time)

    def print_action_report(self, run_time):
        logger.info(f"Processed {self.files_processed} files in {run_time:.2f} seconds. Average Throughput: {self.files_processed/run_time:.2f} files/second")
        if self.unknown_found:
            logger.info(f"Found {self.unknown_found} files without EXIF date data.")
        if self.duplicates_found:
            logger.info(f"Found {self.duplicates_found} duplicate files.")
        if self.files_copied:
            if self.dry_run:
                logger.info(f"Would have copied {self.files_copied} files.")
            else:
                logger.info(f"Copied {self.files_copied} files.")
        if self.files_moved:
            if self.dry_run:
                logger.info(f"Would have moved {self.files_moved} files.")
            else:
                logger.info(f"Moved {self.files_moved} files.")

    def check_directories(self):
        """
        Check if input and output directories exist.
        If input does not exist it exits the process.
        If output does not exist it tries to create it or exit with error.
        """

        if (os.path.isfile(self.input_dir) or os.path.splitext(self.input_dir)[1]) and not os.path.isdir(self.input_dir):
            raise RuntimeError(f"Input directory '{self.input_dir}' is not a directory")
        if not os.path.exists(self.input_dir):
            raise RuntimeError(f"Input directory '{self.input_dir}' does not exist")
        if not os.path.isdir(self.input_dir):
            raise RuntimeError(f"Input directory '{self.input_dir}' is not a directory")
        if not os.path.exists(self.output_dir):
            logger.warning(f"Output directory '{self.output_dir}' does not exist, creating now")
            try:
                if not self.dry_run:
                    os.makedirs(self.output_dir)
            except OSError:
                raise OSError(f"Cannot create output '{self.output_dir}' directory. No write access!")

    def walk_directory(self):
        """
        Walk input directory recursively and call process_file for each file
        except the ignored ones.
        """
        file_iterator = self._iter_files()

        if self.max_concurrency > 1:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.max_concurrency) as executor:
                try:
                    for _ in executor.map(self.process_file, file_iterator):
                        pass
                except KeyboardInterrupt:
                    logger.warning(
                        f"Received interrupt. Shutting down {self.max_concurrency} workers...")
                    executor.shutdown(wait=False)
                    return
        else:
            try:
                for file_path in file_iterator:
                    self.process_file(file_path)
            except KeyboardInterrupt:
                logger.warning("Received interrupt. Shutting down...")
                return

    def _iter_files(self):
        for root, dirnames, files in os.walk(self.input_dir):
            files.sort()
            for filename in files:
                if filename in ignored_files:
                    continue
                yield os.path.join(root, filename)
            if root.count(os.sep) >= self.stop_depth:
                del dirnames[:]

    def rm_subdirs(self):
        def _get_depth(sub_path):
            return sub_path.count(os.sep) - self.input_dir.count(os.sep)

        for root, dirs, files in os.walk(self.input_dir, topdown=False):
            # Traverse the tree bottom-up
            if _get_depth(root) > self.stop_depth:
                continue
            for name in dirs:
                dir_path = os.path.join(root, name)
                if _get_depth(dir_path) > self.stop_depth:
                    continue
                try:
                    os.rmdir(dir_path)  # Try to remove the dir
                    logger.info(f"Deleted empty directory: {dir_path}")
                except OSError as e:
                    logger.info(f"{e.strerror} - {dir_path} not deleted.")

    def get_file_count(self):
        file_count = 0
        for root, dirnames, files in os.walk(self.input_dir):
            file_count += len(files)
            if root.count(os.sep) >= self.stop_depth:
                del dirnames[:]
        return file_count

    def get_file_type(self, mimetype):
        """
        Check if given file_type is image or video
        Return None if other
        Use mimetype to determine if the file is an image or video.
        """
        patternImage = re.compile('^(image/.+|application/vnd.adobe.photoshop)$')
        if patternImage.match(mimetype):
            return 'image'

        patternVideo = re.compile('^(video/.*)$')
        if patternVideo.match(mimetype):
            return 'video'
        return None

    def get_output_dir(self, date):
        """
        Generate output directory path based on the extracted date and
        formatted using dir_format.
        If date is missing from the exifdata the file is going to "unknown"
        directory unless user included a regex from filename or uses timestamp.
        """
        try:
            path = [self.output_dir,
                    self.output_prefix,
                    date['date'].date().strftime(self.dir_format),
                    self.output_suffix]
        except (TypeError, ValueError):
            path = [self.output_dir,
                    self.output_prefix,
                    self.no_date_dir,
                    self.output_suffix]
        # Remove any None values that made it in the path
        path = [p for p in path if p is not None]
        fullpath = os.path.normpath(os.path.sep.join(path))

        if not os.path.isdir(fullpath) and not self.dry_run:
            os.makedirs(fullpath, exist_ok=True)

        return fullpath

    def get_file_name(self, original_filename, date):
        """
        Generate file name based on exif data unless it is missing or
        original filenames are required. Then use original file name
        """
        if self.original_filenames:
            return os.path.basename(original_filename)

        try:
            filename = [
                f'{date["date"].year :04d}',
                f'{date["date"].month :02d}',
                f'{date["date"].day :02d}',
                '-',
                f'{date["date"].hour :02d}',
                f'{date["date"].minute :02d}',
                f'{date["date"].second :02d}',
            ]

            if date['subseconds']:
                filename.append(date['subseconds'])

            return ''.join(filename) + os.path.splitext(original_filename)[1]
        # TODO: Double check if this is correct!
        except TypeError:
            return os.path.basename(original_filename)

    def process_file(self, filename):
        """
        Process the file using the selected strategy
        If file is .xmp skip it so process_xmp method can handle it
        """
        if str.endswith(filename, '.xmp'):
            return None

        progress = f'{filename}'

        output, target_file_name, target_file_path, target_file_type, file_date = self.get_file_name_and_path(filename)
        suffix = 1
        target_file = target_file_path

        while True:
            if self.file_type is not None \
                    and self.file_type != target_file_type:
                progress = f"{progress} => skipped, file is '{target_file_type}' \
but looking for '{self.file_type}'"
                logger.info(progress)
                break

            date_unknown = file_date is None or output.endswith(self.no_date_dir)
            if self.skip_unknown and output.endswith(self.no_date_dir):
                # Skip files that didn't generate a path from EXIF data
                progress = f"{progress} => skipped, unknown date EXIF information for '{target_file_name}'"
                self.unknown_found += 1
                if self.progress:
                    self.pbar.write(progress)
                logger.info(progress)
                break

            if not date_unknown:
                skip = False
                if type(file_date) is dict:
                    file_date = file_date["date"]
                if self.from_date is not None and file_date < self.from_date:
                    progress = f"{progress} => {filename} skipped: date {file_date} is older than --from-date {self.from_date}"
                    skip = True
                if self.to_date is not None and file_date > self.to_date:
                    progress = f"{progress} => {filename} skipped: date {file_date} is newer than --to-date {self.to_date}"
                    skip = True
                if skip:
                    if self.progress:
                        self.pbar.write(progress)
                    logger.info(progress)
                    break

            if os.path.isfile(target_file):
                try:
                    source_stats = os.stat(filename)
                    target_stats = os.stat(target_file)
                except FileNotFoundError:
                    progress = f'{progress} => skipped, no such file or directory'
                    if self.progress:
                        self.pbar.write(progress)
                    logger.warning(progress)
                    break

                if source_stats.st_size == target_stats.st_size and (
                        source_stats.st_mtime == target_stats.st_mtime or
                        filecmp.cmp(filename, target_file, shallow=False)):
                    if self.movedel and self.move and self.skip_unknown:
                        if not self.dry_run:
                            os.remove(filename)
                        progress = f'{progress} => deleted, duplicated file {target_file}'
                    else:
                        progress = f'{progress} => skipped, duplicated file {target_file}'
                    self.duplicates_found += 1
                    if self.progress:
                        self.pbar.write(progress)
                    logger.info(progress)
                    break
            else:
                if self.move:
                    try:
                        self.files_moved += 1
                        if not self.dry_run:
                            shutil.move(filename, target_file)
                    except FileNotFoundError:
                        progress = f'{progress} => skipped, no such file or directory'
                        if self.progress:
                            self.pbar.write(progress)
                        logger.warning(progress)
                        break
                elif self.link and not self.dry_run:
                    os.link(filename, target_file)
                else:
                    try:
                        self.files_copied += 1
                        if not self.dry_run:
                            self.copy_file(filename, target_file)
                    except FileNotFoundError:
                        progress = f'{progress} => skipped, no such file or directory'
                        if self.progress:
                            self.pbar.write(progress)
                        logger.warning(progress)
                        break

                progress = f'{progress} => {target_file}'
                if self.progress:
                    self.pbar.write(progress)
                logger.info(progress)

                self.process_sidecars(filename, target_file_name, suffix, output)
                break

            suffix += 1
            target_split = os.path.splitext(target_file_path)
            target_file = f'{target_split[0]}-{suffix}{target_split[1]}'

        self.files_processed += 1
        if self.progress:
            self.pbar.update(1)

    def get_file_name_and_path(self, filename):
        """
        Returns target file name and path
        """
        exif_data = None
        target_file_type = None

        if self.should_inspect_exif(filename):
            exif_data = Exif(filename).data()

        if exif_data and 'MIMEType' in exif_data:
            target_file_type = self.get_file_type(exif_data['MIMEType'])

        date = None
        if target_file_type in ['image', 'video']:
            date = Date(filename).from_exif(exif_data, self.timestamp, self.date_regex,
                                            self.date_field)
            output = self.get_output_dir(date)
            target_file_name = self.get_file_name(filename, date)
            if not self.original_filenames:
                target_file_name = target_file_name.lower()
        else:
            output = self.get_output_dir([])
            target_file_name = os.path.basename(filename)

        target_file_path = os.path.sep.join([output, target_file_name])
        return output, target_file_name, target_file_path, target_file_type, date

    def should_inspect_exif(self, filename):
        ext = os.path.splitext(filename)[1].lower()
        return ext in self.MEDIA_EXTENSIONS

    def process_sidecars(self, original_filename, file_name, suffix, output):
        """
        Process companion metadata (xmp/aae/json) and paired live-photo videos.
        """
        suffix_str = f'-{suffix}' if suffix > 1 else ''
        base_no_ext = os.path.splitext(original_filename)[0]
        dest_base_no_ext = os.path.splitext(file_name)[0]

        # traditional XMP placements
        xmp_candidates = {
            original_filename + '.xmp': f'{file_name}{suffix_str}.xmp',
            f'{base_no_ext}.xmp': f'{dest_base_no_ext}{suffix_str}.xmp',
        }

        for original, target in xmp_candidates.items():
            if os.path.isfile(original):
                self._transfer_companion(original, os.path.sep.join([output, target]))

        # additional sidecars (Apple AAE, Google JSON, etc.)
        for ext in self.SIDECAR_EXTENSIONS:
            original = f'{base_no_ext}{ext}'
            target = f'{dest_base_no_ext}{suffix_str}{ext}'
            if os.path.isfile(original):
                self._transfer_companion(original, os.path.sep.join([output, target]))

        # paired live-photo style video assets
        ext = os.path.splitext(original_filename)[1].lower()
        for companion_ext in self.PAIRED_VIDEO_MAP.get(ext, ()):
            companion_file = f'{base_no_ext}{companion_ext}'
            if os.path.isfile(companion_file):
                target = f'{dest_base_no_ext}{suffix_str}{companion_ext}'
                self._transfer_companion(companion_file, os.path.sep.join([output, target]))

    def _transfer_companion(self, original, target_path):
        logger.info(f'{original} => {target_path}')
        if self.dry_run:
            return
        if os.path.exists(target_path):
            logger.info(f"Skipping companion copy, target exists: {target_path}")
            return
        if self.move:
            shutil.move(original, target_path)
        elif self.link:
            os.link(original, target_path)
        else:
            self.copy_file(original, target_path)

    def copy_file(self, source, destination, buffer_size=8 * 1024 * 1024):
        """Copy file content and metadata with a larger buffer for big media files."""
        with open(source, 'rb') as src, open(destination, 'wb') as dst:
            shutil.copyfileobj(src, dst, length=buffer_size)
        shutil.copystat(source, destination, follow_symlinks=True)
