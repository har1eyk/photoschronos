import os
import re
from datetime import datetime, timedelta


class Date:
    def __init__(self, filename=None):
        self.filename = filename

    @staticmethod
    def _parse_offset(offset_str):
        """Parse timezone offsets like +02:00 or -0700 into a timedelta."""
        if not offset_str:
            return None
        match = re.search(r'([+-])(\d{2}):?(\d{2})', offset_str)
        if not match:
            return None
        sign = 1 if match.group(1) == '+' else -1
        hours = int(match.group(2))
        minutes = int(match.group(3))
        return timedelta(hours=sign * hours, minutes=sign * minutes)

    @staticmethod
    def parse(date: str) -> str:
        date = date.replace('YYYY', '%Y')  # 2017 (year)
        date = date.replace('YY', '%y')  # 17 (year)
        date = date.replace('m', '%b')  # Dec (month)
        date = date.replace('MM', '%m')  # 12 (month)
        date = date.replace('M', '%B')  # December (month)
        date = date.replace('DDD', '%j')  # 123 (day or year)
        date = date.replace('DD', '%d')  # 25 (day)
        date = date.replace('U', '%U')  # Week number (Sunday as the first day)
        date = date.replace('W', '%W')  # Week number (Monday as the first day)
        date = date.replace('\\', os.path.sep)  # path separator
        date = date.replace('/', os.path.sep)  # path separator
        return date

    @staticmethod
    def strptime(date, date_format):
        return datetime.strptime(date, date_format)

    @staticmethod
    def build(date_object):
        return datetime(
            date_object['year'], date_object['month'], date_object['day'],
            date_object['hour'] if date_object.get('hour') else 0,
            date_object['minute'] if date_object.get('minute') else 0,
            date_object['second'] if date_object.get('second') else 0)

    def from_exif(self, exif, timestamp=None, user_regex=None, date_field=None):
        if date_field:
            keys = date_field.split()
        else:
            keys = ['SubSecCreateDate', 'SubSecDateTimeOriginal', 'CreateDate',
                    'DateTimeOriginal', 'MediaCreateDate']

        datestr = None

        for key in keys:
            # Skip 'bad' dates that return integers (-1) or have the format 0000...
            if key in exif and isinstance(exif[key], str) and not exif[key].startswith('0000'):
                datestr = exif[key]
                break

        # sometimes exif data can return all zeros
        # check to see if valid date first
        # sometimes this returns an int
        if datestr and isinstance(datestr, str) and not \
                datestr.startswith('0000'):
            parsed_date = self.from_datestring(datestr)
        else:
            parsed_date = {'date': None, 'subseconds': ''}

        # apply TimeZone if available and offset wasn't already in the datetime string
        if parsed_date.get('date') is not None and parsed_date.get('offset') is None:
            for tz_key in ('OffsetTimeOriginal', 'OffsetTimeDigitized', 'TimeZone'):
                offset = self._parse_offset(exif.get(tz_key)) if exif.get(tz_key) else None
                if offset:
                    parsed_date['date'] = parsed_date['date'] - offset
                    parsed_date['offset'] = offset
                    break

        if parsed_date.get('date') is not None:
            if parsed_date.get('offset') is None:
                parsed_date.pop('offset', None)
            return parsed_date
        else:
            if self.filename:
                return self.from_filename(user_regex, timestamp)
            else:
                parsed_date.pop('offset', None)
                return parsed_date

    @staticmethod
    def from_datestring(datestr) -> dict:
        datestr_parts = datestr.split('.')
        date = datestr_parts[0]
        subseconds = datestr_parts[1] if len(datestr_parts) > 1 else ''

        # Extract timezone offset from either portion, trimming it away from parsed values
        offset = None
        offset_search = r'(.*)([+-]\d{2}:?\d{2})'
        if re.search(offset_search, date) is not None:
            offset_str = re.sub(offset_search, r'\2', date)
            date = re.sub(offset_search, r'\1', date)
            offset = Date._parse_offset(offset_str)
        if re.search(offset_search, subseconds) is not None:
            offset_str = re.sub(offset_search, r'\2', subseconds)
            subseconds = re.sub(offset_search, r'\1', subseconds)
            offset = Date._parse_offset(offset_str) or offset

        parsed_date_time = None
        for fmt in ('%Y:%m:%d %H:%M:%S', '%Y-%m-%d %H:%M:%S'):
            try:
                parsed_date_time = Date.strptime(date, fmt)
                break
            except ValueError:
                continue

        if parsed_date_time and offset:
            parsed_date_time = parsed_date_time - offset

        return {
            'date': parsed_date_time,
            'subseconds': subseconds,
            'offset': offset
        }

    def from_filename(self, user_regex, timestamp=None):
        # If missing datetime from EXIF data check if filename is in datetime
        # format. For this use a user provided regex if possible. Otherwise
        # assume a filename such as IMG_20160915_123456.jpg as default.
        default_regex = re.compile(r'.*[_-](?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})[_-]?(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})')
        regex = user_regex or default_regex
        matches = regex.search(os.path.basename(self.filename))

        if matches:
            try:
                match_dir = matches.groupdict(default='0')
                # Convert str to int
                match_dir = dict([a, int(x)] for a, x in match_dir.items())
                date = self.build(match_dir)
            except (KeyError, ValueError):
                date = None

            if date:
                return {
                    'date': date,
                    'subseconds': ''
                }

        if timestamp:
            return self.from_timestamp()

    def from_timestamp(self) -> dict:
        date = datetime.fromtimestamp(os.path.getmtime(self.filename))
        return {
            'date': date,
            'subseconds': ''
        }
