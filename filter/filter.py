import abc
import argparse
import dateparser
import io

# 3p
from datetime import timedelta
import pandas as pd
import boto3
from botocore import UNSIGNED
from botocore.config import Config

# usage: filter [-h] [--input-bucket INPUT] [--output-bucket OUTPUT] [--days DAYS]
#               [--hour HOUR] [--hour-range HOUR_RANGE] [--date DATE]
#               [--date-range DATE_RANGE]
#
# optional arguments:
#   -h, --help            show this help message and exit
#   --input INPUT         input path to csv file
#   --output OUTPUT       output path to write filtered results
#   --days DAYS           [filter] comma seperated list of days of week
#   --hour HOUR           [filter] hour to filter on (24hr format)
#   --hour-range HOUR_RANGE
#                         [filter] comma seperated start and end hours, ex 0,12
#   --date DATE           [filter] date to filter on (ex. 2014/08/17)
#   --date-range DATE_RANGE
#                         [filter] comma seperated date range (ex.
#                         2014/08/17,2014/08/18)

AWS_ACCESS_KEY = 'AKIAJIQCIWWG7IKCTMMQ'
AWS_SECRET_KEY = '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'
DEFAULT_INPUT_BUCKET = 'cs498s3mapstore'
DEFAULT_OUTPUT_BUCKET = 'cs498s3mapstore-filtered'
COLUMN_HEADERS = ["cluster_id", "lat", "long", "weekday", "date", "hour", "type"]
DATE_FORMAT = "%Y/%m/%d"
DAY_OF_WEEK = {
    0: 'Monday',
    1: 'Tuesday',
    2: 'Wednesday',
    3: 'Thursday',
    4: 'Friday',
    5: 'Saturday',
    6: 'Sunday'
}

class Filter(object):
    __metaclass__ = abc.ABCMeta
    """ Abstract base class for filters
    """
    @abc.abstractproperty
    def name(self):
        """(str) name of filter"""

    @abc.abstractmethod
    def is_match(self, row):
        """apply filter on self.original"""

class DateFilter(Filter):
    def __init__(self, date):
        self.dt = dateparser.parse(str(date))
        self.date = self.dt.strftime(DATE_FORMAT)

    def __str__(self):
        return 'date={}'.format(self.dt.strftime('%Y-%m-%d'))

    @property
    def name(self):
        return "DateFilter"

    def is_match(self, df):
        return df['date'] == self.date


class DateRangeFilter(Filter):
    dates = []
    def __init__(self, start, end):
        self.dt_start = dateparser.parse(str(start))
        self.dt_end = dateparser.parse(str(end))
        if self.dt_start > self.dt_end:
            raise ValueError("DateRangeFilter requires start date before end date")

        # kind of hack but for date ranges generate all dates in the range
        dt = self.dt_start
        while dt <= self.dt_end:
            self.dates.append(dt.strftime(DATE_FORMAT))
            dt += timedelta(days=1)

    def __str__(self):
        return 'date={}-{}'.format(self.dt_start.strftime('%Y-%m-%d'), self.dt_end.strftime('%Y-%m-%d'))

    @property
    def name(self):
        return "DateRangeFilter"

    def is_match(self, df):
        mask = (1 == 2)
        for date in self.dates:
            mask |= (df['date'] == date)
        return (mask)

class DayOfWeekFilter(Filter):
    def __init__(self, days):
        self.weekdays = []
        # day of the week as an integer, where Monday is 0 and Sunday is 6
        for day in days:
            weekday = None
            if isinstance(day, int):
                if day < 0 or day > 6:
                    raise ValueError("DayFilter as int must fall between 0(Mon) and 6(Sun)")
                else:
                    weekday = day
            elif isinstance(day, str):
                day = day.strip().lower()
                if day in ['0', 'monday', 'mon', 'm']:
                    weekday = 0
                elif day in ['1', 'tuesday', 'tues', 'tu']:
                    weekday = 1
                elif day in ['2', 'wednesday', 'wed', 'w']:
                    weekday = 2
                elif day in ['3', 'thursday', 'thurs', 'th']:
                    weekday = 3
                elif day in ['4', 'friday', 'fri', 'f']:
                    weekday = 4
                elif day in ['5', 'saturday', 'sat']:
                    weekday = 5
                elif day in ['6', 'sunday', 'sun']:
                    weekday = 6
                else:
                    raise ValueError("Invalid string for DayFilter")
            else:
                raise ValueError("Invalid type for DayFilter")
            self.weekdays.append(weekday)

    def __str__(self):
        return 'weekdays={}'.format(','.join([DAY_OF_WEEK[w] for w in self.weekdays]))

    @property
    def name(self):
        return "DayOfWeekFilter"

    def is_match(self, df):
        mask = (1 == 2)
        for day in self.weekdays:
            mask |= (df['weekday'] == day)
        return (mask)

class HourFilter(Filter):
    def __init__(self, hour):
        if isinstance(hour, int):
            self.hour = hour
        elif isinstance(hour, str):
            hour = hour.lower()
            if 'am' in hour:
                self.hour = int(hour.replace('am', '').strip())
            elif 'pm' in hour:
                hour = int(hour.replace('am', '').strip())
                self.hour = hour + 12 if hour < 12 else 0
            else:
                # just try converting to int
                self.hour = int(hour)
        else:
            raise ValueError("Invalid type for HourFilter")

        if self.hour < 0 or self.hour > 23:
            raise ValueError("Invalid value for HourFilter")

    def __str__(self):
        return 'hour={}'.format(self.hour)

    @property
    def name(self):
        return "HourFilter"

    def is_match(self, df):
        return self.hour == df['hour']

class HourRangeFilter(Filter):
    def __init__(self, start, end):
        self.hour_start = HourFilter(start).hour
        self.hour_end = HourFilter(end).hour

    def __str__(self):
        return 'hour={}-{}'.format(self.hour_start, self.hour_end)

    @property
    def name(self):
        return "HourRangeFilter"

    def is_match(self, df):
        return (df['hour'] >= self.hour_start) & (df['hour'] <= self.hour_end)


# *** main script execution starts here ***

def build_mask(df, filters):
    mask = 1 == 1
    for f in filters:
        # print('Applying filter: {}'.format(f))
        mask &= f.is_match(df)
    return mask

def build_filters(args):
    filters = []
    if args.days:
        days = args.days.split(',')
        filters.append(DayOfWeekFilter(days))
    if args.hour:
        filters.append(HourFilter(args.hour))
    elif args.hour_range:
        start, end = args.hour_range.split(',')
        filters.append(HourRangeFilter(start, end))
    if args.date:
        filters.append(DateFilter(args.date))
    elif args.date_range:
        start, end = args.date_range.split(',')
        filters.append(DateRangeFilter(start, end))
    return filters

def normalize_df(df):
    # necessary due to formatting from the MR job output
    new = df["cluster_id_lat"].str.split('\t', n = 1, expand = True)
    df['cluster_id'] = new[0]
    df['lat'] = new[1]
    df.drop(columns=['cluster_id_lat'], inplace=True)
    return df

def filter(input_bucket, output_bucket, filters):
    # first two headers are connected with a tab
    headers = ['cluster_id_lat'] + COLUMN_HEADERS[2:]

    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    contents = s3.list_objects(Bucket=input_bucket).get('Contents', [])
    for item in contents:
        key = item['Key']
        if 'cluster/part' not in key:
            continue


        # get the data into a dataframe
        print('reading data from key {}'.format(key))
        obj = s3.get_object(Bucket=input_bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8', names=headers)
        df = normalize_df(df)

        # filter results
        print('filtering data...')
        mask = build_mask(df, filters)
        df = df[mask]

        # write to s3
        write_df_to_s3(output_bucket, filters, key, df)

def write_df_to_s3(bucket, filters, key, df):
    # XXX: we're initing the session every time which is unecessary
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )
    s3 = session.resource('s3')

    key_parts = key.split('/')
    fmt_str = '_'.join([str(f) for f in filters])
    key_out = '{}/{}'.format(fmt_str, key_parts[-1])

    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer)

    print('writing filtered data to bucket: {} key: {}'.format(bucket, key_out))
    object = s3.Object(bucket, key_out)
    object.put(Body=csv_buffer.getvalue())


def main():
    parser = argparse.ArgumentParser("filter")
    parser.add_argument("--input", type=str, help="input bucket containing cluster data")
    parser.add_argument("--output", type=str, help="output bucket to write filtered results")
    parser.add_argument("--days", type=str, help="[filter] comma seperated list of days of week")
    parser.add_argument("--hour", type=int, help="[filter] hour to filter on (24hr format)")
    parser.add_argument("--hour-range", type=str, help="[filter] comma seperated start and end hours, ex 0,12")
    parser.add_argument("--date", type=str, help="[filter] date to filter on (ex. 2014/08/17)")
    parser.add_argument("--date-range", type=str, help="[filter] comma seperated date range (ex. 2014/08/17,2014/08/18)")
    args = parser.parse_args()
    filters = build_filters(args)

    input_bucket = DEFAULT_INPUT_BUCKET
    if args.input:
        input_bucket = args.input
    output_bucket = DEFAULT_OUTPUT_BUCKET
    if args.output:
        output_bucket = args.output
    if not filters:
        print('please specify a filter option')
        return

    filter(input_bucket, output_bucket, filters)

if __name__ == '__main__':
    main()
