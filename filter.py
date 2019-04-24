import abc
import argparse
import dateparser
from datetime import timedelta
import pandas as pd

# usage: filter [-h] [--input INPUT] [--output OUTPUT] [--days DAYS]
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

COLUMN_HEADERS = ["cluster_id", "lat", "long", "weekday", "hour", "date", "type", "index"]
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
        dt = dateparser.parse(str(date))
        self.date = dt.strftime(DATE_FORMAT)

    def __str__(self):
        return 'date == {}'.format(self.date)

    @property
    def name(self):
        return "DateFilter"

    def is_match(self, df):
        return df['date'] == self.date


class DateRangeFilter(Filter):
    dates = []
    def __init__(self, start, end):
        dt_start = dateparser.parse(str(start))
        dt_end = dateparser.parse(str(end))
        if dt_start > dt_end:
            raise ValueError("DateRangeFilter requires start date before end date")

        # kind of hack but for date ranges generate all dates in the range
        dt = dt_start
        while dt <= dt_end:
            self.dates.append(dt.strftime(DATE_FORMAT))
            dt += timedelta(days=1)

    def __str__(self):
        return '{} <= date <= {}'.format(str(self.dates[0]), str(self.dates[:-1]))

    @property
    def name(self):
        return "DateRangeFilter"

    def is_match(self, df):
        mask = (1 == 2)
        for date in self.dates:
            mask |= (df['date'] == date)
        return (mask)

class DayOfWeekFilter(Filter):
    def __init__(self, day):
        # day of the week as an integer, where Monday is 0 and Sunday is 6
        if isinstance(day, int):
            if day < 0 or day > 6:
                raise ValueError("DayFilter as int must fall between 0(Mon) and 6(Sun)")
            else:
                self.weekday = day
        elif isinstance(day, str):
            day = day.strip().lower()
            if day in ['0', 'monday', 'mon', 'm']:
                self.weekday = 0
            elif day in ['1', 'tuesday', 'tues', 'tu']:
                self.weekday = 1
            elif day in ['2', 'wednesday', 'wed', 'w']:
                self.weekday = 2
            elif day in ['3', 'thursday', 'thurs', 'th']:
                self.weekday = 3
            elif day in ['4', 'friday', 'fri', 'f']:
                self.weekday = 4
            elif day in ['5', 'saturday', 'sat']:
                self.weekday = 5
            elif day in ['6', 'sunday', 'sun']:
                self.weekday = 6
            else:
                raise ValueError("Invalid string for DayFilter")
        else:
            raise ValueError("Invalid type for DayFilter")

    def __str__(self):
        return 'day == {}'.format(DAY_OF_WEEK[self.weekday])

    @property
    def name(self):
        return "DayOfWeekFilter"

    def is_match(self, df):
        return df['weekday'] == self.weekday

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
        return 'hour == {}'.format(self.hour)

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
        return '{} <= hour <= {}'.format(self.hour_start, self.hour_end)

    @property
    def name(self):
        return "HourRangeFilter"

    def is_match(self, df):
        return (df['hour'] >= self.hour_start) & (df['hour'] <= self.hour_end)


# *** main script execution starts here ***

def build_mask(df, filters):
    mask = 1 == 1
    for f in filters:
        print('Applying filter: {}'.format(f))
        mask &= f.is_match(df)
    return mask

def build_filters(args):
    filters = []
    if args.days:
        days = args.days.split(',')
        for day in days:
            filters.append(DayOfWeekFilter(day))
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

def main():
    parser = argparse.ArgumentParser("filter")
    parser.add_argument("--input", type=str, required=True, help="input path to csv file")
    parser.add_argument("--output", type=str, help="output path to write filtered results")
    parser.add_argument("--days", type=str, help="[filter] comma seperated list of days of week")
    parser.add_argument("--hour", type=int, help="[filter] hour to filter on (24hr format)")
    parser.add_argument("--hour-range", type=str, help="[filter] comma seperated start and end hours, ex 0,12")
    parser.add_argument("--date", type=str, help="[filter] date to filter on (ex. 2014/08/17)")
    parser.add_argument("--date-range", type=str, help="[filter] comma seperated date range (ex. 2014/08/17,2014/08/18)")
    args = parser.parse_args()
    filters = build_filters(args)

    # TODO: what is the input type of the file? we may also need to add headers
    df = pd.read_csv(args.input)
    if filters:
        mask = build_mask(df, filters)
        df = df[mask]

    if args.output:
    # TODO: write to 'output' path using the same format as input
        print(df)
    else:
        print(df)

if __name__ == '__main__':
    main()
