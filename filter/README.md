# Uber Pickup Data Filtering Script

## Description

Script for filtering pickup events in the clustered data based on day-of-week, time, etc.
The script reads from an S3 bucket (where we store our MR job results) and filters the
results files based on the given filter attributes. Results are uploaded to separate S3
output bucket which we can use to generate interesting results such as heat maps of
pickup locations.

## Usage

```
usage: filter [-h] [--input-bucket INPUT] [--output-bucket OUTPUT] [--days DAYS]
              [--hour HOUR] [--hour-range HOUR_RANGE] [--date DATE]
              [--date-range DATE_RANGE]
optional arguments:
   -h, --help            show this help message and exit
   --input INPUT         input path to csv file
   --output OUTPUT       output path to write filtered results
   --days DAYS           [filter] comma seperated list of days of week
   --hour HOUR           [filter] hour to filter on (24hr format)
   --hour-range HOUR_RANGE
                         [filter] comma seperated start and end hours, ex 0,12
   --date DATE           [filter] date to filter on (ex. 2014/08/17)
   --date-range DATE_RANGE
                         [filter] comma seperated date range (ex.
                         2014/08/17,2014/08/18)
```


## Motivation

Use the Uber pickup data to understand patterns of ride-sharing usage based on certain times
of day or certain days of the week. By filtering the data we can better identify usage as it
relates to time and how it might fluctuate.

## Challenges

#### S3 access control
Our S3 buckets are public for reads for ease of use. Otherwise we need to method for managing
credentials and access across the different components of our project. In a real setting we'd
want to make these protected buckets.

#### Size of data
The output from the MR jobs is enough data to almost warrant a seperate MR job for filtering.
The filtering script takes a non-trivial time to run (read time + filter time + write time) and
this would only get worse as the amount of data grows.

#### Data format
The MR output format is both tabs and commas which means there is some parsing and formatting
required to make the data easier to use. In a real system it might worthwhile to create some
intermediate step or service for normalization in the data pipeline.
