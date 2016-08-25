"""
Authon: Cyrus M Vahid
This code looks the date entry in a log file supplied in /Data/sample-log.log and counts the events
that occured in a specific timestamp, aggregated every second.
The mapper splits the lines and reducer sums up the occurences per second.

To run the app:
python log_analysis.py Data/sample-log.log
"""
from mrjob.job import MRJob

class LogProcessor(MRJob):
    def mapper(self, _, line):
        yield line[:15], 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    LogProcessor.run()
