"""
Author: Cyrus M Vahid
This is the equivalent of the map reduce job from chapter 3 that processes a NASA log.
To run it run $ python3 nasa-log-processor.py Data/NASA_access_log_Jul95.log
"""


from mrjob.job import MRJob
import re

class LogAnalyzer(MRJob):

    def mapper(self, _, line):
        pattern = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(.+?)" (\d{3}) (\S+)'
        p = re.compile(pattern)
        m = p.match(line)
        if m:
            i = int(m.group(6))
            if i in range(100, 200):
                yield "100-199: Informational", 1
            elif i in range(200, 300):
                yield "200-299: All OK", 1
            elif i in range(300,400):
                yield "300 to 399: Redirection", 1
            elif i in range(400, 500):
                yield "400 to 499: Client Error", 1
            elif i in range(500, 600):
                yield "500 to 599: Server Error", 1
        else:
            yield 'format mismatch', 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    LogAnalyzer.run()






