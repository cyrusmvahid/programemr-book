from mrjob.job import MRJob

class LogProcessor(MRJob):
    def mapper(self, _, line):
        yield line[:14], 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    LogProcessor.run()
