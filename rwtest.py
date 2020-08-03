import time
import sys
import threadpool
import os
import argparse
import re
import json

class TestContext:
    def __init__(self, rootPath):
        self.rootPath = rootPath

    def setFileSize(self, fileSize):
        self.fileSize = fileSize

    def setFileCount(self, fileCount):
        self.fileCount = fileCount

    def setIsSync(self, sync):
        self.sync = sync

    def setTestMethod(self, testMethod):
        self.testMethod = testMethod

    def setBuf(self, buf):
        self.buf = buf

    def cleanFiles(self, cleanFiles):
        self.cleanFiles = cleanFiles

    def __str__(self):
        return 'rootPath: %s, fileSize: %d, fileCount: %d, isSync: %s, testMethod: %s, bufSize: %d, cleanFiles: %s'\
               % (self.rootPath, self.fileSize, self.fileCount, self.sync, self.testMethod,
                  len(self.buf), self.cleanFiles)


units = {"B": 1, "KB": 2**10, "MB": 2**20, "GB": 2**30, "TB": 2**40}

def parse_size(size):
    if size.isdigit():
        return long(size)
    size = size.upper()
    if not re.match(r' ', size):
        size = re.sub(r'([KMGT]?B)', r' \1', size)
    number, unit = [string.strip() for string in size.split()]
    return long(float(number)*units[unit])

def test_write(file_name):
    print "write %s, size: %d, buf size: %d" % (file_name, testContext.fileSize,
                                                len(testContext.buf))

    f = open(file_name, 'wb')
    count = testContext.fileSize / len(testContext.buf)
    while count > 0:
        count = count - 1
        f.write(testContext.buf)
    f.close()
    print "write %s done." % file_name

def test_read(file_name):
    print "read %s, size: %d, buf size: %d" % (file_name, testContext.fileSize,
                                               len(testContext.buf))
    f = open(file_name, 'rb')
    count = testContext.fileSize / len(testContext.buf)
    while count > 0:
        count = count - 1
        f.read(len(testContext.buf))
    f.close()
    print "read %s done." % file_name


def test_readWriteDir(testContext):
    if not os.path.exists(testContext.rootPath):
        os.makedirs(testContext.rootPath)
    if testContext.sync:
        start_time = time.time()
        for i in range(0, testContext.fileCount):
            file_name = testContext.rootPath + "/%04d.txt" % i
            testContext.testMethod(file_name)
    else:
        pool = threadpool.ThreadPool(testContext.fileCount, poll_timeout=None)
        args_list = []
        for i in range(0, testContext.fileCount):
            file_name = testContext.rootPath + "/%04d.txt" % i
            args_list.append(file_name)
        requests = threadpool.makeRequests(testContext.testMethod, args_list)
        start_time = time.time()
        [pool.putRequest(req) for req in requests]
        pool.wait()

    print "CHECK result: ", testContext.rootPath
    dirs = os.listdir(testContext.rootPath)
    for file in dirs:
        filesize = os.path.getsize(testContext.rootPath + "/" + file)
        print "rm " if testContext.cleanFiles else "keep", file, filesize
        if testContext.cleanFiles:
            os.remove(testContext.rootPath + "/" + file)

    cost = time.time() - start_time
    totalSize = testContext.fileSize * testContext.fileCount
    avgSpeed = totalSize / cost
    print "Summary: %s test %s files, each one is %s MB, root dir is %s" \
          % ("sync" if testContext.sync else "async", testContext.fileCount, testContext.fileSize / 1024 / 1024,
             testContext.rootPath)
    print "cost = %.3f s, speed = %.3f MB/S" % (cost, avgSpeed / 1024.0 / 1024.0)
    print "avg cost = %.3f s per file" % (cost / testContext.fileCount)

def main(argv):
    param_rootPath = os.getenv('TEST_ROOT_PATH')
    param_fileSize = os.getenv('TEST_FILE_SIZE')
    param_fileCount = os.getenv('TEST_FILE_COUNT')
    param_sync = os.getenv('TEST_IS_SYNC')
    param_testMethod = os.getenv('TEST_METHOD')
    param_bufferSize = os.getenv('TEST_BUFFER_SIZE')
    param_skipCheck = os.getenv('TEST_SKIP_CHECK')
    param_cleanFiles = os.getenv('TEST_CLEAN_FILES')

    parser = argparse.ArgumentParser()
    parser.add_argument('-rootPath', type=str, help='input a root path')
    parser.add_argument('-fileSize', type=str, help='input fileSize')
    parser.add_argument('-fileCount', type=int, help='input fileCount')
    parser.add_argument('-sync', type=bool, help='input sync or async')
    parser.add_argument('-testMethod', type=str, help='input testMethod name, test_read or test_write')
    parser.add_argument('-bufferSize', type=str, help='input bufferSize')
    parser.add_argument('-skipCheck', type=bool, help='input if skip check rootPath must starts with /data/bucketmbl')
    parser.add_argument('-cleanFiles', type=bool, help='input if cleanFiles in the rootPath')
    args = parser.parse_args()
    if args.rootPath:
        param_rootPath = args.rootPath
    if args.fileSize:
        param_fileSize = args.fileSize
    if args.fileCount:
        param_fileCount = args.fileCount
    if args.sync:
        param_sync = args.sync
    if args.testMethod:
        param_testMethod = args.testMethod
    if args.bufferSize:
        param_bufferSize = args.bufferSize
    if args.skipCheck:
        param_skipCheck = args.skipCheck
    if args.cleanFiles:
        param_cleanFiles = args.cleanFiles
    if not json.loads(param_skipCheck) and not param_rootPath.startswith('/data/bucketmbl'):
        print "TEST_ROOT_PATH must starts with /data/bucketmbl for security purpose."
        return

    buf = bytearray(parse_size(param_bufferSize))

    testMethod = getattr(sys.modules['__main__'], param_testMethod)

    global testContext
    testContext = TestContext(param_rootPath)
    testContext.fileSize = parse_size(param_fileSize)
    testContext.fileCount = int(param_fileCount)
    testContext.buf = buf
    testContext.sync = json.loads(param_sync)
    testContext.testMethod = testMethod
    testContext.cleanFiles = json.loads(param_cleanFiles)
    print testContext

    if testMethod == test_read:
        print "Start to drop cache."
        os.system("echo 3 > /proc/sys/vm/drop_caches")
        print "Dropped cache."
    test_readWriteDir(testContext)

if __name__ == "__main__":
    main(sys.argv)
