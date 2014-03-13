# Copyright (c) 2013 - Rackspace Inc.
# Copyright (c) 2014 - Dark Secret Software Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

import logging
import logging.handlers
import multiprocessing
import os
import re
import threading
import traceback
import sys
import time


class ParentLoggerDoesNotExist(Exception):
    def __init__(self, parent_logger_name):
        self.reason = "Cannot create child logger as parent logger with the" \
                      "name %s does not exist." % parent_logger_name


class QueueHandler(logging.Handler):
    def __init__(self, queue):
        super(QueueHandler, self).__init__()
        self.queue = queue

    def emit(self, record):
        try:
            # ensure that exc_info and args
            # have been stringified.  Removes any chance of
            # unpickleable things inside and possibly reduces
            # message size sent over the pipe
            if record.exc_info:
                # just to get traceback text into record.exc_text
                self.format(record)
                # remove exception info as it's not needed any more
                record.exc_info = None
            if record.args:
                record.msg = record.msg % record.args
                record.args = None
            self.queue.put_nowait(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


class LogManager(object):
    def __init__(self, project_name, worker_name):
        self.loggers = {}
        self.logger_queue_map = {}
        self.default_logger_location = '/var/log/%s/%s' % (
                                                project_name, '%s.log')
        self.default_logger_name = '%s-default' % project_name

        logger = self.get_logger(worker_name, is_parent=True)
        self.queue = self.get_queue(logger.name)

    def set_default_logger_location(self, loc):
        self.default_logger_location = loc

    def set_default_logger_name(self, name):
        default_logger_name = name

    def _create_parent_logger(self, name):
        if name not in self.loggers:
            logger = _create_timed_rotating_logger(name)
            self.loggers[name] = logger
            self.logger_queue_map[name] = multiprocessing.Queue(-1)

        return self.loggers[name]

    def _create_child_logger(self, name):
        child_logger_name = "child_%s" % name
        if child_logger_name in self.loggers:
            return self.loggers[child_logger_name]

        if name in self.loggers:
            queue = self.logger_queue_map[name]
            logger = self._create_queue_logger(child_logger_name, queue)
            self.loggers[child_logger_name] = logger
            return self.loggers[child_logger_name]
        raise ParentLoggerDoesNotExist(name)

    def _create_queue_logger(self, name, queue):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        handler = QueueHandler(queue)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _create_timed_rotating_logger(self, name):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        handler = TimedRotatingFileHandlerWithCurrentTimestamp(
            default_logger_location % name, when='midnight', interval=1,
            backupCount=6)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.handlers[0].doRollover()
        return logger

    def get_logger(name=None, is_parent=True):
        if name is None:
            name = self.default_logger_name
        if is_parent:
            return self._create_parent_logger(name)
        return self._create_child_logger(name)

    def get_queue(self, name):
        return self.logger_queue_map[name]

    def start(self):
        self.thread = threading.Thread(target=self._receive)
        self.thread.daemon = True
        self.thread.start()

    def _receive(self):
        while True:
            try:
                record = self.queue.get()
                # None is sent as a sentinel to tell the listener to quit
                if record is None:
                    break
                self.logger.handle(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except EOFError:
                break
            except:
                traceback.print_exc(file=sys.stderr)
    def end(self):
        self.queue.put_nowait(None)
        self.thread.join()
        for handler in self.logger.handlers:
            handler.close()

    def _get_child_logger(self, name):
        if name is None:
            name = self.default_logger_name
        return self.get_logger(name=name, is_parent=False)

    def warn(self, msg, name=None):
        self._get_child_logger(name=name).warn(msg)

    def error(self, msg, name=None):
        self._get_child_logger(name=name).error(msg)

    def info(self, msg, name=None):
        self.get_logger(name=name).info(msg)


class TimedRotatingFileHandlerWithCurrentTimestamp(
        logging.handlers.TimedRotatingFileHandler):

    def __init__(self, filename, when='h', interval=1, backupCount=0,
                 encoding=None, delay=False, utc=False):
        logging.handlers.TimedRotatingFileHandler.__init__(
            self, filename, when, interval, backupCount, encoding, delay, utc)
        self.suffix = "%Y-%m-%d_%H-%M-%S"
        self.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$")

    def doRollover(self):
        """Exactly the same as TimedRotatingFileHandler's doRollover() except
        that the current date/time stamp is appended to the filename rather
        than the start date/time stamp, when the rollover happens."""
        currentTime = int(time.time())
        if self.stream:
            self.stream.close()
            self.stream = None
        if self.utc:
            timeTuple = time.gmtime(currentTime)
        else:
            timeTuple = time.localtime(currentTime)
        dfn = self.baseFilename + "." + time.strftime(self.suffix, timeTuple)
        if os.path.exists(dfn):
            os.remove(dfn)
        os.rename(self.baseFilename, dfn)
        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                os.remove(s)
        self.mode = 'w'
        self.stream = self._open()
        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt = newRolloverAt + self.interval
        # If DST changes and midnight or weekly rollover, adjust for this.
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')
                and not self.utc):
            dstNow = time.localtime(currentTime)[-1]
            dstAtRollover = time.localtime(newRolloverAt)[-1]
            if dstNow != dstAtRollover:
                if not dstNow:
                    # DST kicks in before next rollover,
                    # so we need to deduct an hour
                    newRolloverAt = newRolloverAt - 3600
                else:
                    # DST bows out before next rollover,
                    # so we need to add an hour
                    newRolloverAt = newRolloverAt + 3600
        self.rolloverAt = newRolloverAt
