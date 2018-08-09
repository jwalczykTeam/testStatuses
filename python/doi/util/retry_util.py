import log_util
import time


def retry(func):
    """
    Decorator to retry a method if it fails meant to be use in methods
    that depend on outside components to deal with them failing
    ie (astro, compose, messagehub)

    Similar to the Rudi one on mlpipeline/decorators.py
    """

    def go(*args, **kwargs):

        wait = 5
        retry_count = 0
        max_retry_count = 1

        logger = log_util.get_logger("astro.jobs.util.retry_util")

        while True:

            try:
                return func(*args, **kwargs)
            except Exception as e:
                if retry_count >= max_retry_count:
                    raise e
                logger.error(
                    "{} method failed. Waiting {}s to retry: {}".format(
                        func.__name__,
                        wait,
                        e.message))
                retry_count += 1
                time.sleep(wait)

    return go
