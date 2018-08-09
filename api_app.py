#!/usr/bin/env python

from doi.util import log_util
from doi.util import ssl_util
import connexion
import logging
import sys


if __name__ == '__main__':
    logger = log_util.get_logger("astro")
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)

    #app = connexion.App(__name__, specification_dir='./swagger/', debug=True)
    app = connexion.App(__name__, specification_dir='./swagger/')
    app.add_api('swagger.yaml', arguments={'title': 'Astro API'})
    port, context = ssl_util.get_ssl_context_from_env()
    app.run(port=port, threaded=True, ssl_context=context)
