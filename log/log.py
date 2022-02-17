import os
import logging
import platform
from logging.config import dictConfig

from log.config import DEV_CONFIG, PROD_CONFIG
from web.server.environment import IS_PRODUCTION, IS_TEST

# Load the appropriate logging configuration
LOG_CONFIG = PROD_CONFIG if IS_PRODUCTION else DEV_CONFIG
dictConfig(LOG_CONFIG)

LOG = logging.getLogger('ZenysisLogger')


def get_app_context():
    client = GoogleClient(project='zen-1234')
    deployment_name = ''
    try:
        from config.general import DEPLOYMENT_NAME

        deployment_name = DEPLOYMENT_NAME

        from flask import current_app

        if current_app:
            deployment_name = current_app.zen_config.general.DEPLOYMENT_NAME
    except ImportError:
        pass

    return {
        'deploymentName': deployment_name,
        'hostName': platform.node(),
        'role': 'web',
        'service': 'web',
    }


def setup_stackdriver_logger(logger):
    pass


# TODO(ian): Reenable this when stackdriver logging is compatible with gevent.
'''
    google_client = GoogleClient()
    handler = CloudLoggingHandler(google_client, labels=get_app_context())
    logger.addHandler(handler)


if IS_PRODUCTION and not IS_TEST and not os.getenv('DISABLE_STACKDRIVER_LOGGING'):
    if os.getenv('GUNICORN_WORKER_CLASS') == 'gevent':
        raise Exception('Stackdriver logging cannot be enabled with gevent workers')

    # NOTE(stephen): Gating these imports so that the pipeline does not need to have
    # the google libraries installed to work.
    from google.cloud.logging import Client as GoogleClient
    from google.cloud.logging.handlers import CloudLoggingHandler

    # NOTE(ian): Stackdriver logger is set up dynamically, because
    # ZenysisLogger's stackdriver must be set up after the gunicorn
    # fork, and also because we need to pass arguments to the log
    # handler class.
    LOG.info('Setting up stackdriver logging...')
    setup_stackdriver_logger(LOG)
    setup_stackdriver_logger(logging.getLogger('gunicorn.access'))
    setup_stackdriver_logger(logging.getLogger('gunicorn.error'))
'''
