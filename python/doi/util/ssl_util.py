'''
Created on Dec 23, 2016

@author: alberto
'''

import os
import json
from doi import constants

CA_PATHS = [
    "/etc/pki/tls/cert.pem",    # RedHat
    "/etc/ssl/certs/",           # Ubuntu
    "/usr/local/etc/openssl/cert.pem"  # OSX
]

def get_ca_path():
    ca_path = None
    for ca_path in CA_PATHS:
        if os.path.exists(ca_path):
            break

    assert ca_path is not None, "Cannot find ca path for this platform"
    return ca_path


def get_ssl_context_from_env(default_port=8080, default_ssl_port=8443):
    api_config_str = os.environ.get(constants.ASTRO_API_CONFIG_ENV_VAR)
    if api_config_str:
        api_config = json.loads(api_config_str)
        use_https = api_config.get('use_https', False)
        if use_https:
            cert_file = api_config['cert_file']
            private_key_file = api_config['private_key_file']
            context = (cert_file, private_key_file)
            port = api_config.get('port', default_ssl_port)
        else:
            context = None
            port = api_config.get('port', default_port)
    else:
        context = None
        port = default_port

    return (port, context)
