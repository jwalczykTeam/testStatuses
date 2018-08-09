'''
Created on Nov 7, 2016

@author: adminibm
'''

import os
from doi_test.util.local_test import LocalAstroInstance
from doi_test.util.remote_test import RemoteAstroInstance

_ASTRO_TARGET="ASTRO_TARGET_ENV"


def get_astro_instance(config, test_module=None, test_class=None):
    env = os.environ[_ASTRO_TARGET] if _ASTRO_TARGET in os.environ else 'local'

    if env == 'local':
        return LocalAstroInstance(config['local'], test_module, test_class)
    elif env == 'remote':
        return RemoteAstroInstance(config['remote'], test_module, test_class)
    else:
        raise ValueError("Invalid Astro target env set: {}".format(env))
