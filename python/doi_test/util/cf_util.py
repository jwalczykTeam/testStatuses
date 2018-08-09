'''
Created on Dec 14, 2016

@author: adminibm
'''

import os, json
from cloudfoundry_client.client import CloudFoundryClient


def get_cf_login_info():
    '''
    Get cf login info from local ~/.cf folder. You are supposed to
    have the cf cli installed and logged in.
    '''
    cf_conf = os.path.join(os.environ.get('HOME'), '.cf', 'config.json')
    with open(cf_conf) as json_data:
        d = json.load(json_data)

    return d


def get_db_mh_credentials(target_endpoint, access_token, container_bridge_app='containerbridge'):
    db_credentials = mh_credentials = None
    client = CloudFoundryClient(target_endpoint, skip_verification=True)

    '''
    if username and password:
        client.init_with_user_credentials(username, password)
    else:
        client.init_with_token(get_cf_login_info()['RefreshToken'])
    '''

    client.init_with_token(access_token)
    cb = client.apps.get_first(name='containerbridge')
    if cb:
        services = client.apps.get_env(cb['metadata']['guid'])['system_env_json']['VCAP_SERVICES']
        if services:
            for s in services['user-provided']:
                nm = s.get('name', '').lower()
                if 'elasticsearch' in nm:
                    db_credentials = s['credentials']
                elif 'messagehub' in nm:
                    mh_credentials = s['credentials']

                if db_credentials and mh_credentials:
                    break

    return ({"compose-elasticsearch": db_credentials}, {"messagehub": mh_credentials})
