#!/usr/bin/python
#
# (c) 2015, Steve Gargan <steve.gargan@gmail.com>
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

DOCUMENTATION = """
module: consul_kv
short_description: Manipulate entries in the key/value store of a consul cluster.
description:
  - Allows the addition, modification and deletion of key/value entries in a
    consul cluster via the agent. The entire contents of the record, including
    the indices, flags and session are returned as 'value'.
  - If the key represents a prefix then Note that when a value is removed, the existing
    value if any is returned as part of the results.
  - "See http://www.consul.io/docs/agent/http.html#kv for more details."
requirements:
  - "python >= 2.6"
  - python-consul
  - requests
version_added: "2.0"
author: "Steve Gargan (@sgargan)"
options:
    state:
        description:
          - the action to take with the supplied key and value. If the state is
            'present', the key contents will be set to the value supplied,
            'changed' will be set to true only if the value was different to the
            current contents. The state 'absent' will remove the key/value pair,
            again 'changed' will be set to true only if the key actually existed
            prior to the removal. An attempt can be made to obtain or free the
            lock associated with a key/value pair with the states 'acquire' or
            'release' respectively. a valid session must be supplied to make the
            attempt changed will be true if the attempt is successful, false
            otherwise.
        required: false
        choices: ['present', 'absent', 'acquire', 'release']
        default: present
    key:
        description:
          - the key at which the value should be stored.
        required: true
    value:
        description:
          - the value should be associated with the given key, required if state
            is present
        required: true
    recurse:
        description:
          - if the key represents a prefix, each entry with the prefix can be
            retrieved by setting this to true.
        required: false
        default: false
    session:
        description:
          - the session that should be used to acquire or release a lock
            associated with a key/value pair
        required: false
        default: None
    token:
        description:
          - the token key indentifying an ACL rule set that controls access to
            the key value pair
        required: false
        default: None
    cas:
        description:
          - used when acquiring a lock with a session. If the cas is 0, then
            Consul will only put the key if it does not already exist. If the
            cas value is non-zero, then the key is only set if the index matches
            the ModifyIndex of that key.
        required: false
        default: None
    flags:
        description:
          - opaque integer value that can be passed when setting a value.
        required: false
        default: None
    host:
        description:
          - host of the consul agent defaults to localhost
        required: false
        default: localhost
    port:
        description:
          - the port on which the consul agent is running
        required: false
        default: 8500
    retrieve:
        description:
            - retrieve the index and stored data for a given key when added to consul
        required: false
        default: True
    json:
        description:
          - Used for addition of key values only into consul. Importing via json file or json object.
            Do not use with key/value inputs - these are mutually exclusive
        required: false
"""


EXAMPLES = '''

  - name: add or update the value associated with a key in the key/value store
    consul_kv:
      key: somekey
      value: somevalue

  - name: remove a key from the store
    consul_kv:
      key: somekey
      state: absent

  - name: add a node to an arbitrary group via consul inventory (see consul.ini)
    consul_kv:
      key: ansible/groups/dc1/somenode
      value: 'top_secret'

  - name: import json file with key values into consul kv store
    consul_kv:
      json: "configurations/env.json"
      host: "{{ consul_host }}"
      state: present
      token: "{{ environment_token }}"
    register: import_result

'''

import sys

try:
    import json
except ImportError:
    import simplejson as json

try:
    import consul
    from requests.exceptions import ConnectionError
    python_consul_installed = True
except ImportError, e:
    python_consul_installed = False

from requests.exceptions import ConnectionError


def execute(module):
    state = module.params.get('state')

    if state == 'acquire' or state == 'release':
        lock(module, state)
    if state == 'present':
        if module.params.get('json'):
            changed, result = import_values(module)
            module.exit_json(changed=changed, result=result)
        else:
            result = add_value(module)
            module.exit_json(changed=result["changed"], result=result)
    else:
        remove_value(module)


def lock(module, state):

    session = module.params.get('session')
    key = module.params.get('key')
    value = module.params.get('value')

    if not session:
        module.fail(
            msg='%s of lock for %s requested but no session supplied' %
            (state, key))

    if state == 'acquire':
        successful = consul_api.kv.put(key, value,
                                       cas=module.params.get('cas'),
                                       acquire=session,
                                       flags=module.params.get('flags'))
    else:
        successful = consul_api.kv.put(key, value,
                                       cas=module.params.get('cas'),
                                       release=session,
                                       flags=module.params.get('flags'))

    module.exit_json(changed=successful,
                     index=index,
                     key=key)


def import_values(module):
    result = list()
    changed = False
    try:
        key_values = json.loads(module.params.get('json'))
    except ValueError as e:
        try:
            with open(module.params.get('json'), 'r') as json_data:
                key_values = json.load(json_data)
                json_data.close()
        except Exception as e:
            module.fail_json(msg=str(e))
    except Exception as e:
        module.fail_json(msg=str(e))

    updated_dictionary = convert_dictionary(key_values)
    for k, v in updated_dictionary.iteritems():
        add_value_result = add_value(module, k, v)
        if add_value_result["changed"]:
            changed = add_value_result["changed"]

        result.append(add_value_result)

    return changed, result


def convert_dictionary(input_dict, path=""):
    new_dict = dict()
    for k, v in input_dict.items():
        new_key = path + k
        if isinstance(v, dict):
            new_dict.update(convert_dictionary(v, new_key + "/"))
        else:
            new_dict[new_key] = v
    return new_dict


def add_value(module, override_key=None, override_value=None):
    result = dict()
    result["changed"] = False
    consul_api = get_consul_api(module)

    key = override_key or module.params.get('key')
    value = override_value or module.params.get('value')

    result["key"] = key

    index, existing = consul_api.kv.get(key)

    changed = not existing or (existing and existing['Value'] != value)
    if changed and not module.check_mode:
        changed = bool(consul_api.kv.put(key, value,
                                            cas=module.params.get('cas'),
                                            flags=module.params.get('flags')))

    if module.params.get('retrieve'):
        result["index"], result["data"] = consul_api.kv.get(key)

    return result


def remove_value(module):
    ''' remove the value associated with the given key. if the recurse parameter
     is set then any key prefixed with the given key will be removed. '''
    consul_api = get_consul_api(module)

    key = module.params.get('key')
    value = module.params.get('value')

    index, existing = consul_api.kv.get(
        key, recurse=module.params.get('recurse'))

    changed = existing != None
    if changed and not module.check_mode:
        consul_api.kv.delete(key, module.params.get('recurse'))

    module.exit_json(changed=changed,
                     index=index,
                     key=key,
                     data=existing)


def get_consul_api(module, token=None):
    return consul.Consul(host=module.params.get('host'),
                         port=module.params.get('port'),
                         token=module.params.get('token'))


def test_dependencies(module):
    if not python_consul_installed:
        module.fail_json(msg="python-consul required for this module. "\
              "see http://python-consul.readthedocs.org/en/latest/#installation")


def main():

    argument_spec = dict(
        cas=dict(required=False),
        flags=dict(required=False),
        key=dict(required=False),
        host=dict(default='localhost'),
        port=dict(default=8500, type='int'),
        recurse=dict(required=False, type='bool'),
        retrieve=dict(required=False, default=True),
        state=dict(default='present', choices=['present', 'absent']),
        token=dict(required=False, default='anonymous', no_log=True),
        value=dict(required=False),
        json=dict(required=False),
    )

    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=False,
        mutually_exclusive=[
            ['json', 'key'],
            ['json', 'value'],
        ])

    test_dependencies(module)

    try:
        execute(module)
    except ConnectionError, e:
        module.fail_json(msg='Could not connect to consul agent at %s:%s, error was %s' % (
                            module.params.get('host'), module.params.get('port'), str(e)))
    except Exception, e:
        module.fail_json(msg=str(e))


# import module snippets
from ansible.module_utils.basic import *
if __name__ == '__main__':
    main()
