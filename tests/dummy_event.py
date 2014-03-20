# Copyright 2014 - Dark Secret Software Inc.
# All Rights Reserved.


def get_event():
    return {"event_type": "compute.instance.exists",
            '_context_request_id': "req-1234",
            '_context_project_id': "tenant-1",
            "timestamp": "2013-06-20 17:31:57.939614",
            "publisher_id": "compute.global.preprod-ord.ohthree.com",
            "payload": {
                'instance_id': "ins-4567",
                "status": "saving",
                "container_format": "ovf",
                "properties": {
                    "image_type": "snapshot",
                },
                "options": [
                    "one",
                    "two",
                    {"server": "bart",
                     "region": "springfield"},
                    "three"
                ],
                "tenant": "5877054",
                "old_state": 'old_state',
                "old_task_state": 'old_task',
                "image_meta": {
                    "org.openstack__1__architecture": 'os_arch',
                    "org.openstack__1__os_distro": 'os_distro',
                    "org.openstack__1__os_version": 'os_version',
                    "com.rackspace__1__options": 'rax_opt',
                },
                "state": 'state',
                "new_task_state": 'task',
                "bandwidth": {
                    "private": {"bw_in": 0, "bw_out": 264902},
                    "public": {"bw_in": 0, "bw_out": 1697240969}
                }
            }
        }
