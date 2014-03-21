# Copyright 2014 - Dark Secret Software Inc.
# All Rights Reserved.

import unittest

import dummy_event 
from notabene import flatten_notification


expected = [
    ('_context_request_id', 'req-1234'), 
    ('event_type', 'compute.instance.exists'), 
    ('timestamp', '2013-06-20 17:31:57.939614'), 
    ('_context_project_id', 'tenant-1'),
    ('publisher_id', 'compute.global.preprod-ord.ohthree.com'), 
    ('payload.status', 'saving'), 
    ('payload.container_format', 'ovf'), 
    ('payload.bandwidth.public.bw_in', 0), 
    ('payload.bandwidth.public.bw_out', 1697240969), 
    ('payload.bandwidth.private.bw_in', 0), 
    ('payload.bandwidth.private.bw_out', 264902), 
    ('payload.properties.image_type', 'snapshot'), 
    ('payload.tenant', '5877054'), 
    ('payload.options[0]', 'one'), 
    ('payload.options[1]', 'two'), 
    ('payload.options[2].region', 'springfield'), 
    ('payload.options[2].server', 'bart'), 
    ('payload.options[3]', 'three'), 
    ('payload.instance_id', 'ins-4567'), 
    ('payload.state', 'state'), 
    ('payload.image_meta.com.rackspace__1__options', 'rax_opt'), 
    ('payload.image_meta.org.openstack__1__os_version', 'os_version'), 
    ('payload.image_meta.org.openstack__1__os_distro', 'os_distro'), 
    ('payload.image_meta.org.openstack__1__architecture', 'os_arch'), 
    ('payload.new_task_state', 'task'), 
    ('payload.old_state', 'old_state'), 
    ('payload.old_task_state', 'old_task')
]


class TestFlatten(unittest.TestCase):

    def test_flatten(self):
        event = dummy_event.get_event()

        flat = []
        flatten_notification.flatten(event, flat)

        
        self.assertEqual(expected, flat)
