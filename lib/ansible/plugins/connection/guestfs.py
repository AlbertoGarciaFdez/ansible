# Based on local.py (c) 2012, Michael DeHaan <michael.dehaan@gmail.com>
# (c) 2013, Maykel Moya <mmoya@speedyrails.com>
# (c) 2015, Toshio Kuratomi <tkuratomi@ansible.com>
# (c) 2015, Michael Scherer <mscherer@redhat.com>
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
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

import os
import os.path
import guestfs
import json
import re

from ansible import constants as C
from ansible.errors import AnsibleError
from ansible.plugins.connection import ConnectionBase

try:
    from __main__ import display
except ImportError:
    from ansible.utils.display import Display
    display = Display()

BUFSIZE = 65536
EXECUTION_SCRIPT = """
import sys
import json
import os
from subprocess import PIPE, Popen

cmd_file = sys.argv[1]
if not os.path.exists(cmd_file):
    sys.exit(1)

cmd = json.loads(open(cmd_file).read())

r = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
(stdout, stderr) =  r.communicate()

output = {
    "rc": r.returncode,
    "stdout": stdout.decode('UTF-8'),
    "stderr": stderr.decode('UTF-8'),
}
json.dump(output, open(cmd_file + ".out","w"))
"""


class Connection(ConnectionBase):
    ''' Libguestfs based connections '''

    transport = 'guestfs'
    has_pipelining = True
    # su currently has an undiagnosed issue with calculating the file
    # checksums (so copy, for instance, doesn't work right)
    # Have to look into that before re-enabling this
    become_methods = frozenset(C.BECOME_METHODS).difference(('su',))

    def __init__(self, play_context, new_stdin, *args, **kwargs):
        super(Connection, self).__init__(play_context, new_stdin,
                                         *args, **kwargs)

        self.disk = self._play_context.remote_addr

        if not os.path.isfile(self.disk):
            raise AnsibleError("%s is not a file" % self.disk)

        self.guestfs = guestfs.GuestFS(python_return_dict=True)

        self.guestfs.add_drive_opts(self.disk)

    def _connect(self):
        ''' connect to the chroot; nothing to do here '''
        super(Connection, self)._connect()
        if not self._connected:

            self.guestfs.set_network(True)
            self.guestfs.launch()
            #   set_selinux ?
            # code taken from the manpages
            roots = self.guestfs.inspect_os()
            if len(roots) != 1:
                raise AnsibleError("%s has more than one OS,"
                                   "aborting" % self.disk)

            root = roots[0]
            mps = self.guestfs.inspect_get_mountpoints(root)

            def compare(a, b):
                return len(a) - len(b)

            for device in sorted(mps.keys(), compare):
                try:
                    self.guestfs.mount(mps[device], device)
                except RuntimeError as msg:
                    print("%s (ignored)" % msg)

            #TODO add some verification
            for tmp in ('/run', '/tmp'):
                if self.guestfs.is_dir(tmp):
                    self.guestfs.sh("mount -t tmpfs -o size=4M tmpfs %s" % tmp)
                    self.tmp = tmp
                    break
            #TODO make sure that at least one did succeed
            self.guestfs.write(self._script_name(), EXECUTION_SCRIPT)
            for p in ('python', 'python3'):
                python_version = self.guestfs.sh('%s --version' % p)
                # check for error message
                if re.match(python_version, '^Python \d.\d+\.\d+$'):
                    break
            self._python = p
            self._connected = True

    # TODO randomize the filename ?
    def _script_name(self):
        return '%s/script' % self.tmp

    def _cmd_name(self):
        return '%s/cmd' % self.tmp

    def _cmd_result_name(self):
        return '%s/cmd.out' % self.tmp

    def exec_command(self, cmd, in_data=None, sudoable=False):
        #TODO handle sudo ?
        ''' run a command on the chroot '''
        super(Connection, self).exec_command(cmd, in_data=in_data,
                                             sudoable=sudoable)

        self.guestfs.write(self._cmd_name(), json.dumps(cmd))
        result = self.guestfs.sh('%s %s %s' % (self._python,
                                               self._script_name(),
                                               self._cmd_name()))
        if result:
            print(result)
            #TODO abort ?
        r = json.loads(self.guestfs.read_file(self._cmd_result_name()))
        # TODO remove the result file
        return (r['rc'], r['stdout'], r['stderr'])

    def put_file(self, in_path, out_path):
        ''' transfer a file from local to chroot '''
        super(Connection, self).put_file(in_path, out_path)
        display.vvv("PUT %s TO %s" % (in_path, out_path), host=self.disk)
        self.guestfs.upload(in_path, out_path)

    def fetch_file(self, in_path, out_path):
        ''' fetch a file from chroot to local '''
        super(Connection, self).fetch_file(in_path, out_path)
        display.vvv("FETCH %s TO %s" % (in_path, out_path), host=self.chroot)
        self.guestfs.download(in_path, out_path)

    def close(self):
        ''' terminate the connection; nothing to do here '''
        super(Connection, self).close()
        self.guestfish.umount_all()
        #TODO something else ?
        # stop the VM ?
        self._connected = False
