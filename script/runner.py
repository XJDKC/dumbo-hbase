#/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Peng Chao
# Copyright:
# Date:
# Distributed under terms of the license.

import sys
import os
from ConfigParser import SafeConfigParser


class DumboCommand(object):
    def __init__(self, configfile):
        self.config = SafeConfigParser()
        self.config.read(configfile)
        self.main_file = ''
        self.hbase_input = False
        self.hbase_output = False
        self.params = []

    def read_config(self):
        self.set_env()
        self.set_main()
        self.set_hbase()
        self.set_dependencies()

    def set_env(self):
        envs = dict(self.config.items('env'))
        if 'hadoop' in envs:
            self.set_param(envs, 'hadoop')

        if 'hadooplib' in envs:
            self.set_param(envs, 'hadooplib')

    def set_main(self):
        dumbos = dict(self.config.items('main'))
        if 'main_file' not in dumbos:
            print 'main_file not found'
            sys.exit(1)
        self.main_file = dumbos['main_file']

        if 'output' not in dumbos:
            print 'output not found'
            sys.exit(1)

        if 'hbase_input' in dumbos and dumbos['hbase_input'] == 'yes':
            self.params.extend(['-inputformat', 'com.dumbomr.mapred.TypedBytesTableInputFormat'])
            self.hbase_input = True

        if 'hbase_output' in dumbos and dumbos['hbase_output'] == 'yes':
            self.params.extend(['-outputformat', 'com.dumbomr.mapred.TypedBytesTableOutputFormat'])
            self.hbase_output = True
        elif 'outputformat' in dumbos:
            self.set_param(dumbos, 'outputformat')

        for param in ['output', 'overwrite', 'nummaptasks', 'numreducetasks', 'memlimit']:
            if param in dumbos:
                self.set_param(dumbos, param)

    def set_param(self, conf, key):
        self.params.extend(['-' + key, conf[key]])

    def set_hbase(self):
        params = dict(self.config.items('hbase'))
        if self.hbase_input or self.hbase_output:
            self.params.extend(['-hadoopconf', 'hbase.zookeeper.quorum=' + params['hbase.zookeeper.quorum']])
        else:
            return

        if self.hbase_input:
            self.set_param(params, 'input')
            if 'columns' in params:
                self.params.extend(['-hadoopconf', 'columns=' + params['columns']])
            elif 'columnfamilies' in params:
                self.params.extend(['-hadoopconf', 'columnfamilies=' + params['columnfamilies']])

        if self.hbase_output:
            self.params.extend(['-hadoopconf', 'output=' + params['output']])

    def set_dependencies(self):
        for f in self.read_values('dependence'):
            self.params.extend(['-file', f])

        for lib in self.read_values('libegg'):
            self.params.extend(['-libegg', lib])

    def read_values(self, section):
        return [x[1] for x in self.config.items(section)]

    def get_command(self):
        cmds = ["dumbo start"]
        cmds.append(self.main_file)
        cmds.extend(self.params)
        return ' '.join(cmds)

if __name__ == '__main__':
    cmd = DumboCommand(sys.argv[1])
    cmd.read_config()
    cmd_str = cmd.get_command()
    print cmd_str
    os.system(cmd_str)
