# dwcontents
# Copyright 2018 data.world, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License.
#
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# This product includes software developed at
# data.world, Inc.(http://data.world/).

from notebook.services.contents.tests.test_manager import TestContentsManager
from pytest import mark

from dwcontents.contents import DwContents
from dwcontents.utils import normalize_path


@mark.usefixtures('api_class')
class DwContentsSpecTest(TestContentsManager):
    def setUp(self):
        self.api = self.api_class()
        self.contents_manager = DwContents(
            root_dir='testy-tester/jupyter',
            compatibility_mode=True,
            api=self.api
        )

    def tearDown(self):
        pass

    def make_dir(self, api_path):
        dummy_file = normalize_path(api_path, 'dummy')
        self.api.upload_file('testy-tester', 'jupyter', dummy_file, '')
