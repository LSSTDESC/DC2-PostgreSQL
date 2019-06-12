# Copyright (C) 2019  LSST Dark Energy Science Collaboration (DESC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import unittest

from lib.assumptions import Assumptions

class testAssumptions(unittest.TestCase):

    def setUp(self):
        self.yaml_file = 'test/assumptions.yaml'
        
    def test_table_read(self):
        assump = Assumptions(self.yaml_file)
        tables = assump.get_tables()

        for t in tables:
            print(t)


if __name__ == '__main__':
    unittest.main()

    
