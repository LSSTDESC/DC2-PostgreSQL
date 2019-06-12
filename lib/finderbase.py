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

class Finder(object):
    """
    Base class which describes interface for finding things in the data
    collection.  Classes inheriting from it should override all functions 
    and typically will have additional functions specific to the data 
    they access.
    """

    def __init__(self):
        pass
        
    def get_determiners(self):
        """
        Return a list of strings which are used as keyword arguments in
        subsequent 'get' routines in decreasing order of requiredness.
        For example, callers cannot supply the second one without also supply
        the first.
        """

        return None

    def get_determiner_dict(self, filepath):
        """
        @param  input data file
        @return dict of determiner values for specified file
        """
        return None

    def get_some_file(self):
        """
        Return file path to a 'typical' input file, from which data schema
        may be inferred
        """

        return None

    def get_some_image_file(self):
        """
        Return file path to a 'typical' input file, from which data schema
        may be inferred
        """

        return None
