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

import re
import os,sys

from  .finderbase import Finder

class ForcedSourceFinder(Finder):
    """
    Encapsulates information on file names and structure of file hierarchy
    containing forced source data.  That structure is:
    rootdir, containing subdirectories with names of the form
         <visit>-<filter>   
    where <visit> is an integer, zero-filled to some fixed length
    and   <filter> is one of a small set of strings. 
    Each visit directory has raft directories with names of the form
    Rmn    where m and n are digits   (also has other stuff which we ignore)
    Each raft directory has files with names of the form
      forced_<visit>-<filter>-<raft>-Sij-det
     
    """

    def __init__(self, rootdir, min_len=46080, dm_version=""):
        """
        Parameters
        ----------
        rootdir : str
            Path to root directory containing all forced source data
        min_len : int
            Files of this length have empty data tables but are otherwise
            complete
        db_version : str
            Ignored for now.  Could be used to select form of file
            hierarchy and naming conventions appropriate to a data set
        """
        self.rootdir = rootdir
        self.dm_version = ""

        self.filters = ['u', 'g', 'r', 'i', 'y', 'z' ]
        #self.visit_width = 8    # should use this instead of hard-coding
        self.visit_re = '([0-9]{8})'
        filter_string = ''.join(self.filters)
        self.visitdir_re = self.visit_re + '-[' + filter_string + ']'
        self.raft_re = 'R([0-4]{2})'
        self.ccd_re = 'S([0-2]{2})'
        self.basename_re = '-'.join(['forced_' + self.visitdir_re, self.raft_re,
                                     self.ccd_re, 'det[0-9]{3}.fits'])

        self.basename_fmt = '-'.join(['forced_{}','{}','{}'])
        self.dirs = [self.visitdir_re, self.raft_re]

        self.determiners = ['visit', 'raft', 'sensor']

        # Files of this length have no data
        self.min_len = min_len

        # Convenience to avoid recalculating
        self.__subdirs = []

    def get_determiners(self) :
        """
        Return a list of strings which are used as keyword arguments in
        subsequent 'get' routines in decreasing order of requiredness.
        For example, callers cannot supply the second one without also supply
        the first.
        """
        return self.determiners

    def get_determiner_dict(self, filepath):
        """
        Return determiners associated with a particular input file

        Parameters
        ----------
        filepath : str
            path to input file
        
        Returns
        -------
        dict
           keys are determiners as returned by get_determiners above
            
        """
        fbase = os.path.basename(filepath)
        print('get_determiner_dict:  basename ', fbase)
        m = re.fullmatch(self.basename_re, fbase)
        if m:
            d = {'visit' : m.group(1),
                 'raft' : m.group(2),
                 'sensor' : m.group(3)}
            print('Found visit ', d['visit'])
            return d
        raise ValueError('get_determiner_dict: bad filepath argument ' + filepath)

    def get_file_path(self, visit, raft=None, sensor=None) :
        """
        Depending on supplied arguments, return visit directory, visit/raft
        directory, or particular file for visit, raft, sensor

        Parameters
        ----------
           visit : int
           
           raft :  str
              if present, of form Rxx where xx are digits in range 0 - 4
           sensor : 
              if present, of form Sxx where xx are gidings in range 0 - 2
        """
        if (type(visit) != type(3) ):
            raise TypeError("get_file_path: bad visit argument: " + str(visit) )
        if raft is not None:
            if re.fullmatch(self.raft_re, raft) is None:
                raise ValueError("get_file_path: bad raft argument: " + str(raft))
            if sensor is not None:
                if re.fullmatch(self.ccd_re, sensor) is None:
                    raise ValueError("get_file_path: bad sensor argument: " + str(sensor))
        visitstr = '{:08}'.format(visit)
        if len(self.__subdirs) == 0:
            self.__subdirs = os.listdir(self.rootdir)
        visit_dir = None
        for s in self.__subdirs:
            if re.match(visitstr, s):
                visit_dir = s
                break
        if visit_dir is None: return None
        if raft is None:     # Only asked for visit directory
            return os.path.join(self.rootdir, visit_dir)

        raft_dir = os.path.join(self.rootdir,visit_dir, raft)
        if sensor is None:
            return raft_dir
        files = os.listdir(raft_dir)
        for f in files:
            starts_with = self.basename_fmt.format(visit_dir, raft, sensor)
            if re.match(starts_with, f) and re.fullmatch(self.basename_re, f):
                return os.path.join(raft_dir, f)

        return None

    def get_some_file(self) :
        """
        Call in case one just wants to determine the schema
        
        Returns
        -------
        tuple of full file path (str) and dict of determiners
        """

        if len(self.__subdirs) == 0:
            self.__subdirs = os.listdir(self.rootdir)
        for v in self.__subdirs:
            visit = v[:-2]    # strip off filter
            visit_dir = os.path.join(self.rootdir, v)
            rdirs = os.listdir(visit_dir)
            for r in rdirs:
                if re.fullmatch(self.raft_re, r):
                    raft = r[1:]
                    fs = os.listdir(os.path.join(visit_dir, r))
                    for f in fs:
                        #print('Found a file ', f)
                        m = re.fullmatch(self.basename_re, f)
                        if m:
                            d = {'visit' : m.group(1),
                                 'raft' : m.group(2),
                                 'sensor' : m.group(3)}
                            return os.path.join(visit_dir, r, f), d
                
        return None

    def get_visit_files(self, visit, nonempty=True):
        """
        Return a list of all data files belonging to a visit.

        Parameters
        ----------
        visit : int
            visit for which filepaths are requested
        nonempty : bool
            if true only return files of size greater than min length
            set at initialization

        Returns
        -------
        list of full filepaths (strings)
        """
        visitdir = self.get_file_path(visit)
        if visitdir is None: 
            return []
        print('File path for visit ', visit, ' is: ', visitdir)
        files = []
        for d in os.listdir(visitdir):
            if re.fullmatch(self.raft_re, d):
                rdir = os.path.join(visitdir, d)
                print('Found raft dir ',rdir)
                for f in os.listdir(rdir):
                    if re.fullmatch(self.basename_re, f):
                        if nonempty:
                            s = os.stat(os.path.join(rdir,f))
                            if (s.st_size <= self.min_len) : continue
                        files.append(os.path.join(rdir, f))
        return files

    def get_visits(self):
        """   
        Returns
        -------
        sorted list of ints, identifying visits

        """
        if len(self.__subdirs) == 0:
            self.__subdirs = os.listdir(self.rootdir)
        visits = []
        for v in self.__subdirs:
            visits.append(int(v[:-2])) # strip off filter

        visits.sort()
        return visits
