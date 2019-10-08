# Copyright (C) 2019  LSST Dark Energy Science Collaboration (DESC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This file is part of the project DC2-PostgreSQL
# DC2-PostgreSQL is free software: you can redistribute it and/or modify
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from yaml import load as yload
from yaml import FullLoader
import re
from .misc import PoppingOrderedDict
from .misc import warning
from .sourcetable import Field
from .dbimage import DbImage
from .expressions import rpn_eval, rpn_eval_array
import numpy as np

class Assumptions(object):
    """
    Maintain information describing a priori assumptions about
    tables in a database and inputs which may or may not be used to
    generate columnes.   Typically initialize by reading a yaml file
    with up to three dict entries for symbols (governing possible 
    substitutions in column names), ignores (describe input columns
    to ignore) and tables.  

    """

    def __init__(self, infile):
        """
        Initialize an Assumptions object
        
        Parameters
        ----------
        infile : str
            yaml file describing assumptions

        """
        self.inf = infile        # string or  file pointer to yaml text file
        self.parsed = None
        self.ignores = None            # store compiled
        # will be dict of table info.  One entry per table. Value is
        # a dict of fields,  key =  name, but without data
        self.finals = None 
        #self.constraints = None      constraints are per-table

    def parse(self):
        """
        Store information from yaml file internally

        """
        if self.parsed:  return self.parsed

        with open(self.inf) as f:
            self.parsed = yload(f, Loader=FullLoader)
        self._verify()

    def _verify(self):
        """
        Check that parsed input is not totally off the wall.
        Could do more.
        """
        parsed = self.parsed
        if type(parsed) != type({}):
            raise TypeError("Input is not a dict")
        for k in parsed:
            if k not in ['ignores', 'tables']:
                warning("Assumptions.__verify: Unknown field ", k,
                        " will be ignored ")
                continue
            if k == 'ignores' and type(parsed[k]) != type([]):
                raise TypeException("Contents of {} is not a list!".format(k))
            if k == 'tables':
                if type(parsed[k]) != type({}):
                    raise TypeException("Contents of {} is not a dict!".format(k))
                for t_name, t_elt in parsed['tables'].items():
                    if type(t_elt) != type({}):
                        raise TypeException("Improper table definition")

                    #for field in t_elt['table']:
                    #    print('key: ',str(field),' value: ', str(t_elt['table'][field]) )
    def apply(self, raw,  schema_name, **kw):
        """
        'Apply' assumptions to information from an input data file

        Parameters
        ----------
        raw  : SourceTable instance 
             Typically initialized from FITS file
        schema_name : str
        kw   :  dict
            Key-value pairs which may be assoc. with input but not explicitly
            as fields in the SourceTable, such as e.g. visit, raft and sensor

        Returns
        -------
        dict of DbImages, keyed by table name.

        For now only handle case where everything goes in a single DbImage

        """
        print('assumptions.apply kw arguments: ')
        for k in kw:
            print('key is ', k, ' value is ', kw[k])

        # apply ignores
        self._compile_ignores()
        if self.ignores != None:
            remaining = PoppingOrderedDict()
            for key, f in raw.fields.items():
                matched = False
                for p in self.ignores:
                    if p.fullmatch(key):
                        matched = True
                        break;
                if not matched: remaining[key] = f



        #  The dict `remaining` now contains only fields we expect to use.
        #  field name is used for dict key, as in SourceTable.fields

        #  Compute data length from the first field, needed for compute fields
        for k in remaining:
            data_len = len(remaining[k].data)
            break

        fields = PoppingOrderedDict()
        table_name = list(self.parsed['tables'].keys())[0]
        table_def = self.parsed['tables'][table_name]

        column_dicts, column_group_dicts = self._get_names(table_def)

        for key, f in remaining.items():
            # check each one matches a column name or column group in our table
            # (For now assume we have only one table)
            matched = False
            for i in range(len(column_dicts)):
                if column_dicts[i]['name'] == key:
                    matched = True
                    fields[key] = f
                    del(column_dicts[i])
                    break
            if matched: continue
            for c in column_group_dicts:
                if re.fullmatch(c['name_re'], key):
                    matched = True
                    fields[key] = f
                    break
            if not matched:
                print("Column ", key, " unknown to Assumptions file")
                # and maybe raise exception?


        # If there are any entries left in column_dicts they better have
        # the compute or compute_array attribute
        for d in column_dicts:
            if 'compute' in d:
                c_list = d['compute']
                cf_list = []
                for i in range(len(c_list)):
                    cf_list.append(str(c_list[i]).format(**kw))

                s_val = rpn_eval([], cf_list)
                print('computed value ', s_val)
                val = s_val
                nptype = np.int64     # default
                if d['dtype'] == 'int8' : nptype = np.int8
                dat = np.full([data_len], int(val), nptype)


                field = Field(d['name'], d['type'], None, dat, d['doc'], 
                              d['compute'])
                fields[d['name']] = field
            else:
                if 'compute_array' in d:
                    dat = rpn_eval_array(d['inputs'], d['compute_array'],
                                         raw.fields)
                    field = Field(d['name'], d['type'], None, dat, d['doc'],
                                  None)
                    fields[d['name']] = field
                else:
                    print("Field ", key, 
                          ", known to Assumptions, not found in input")


        dbimage = DbImage(table_name, fields, schema_name)
        dbimage.set_filters([""])
        constraints = self._get_constraints(table_name)
        foreign_list = []
        index_list = []
        for c in constraints:
            if c['constraint_type'] == 'fk':
                foreign_list.append(c)
            else:
                index_list.append(c)

        dbimage.accept_foreign(foreign_list)
        dbimage.accept_indexes(index_list)

        # If we know about double precision fields which should stay
        # double precision, this would be the place to call
        # dbimage.append_doubles(list-of-names)

        self.finals = PoppingOrderedDict()    
        self.finals[table_name] = dbimage
        
        return self.finals
        
    def _get_names(self, assump_table):
        """
        Parameter
        ---------
        The part of the parsed assumptions belonging to a particular table

        Returns
        -------
        two lists: one of column names and one of column_group names
        """
        columns = assump_table['columns']
        column_names = []
        column_group_names = []
        for c in columns:
            if c['column_type'] == 'column' :
                column_names.append(c)
            else:
                column_group_names.append(c)
        return column_names, column_group_names

    def _get_ignores(self):
        if not self.parsed: self.parse()
        
        if 'ignores' in self.parsed: return self.parsed['ignores']
        return None

    def _compile_ignores(self):
        igs = self._get_ignores()
        if igs == None: return
        self.ignores = []
        for ig in igs:
            self.ignores.append(re.compile(ig))

    def get_tables(self):
        """
        Returns
        -------
        list of strings
           Each string is a table name mentioned in the Assumptions
        """
        if not self.parsed: self.parse()
        if 'tables' not in self.parsed: return None

        return list(self.parsed['tables'].keys())

    def _get_constraints(self, table_name):
        if not self.parsed: self.parse()

        if table_name in self.parsed['tables']:
            our_table = self.parsed['tables'][table_name]
            return our_table['constraints']

        return None

    #### Probably don't need either of get_foreign_keys or get_indexes
    def get_foreign_keys(self, table_name):
        """
        Parameters
        ----------
        table_name : str

        Returns
        _______
        A list of foreign key definitions
        """
        constraints = _get_constraints(table_name)
        if constraints is None: return []
        foreign = []
        for c in constraints:
            if c['constraint_type'] == 'fk':
                foreign.append(c)
        return foreign


    def get_indexes(self, table_name):
        """
        Parameters
        ----------
        table_name : str

        Returns
        -------
        A list of index definitions 
        """
        return []                #   **** TO-DO ****
