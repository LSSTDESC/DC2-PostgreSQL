# Copyright notice??
from yaml import load as yload
import re

class Assumptions(object):
    """
    Maintain information describing a priori assumptions about
    tables in a database and inputs which may or may not be used to
    generate columnes.   Typically initialize by reading a yaml file
    with up to three dict entries for symbols (governing possible 
    substitutions in column names), ignores (describe input columns
    to ignore) and tables.  

    """
    def __init__(self, infile, schema):
        self.inf = infile        # string or  file pointer to yaml text file
        self.schema = schema
        self.parsed = None

    def parse(self):
        if self.parsed:  return self.parsed

        with open(self.inf) as f:
            self.parsed = yload(f)
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
            if k not in ['symbols', 'ignores', 'tables']:
                print("Assumptions.__verify: Unknown field ", k,
                      " will be ignored ")
                continue
            if type(parsed[k]) != type([]):
                raise TypeException("Contents of {} is not a list!".format(k))
            if k == 'tables':
                for t_elt in parsed['tables']:
                    if type(t_elt) != type({}):
                        raise TypeException("Improper table definition")
                    if 'table' not in t_elt:
                        raise ValueException("Improper table defintion")
                    if 'name' not in t_elt['table']:
                        raise TypeException("Table definition missing name field")
                    #for field in t_elt['table']:
                    #    print('key: ',str(field),' value: ', str(t_elt['table'][field]) )
    def get_schema(self):
        return self.schema

    def get_ignores(self):
        if not self.parsed: self.parse()
        
        if 'ignores' in self.parsed: return self.parsed['ignores']
        return None

    def get_tables(self):
        if not self.parsed: self.parse()
        if 'tables' not in self.parsed: return None

        table_names = []
        for t in self.parsed['tables']:
            table_names.append(t['table']['name'])
        
        return table_names

if __name__=='__main__':
    import sys
    if len(sys.argv) > 1:
        yaml_file = sys.argv[1]
    else:
        yaml_file = 'assumptions.yaml'

    schema = 'run12p_v4'
    
    assump = Assumptions(yaml_file, schema)
    #assump.parse()

    tables = assump.get_tables()

    for t in tables:
        print(t)
