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

    
