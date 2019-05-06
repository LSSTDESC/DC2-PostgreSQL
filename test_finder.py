import unittest

from lib.forcedsource_finder import ForcedSourceFinder

class testFinder(unittest.TestCase):

    def setUp(self):
        self.finder = ForcedSourceFinder('/global/cscratch1/sd/desc/DC2/data/Run1.2p/w_2018_39/rerun/coadd-v4/forced')

    def test_some_file(self):
        aFile,dets = self.finder.get_some_file()
        if aFile is not None:
            print("Found file " + aFile)


    def test_per_visit(self):
        visit = 210472
        visitdir = self.finder.get_file_path(visit)
        if visitdir is not None:
            print("Found visit dir: ", visitdir)
            raftdir = self.finder.get_file_path(visit, raft='R01')
            if raftdir is not None:
                print("Found raft dir: ", raftdir)
                f = self.finder.get_file_path(visit, raft='R01', sensor='S20')
                if f is not None:
                    print("Found file: ", f)

    def test_get_visits(self):
        visits = self.finder.get_visits()
        print(len(visits), ' visits found')
        for i in range(20):
            print('visit[{}] is {}'.format(i,visits[i]))

if __name__ == '__main__':
    unittest.main()

