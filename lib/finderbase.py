
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
