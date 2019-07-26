# Copyright (C) 2016-2019  Sogo Mineo
# Copyright (C) 2019  LSST Dark Energy Science Collaboration (DESC)
#
# This file is part of the project DC2-PostgreSQL
# DC2-PostgreSQL is free software: you can redistribute it and/or modify
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
#
# This file has been significantly modified for use with DESC simulated data by
# LSST Dark Energy Science Collaboration (DESC)

import collections
import glob
import os
import re

import psycopg2

import numpy as np

from . import config
from . import libdb


def get_image_path(rerunDir, tract, patch, filter):
    """
    Get the path to the stacked image identified by the arguments.
    The file may be compressed, but the path is virtualized so it always ends with '.fits'.
    """
    if patch == "*":
        x, y = "*", "*"
    else:
        x, y = patch // 100, patch % 100

    if config.withSkymapWcs:
        return os.path.join(
            config.withSkymapWcs, "skymap_wcs-{tract}.fits".format(**locals())
        )
    else:
        #return "{rerunDir}/deepCoadd/{filter}/{tract}/{x},{y}/calexp-{filter}-{tract}-{x},{y}.fits".format(**locals())
        return "{rerunDir}/deepCoadd-results/{filter}/{tract}/{x},{y}/calexp-{filter}-{tract}-{x},{y}.fits".format(**locals())



def get_existing_filters(rerunDir, hsc=False):
    """
    Search "rerunDir" and return existing filters in it.
    @param rerunDir
        Path to the rerun directory from which to generate the master table
    @return
        List of filter names, sorted.
    """

    filters = os.listdir("{rerunDir}/deepCoadd-results".format(**locals()))
    if hsc:
        return sorted((f for f in filters if re.match(r'^(?:HSC-.*|NB.*)$', f)),
                      key=lambda f: filterOrder[f])
    else:
        return sorted((f for f in filters if re.match(r'^[a-z]$', f)))



def get_existing_tracts(rerunDir):
    """
    Search "rerunDir" and return existing tracts in it.
    @param rerunDir
        Path to the rerun directory from which to generate the master table
    @return
        List of tract numbers, sorted.
    """
    tracts = [os.path.basename(path) for path in glob.iglob("{rerunDir}/deepCoadd-results/*/*".format(**locals()))]

    return sorted(set(
        int(tract) for tract in tracts if re.match(r'^[0-9]+$', tract)
    ) )

def patch_to_number(patchStr):
    """
    Convert "x,y" to 100*x + y
    """
    x, y = patchStr.split(',')
    return int(x)*100 + int(y)


def path_decompose(path):
    """
    Decompose a given file path to (tract, patch, filter) or (trct, patch)
    @return
        (tract, patch, filter) or (trct, patch)
    """
    basename = os.path.basename(path)
    #print("In path_decompose called with path=", path)
    #print("Computed basename=", basename)
    # For LSST forced source filepaths have only 'forced', not 'forced_src'
    # Except for run 2.1i they *do* have forced_src, so allow for either one
    #m = re.match(r'^(?:calexp|forced_src|meas|ran|forced_src_undeblendedConvolved)-(HSC-\w+|NB-?\w+)-([0-9]+)-([0-9]+),([0-9]+)\.fits(?:\.gz)?$', basename)
    m = re.match(r'^(?:calexp|forced|forced_src|meas|ran|forced_src_undeblendedConvolved)-([a-z])-([0-9]+)-([0-9]+),([0-9]+)\.fits(?:\.gz)?$', basename)
    if m:
        filter, tract, x, y = m.groups()
        patch = int(x)*100 + int(y)
        return int(tract), patch, filter
    m = re.match(r'^ref-([0-9]+)-([0-9]+),([0-9]+)\.fits(?:\.gz)?$', basename)
    if m:
        tract, x, y = m.groups()
        patch = int(x)*100 + int(y)
        return int(tract), patch

    raise RuntimeError("Path cannot be decomposed: " + path)


def path_exists(path):
    """
    os.path.exists() with gzip compression considered
    """
    return os.path.exists(path) or os.path.exists(path + ".gz")


def new_db_connection():
    """
    Create a connection to the database.
    """
    if config.NDEBUG:
        return psycopg2.connect(**config.dbServer)
    else:
        return libdb.DBConnectionDebug(psycopg2.connect(**config.dbServer))


def db_table_exists(schema_name, table_name):
    """
    Return True or False accordingly
    """

    db = lib.common.new_db_connection()

    ret = False
    with db.cursor() as cursor:
        try:
            cursor.execute('SELECT 0 FROM "{schemaName}"."{table_name}" WHERE FALSE;'.format(**locals()))
            ret = True
        except psycopg2.ProgrammingError:
            db.rollback()
        finally:
            db.close()
    return ret

# Not needed for LSST DC2 data.  Short names are always used.
filterToShortName = collections.OrderedDict([
#    ("HSC-G" , "g"    ),
#    ("HSC-R" , "r"    ),
#    ("HSC-I" , "i"    ),
#    ("HSC-Z" , "z"    ),
#    ("HSC-Y" , "y"    ),
#    ("NB0387", "n387" ),
#    ("NB0515", "n515" ),
#    ("NB0816", "n816" ),
#    ("NB0921", "n921" ),
#    ("NB1010", "n1010"),
    ("g", "g"),
    ("r", "r"),
    ("i", "i"),
    ("z", "z"),
    ("y", "y"),
    ("u", "u"),
])

filterOrder = {
    value: key
    for key, value in enumerate(filterToShortName.keys())
}

absorptionCoeff = {
    "HSC-G"  : 3.240,
    "HSC-R"  : 2.276,
    "HSC-I"  : 1.633,
    "HSC-Z"  : 1.263,
    "HSC-Y"  : 1.075,
    "NB0387" : 4.007,
    "NB0468" : 3.351,
    "NB0515" : 2.939,
    "NB0527" : 2.855,
    "NB0656" : 2.077,
    "NB0718" : 1.812,
    "NB0816" : 1.458,
    "NB0921" : 1.187,
    "NB0973" : 1.083,
    "NB1010" : 1.013,
    "IB0945" : 1.134,
}

defaultPixelScale = 0.168
defaultZeroPoint = 27.0

def flux_to_mag(flux, zero_point=defaultZeroPoint):
    return (zero_point - (2.5 * np.log10(flux)))

def flux_to_magerr(flux, fluxerr):
    return (  (2.5/np.log(10)) * (fluxerr / flux) )
    
