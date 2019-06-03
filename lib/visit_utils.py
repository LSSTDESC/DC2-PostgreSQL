"""
Utilities for ingesting Stack data products into PostreSQL tables.
"""
from __future__ import absolute_import, print_function, division
import os
import sys
from collections import OrderedDict
import sqlite3
import psycopg2
import numpy as np
import astropy.io.fits as fits
import lsst.afw.math as afwMath
import lsst.daf.persistence as dp
##import lsst.utils as lsstUtils
##from .Pserv import create_csv_file_from_fits

__all__ = ['FluxCalibrator', 'make_ccdVisitId', 'create_table',
           'ingest_registry', 'ingest_calexp_info',
           'ingest_ForcedSource_data']
#           'ingest_ForcedSource_data', 'ingest_Object_data']

class FluxCalibrator(object):
    """
    Functor class to convert uncalibrated fluxes from a given exposure
    to nanomaggies.

    Attributes
    ----------
    zeroPoint : float
        Zero point in ADU for the exposure in question.
    """
    def __init__(self, zeroPoint):
        """
        Class constructor

        Parameters
        ----------
        zeroPoint : float
            Zero point in ADU for the exposure in question.
        """

        self.zeroPoint = zeroPoint

    def get_nanomaggies(self, flux):
        """
        Convert flux to nanomaggies.

        Parameters
        ----------
        flux : float
            Measure source flux in ADU.

        Returns
        -------
        float
            Source flux in nanomaggies.
        """
        return 1e9*flux/self.zeroPoint
#        if flux > 0:
#            return 1e9*flux/self.zeroPoint
#        else:
#            return np.nan

    def __call__(self, flux):
        """
        Convert flux to nanomaggies.

        Parameters
        ----------
        flux : float or sequence
            Measure source flux(es) in ADU.

        Returns
        -------
        float or np.array
            Source flux(es) in nanomaggies.
        """
        try:
            return np.array([self.get_nanomaggies(x) for x in flux])
        except TypeError:
            return self.get_nanomaggies(flux)

def make_ccdVisitId(visit, raft, sensor):
    """
    Create the ccdVisitId to be used in the CcdVisit table.

    Parameters
    ----------
    visit : int
         visitId of the desired visit.
    raft : str
         Identifier of the raft location in the LSST focal plane,
         e.g., 'R22'.
    sensor : str
         Identifier of the sensor location in a raft, e.g., 'S11'.

    Returns
    -------
    int
        An integer that uniquely identifies the visit-raft-sensor
        combination.
    """
    # There are around 2.5 million visits in the 10 year survey, so 7
    # digits should suffice for the visit part. 
    # (jrb mod to use 8 digits, as is done in forced file name.)
    #  Prepend the RRSS
    # combination to that and return as an int.
    #ccdVisitId = int(raft[:3:2] + sensor[:3:2] + "%07i" % visit)
    ccdVisitId = int(raft[1:3] + sensor[1:3] + "%08i" % visit)
    return ccdVisitId

def create_table(connection, table_name, schema='public', sql_path='.',
                 dry_run=False, clobber=False):
    """
    Create the specified table using the corresponding script in the
    sql subfolder.

    Parameters
    ----------
    connection : psycopg2.connection
        The connection object to use to create the table.
    table_name : str
        The table name corresponding to the script in the sql subfolder.
    schema_name : str
        If non-empty, created table will be identified as 
          schame_name.table_name
    dry_run : bool, optional
        If True, just print the table creation code to the screen, but
        don't execute.  The default value is False.
    clobber : bool, optional
        Overwrite the table if it already exists. Default: False
    sql_path : string, optional
        Defaults to current directory

    """
    schema_name = schema
    table_id = schema_name + '.' + table_name

    # Read in the script
    create_script = os.path.join(sql_path, 'create_%s.sql' % table_name)
    create_string = ''
    if clobber:
        create_string = 'drop table if exists %s; \n' % table_id
    more = True
    with    open(create_script, "r") as f:
        while more:
            line = f.readline()
            if line == '': 
                more = False
                break
            create_string += line

    # Put in schema name
    create_string = create_string.format(**locals())

    create_schema_string = 'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'.format(**locals())

    if dry_run:
        print('Create schema command: ')
        print(create_schema_string)
        print('Create table command: ')
        print(create_string)
        return

    with connection.cursor() as cursor:
        cursor.execute(create_schema_string)
        cursor.execute(create_string)
        connection.commit()
        print('Committed commands to create schema, table')
        
def ingest_registry(connection, registry_file, schema_name, dry_run=False):
    """
    Ingest some relevant data from a registry.sqlite3 file into
    the CcdVisit table.

    Parameters
    ----------
    connection : pscopg2.onnection
        The connection object to use to modify the CcdVisit table.
    registry_file : str
        The sqlite registry file containing the visit information.
    schema_name : str
        Will be prepended to table name ('ccdvisit')
    dry_run : bool
    """
    registry = sqlite3.connect(registry_file)
    query = """select dateObs, visit, filter, raftName, detectorName,
               expTime from raw  order by visit asc"""
    cnt=0

    with connection.cursor() as cursor:
        for row in registry.execute(query):
            dateObs, visit, filter_, raft, ccd, expTime = tuple(row)
            dateObs = dateObs[:len('2016-03-18 00:00:00.000000')]
            ccdVisitId = make_ccdVisitId(visit, raft, ccd)
            if cnt == 0:
                print('cnt: ', cnt)
                print('raft: ', raft)
                print('ccd: ', ccd)
                print('dateObs:  ',dateObs) 
                print('ccdVisitId: ', ccdVisitId)
            query = """insert into "%(schema_name)s".CcdVisit (ccdVisitId, visitId,ccdName,raftName,filterName,obsStart) values (%(ccdVisitId)i, %(visit)i, '%(ccd)s','%(raft)s', '%(filter_)s',
                   '%(dateObs)s')   """ % locals()
                   #on conflict (ccdvisitid)  do update set ccdVisitId=%(ccdVisitId)i,
                   #visitId=%(visit)i, ccdName='%(ccd)s',
                   #raftName='%(raft)s', filterName='%(filter_)s',
                   #obsStart='%(dateObs)s'  """ % locals()

            cnt += 1

            if dry_run:
                print("Query:  ")
                print(query)
                if cnt > 5: return
                #print("query:", query)
            else:
                try:
                    cursor.execute(query)
                except Exception as eobj:
                    print("query:", query)
                    raise eobj
                #if cnt > 5 : break
        print('Total count of inserted rows: ', cnt)
        connection.commit() 

def ingest_calexp_info(connection, repo, schema_name, dry_run=False,
                       min_visit=0, max_visit=100000000):
    """
    Extract information such as zeroPoint, seeing, sky background, sky
    noise, etc., from the calexp products and insert the values into
    the CcdVisit table.

    Parameters
    ----------
    connection : desc.pserv.DbConnection
        The connection object to use to modify the CcdVisit table.
    repo : str
        The path the output data repository used by the Stack.
    schema_name : str
        Will be prepended to table name ('ccdvisit')
    min_visit :   int
        Only process visits with id at least this big
    max_visit :   int
        If not None, only process for visits with id <= max_visit
    """
    # Use the Butler to find all of the visit/sensor combinations.
    butler = dp.Butler(repo)
    datarefs = butler.subset('calexp')

    # Possible alternate approach: 
    # query ccdvisit table to get list of visits between min_visit and max_visit
    # for each visit
    #    visitrefs = butler.subset('calexp', dataId={'visit' : the_visit})
    #    loop over these refs
    #    commit
    num_datarefs = len(datarefs)
    print('Ingesting %i visit/sensor combinations' % num_datarefs)
    sys.stdout.flush()
    nrows = 0
    with connection.cursor() as cursor:
        for dataref in datarefs:
            if dataref.dataId['visit'] < min_visit : continue
            if dataref.dataId['visit'] >  max_visit : continue
            if nrows % int(num_datarefs/20) == 0:
                sys.stdout.write('.')
                sys.stdout.flush()
            calexp = dataref.get('calexp')
            calexp_bg = dataref.get('calexpBackground')
            ccdVisitId = make_ccdVisitId(dataref.dataId['visit'],
                                         dataref.dataId['raftName'],
                                         dataref.dataId['detectorName'])

            # Compute zeroPoint, seeing, skyBg, skyNoise column values.
            try:
                zeroPoint = calexp.getPhotoCalib().instFluxToMagnitude(1)
            except:
                continue
            # For the psf_fwhm (=seeing) calculation, see
            # https://github.com/lsst/meas_deblender/blob/master/python/lsst/meas/deblender/deblend.py#L227
            try:
                pixel_scale = calexp.getWcs().getPixelScale().asArcseconds()
                seeing = (calexp.getPsf().computeShape().getDeterminantRadius()
                          *2.35*pixel_scale)
            except:
                seeing = -1000.0
            # Retrieving the nominal background image is computationally
            # expensive and just returns an interpolated version of the
            # stats_image (see
            # https://github.com/lsst/afw/blob/master/src/math/BackgroundMI.cc#L87),
            # so just get the stats image.
            #bg_image = calexp_bg.getImage()
            try:
                bg_image = calexp_bg[0][0].getStatsImage()
                skyBg = afwMath.makeStatistics(bg_image, afwMath.MEDIAN).getValue()
            except:
                skyBg = -1000.0
            try:
                skyNoise = afwMath.makeStatistics(calexp.getMaskedImage(),
                                                  afwMath.STDEVCLIP).getValue()
            except:
                skyNoise = -1000.0

            query = """update "%(schema_name)s".ccdvisit
                       set zeroPoint=%(zeroPoint)15.9e,
                       seeing=%(seeing)15.9e,
                       skyBg=%(skyBg)15.9e, skyNoise=%(skyNoise)15.9e
                       where ccdVisitId=%(ccdVisitId)i """ % locals()  
            if dry_run:
                print('Query for row ', nrows, ': ')
                print(query)
                if nrows > 5: return
            else:
                try: 
                    cursor.execute(query)
                except Exception as eobj:
                    print("query:", query)
                    raise eobj
            nrows += 1
    print('Total count of updated rows: ', nrows)
    connection.commit()

