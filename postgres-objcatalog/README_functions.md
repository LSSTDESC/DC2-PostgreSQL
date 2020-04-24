# Useful User Functions

These are of two types: those that involve tract/patch, and those aiding
spatial search using ra and dec.

## Tract/patch functions
**NOTE:** These functions depend on the fact that, so far, object ids are
encoded to include tract and patch information.  There is no guarantee this
will always be the case.

*   ```tract_from_object_id(object_id)
    returns: tract number, e.g. 4023 ```

*  ``` patch_from_object_id(object_id)
   returns: 100*patch_x + patch_y, e.g. 203 ```

*  ```patch_s_from_object_id(object_id)
   returns:  patch as string of the form 'x,y'  e.g., '2,3' ```

*   ```tractSearch(object_id, tract)
    returns: True if object_id is in the tract, else False ```

*   ```tractSearch(object_id, tractStart, tractEnd)
    returns: True if object_id is in any tract numerically between tractStart
    and tractEnd, inclusive; else False ```

## Spatial search
These functions make use of a special datatype, `earth`. It's implemented
as an array of 3 doubles, to be interpreted as a (Cartesian) point on the
unit sphere.  Tables like `object` which have position information are
implemented in Postgres with an indexed column of type `earth`.  For
`object` that column is named `coord`.   There are functions to go back
and forth between (ra, dec) and coord and functions to search, using one or
the other representation.

*
```
   coord_to_dec(coord)
   returns: declination
```

*
```
   coord_to_ra(coord)
   returns: right ascension
```
*
```
   radec_to_coord(ra, dec)
   returns: cordinates of type **earth**
   
```

*
```
   coneSearch(coord, ra, dec, radius)
   returns: True if coord is in the cone about (**ra**, **dec**)
   of radius **radius** (in arcseconds) ; else False
```

*
```
   boxSearch(coord, ra1, ra2, dec1, dec2)
   returns: true if coord is in the area on the sphere delimited by
   ra1, ra2, dec1 and dec2.
```   

## Examples
* tracts and patches
```
desc_dc2_drp=> select objectid, ra, dec, tract_from_object_id(objectid) as tract,
patch_s_from_object_id(objectid) as patch from run21i_v1.dpdd
where objectid=15156080594136793;
     objectid      |        ra        |        dec        |  tract | patch
-------------------+------------------+-------------------+--------+-------
 15156080594136793 | 59.0011560736368 | -40.0046486700794 |   3446 | 3,0
(1 row)
```
* searching
```
desc_dc2_drp=> select objectid, ra, dec from run21i_v1.dpdd
where conesearch(coord, 59.0, -40.0, 20);
     objectid      |        ra        |        dec        
-------------------+------------------+-------------------
 15156080594127840 | 58.9976535829802 | -40.0051982952461
 15156080594127893 | 59.0035922938577 | -40.0025182060955
 15156080594127944 | 58.9975850998315 |  -39.998291471669
 15156080594136767 | 59.0053029233318 | -40.0037647461018
 15156080594136792 | 59.0006302789276 | -40.0048016580834
 15156080594136793 | 59.0011560736368 | -40.0046486700794
 15156080594136856 | 58.9931049162365 | -40.0003878085877
 15156080594136884 | 58.9938169686714 |  -39.997577598651
(8 rows)
```




