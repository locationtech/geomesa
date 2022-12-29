.. _quickstart:

================================================================================
Quick start
================================================================================

Coordinate transformations are defined by, what in PROJ terminology is
known as, "proj-strings". A proj-string describes any transformation regardless of
how simple or complicated it might be. The simplest case is projection of geodetic
coordinates. This section focuses on the simpler cases and introduces the basic
anatomy of the proj-string. The complex cases are discussed in
:doc:`transformation`.

A proj-strings holds the parameters of a given coordinate transformation, e.g.

::

    +proj=merc +lat_ts=56.5 +ellps=GRS80

I.e. a proj-string consists of a projection specifier, ``+proj``, a number of
parameters that applies to the projection and, if needed, a description of a
datum shift. In the example above geodetic coordinates are transformed to
projected space with the :doc:`Mercator projection<../operations/projections/merc>` with
the latitude of true scale at 56.5 degrees north on the GRS80 ellipsoid. Every
projection in PROJ is identified by a shorthand such as ``merc`` in the above
example.

By using the  above projection definition as parameters for the command line
utility :program:`proj` we can convert the geodetic coordinates to projected space:

::

    $ proj +proj=merc +lat_ts=56.5 +ellps=GRS80

If called as above :program:`proj` will be in interactive mode, letting you
type the input data manually and getting a response presented on screen.
:program:`proj` works as any UNIX filter though, which means that you can also
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
pipe data to the utility, for instance by using the ``echo`` command:
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
pipe data to the utility, for instance by using the ``echo`` command:
=======
pipe data to the utility, for instance by using the :program:`echo` command:
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)

::

    $ echo 55.2 12.2 | proj +proj=merc +lat_ts=56.5 +ellps=GRS80
    3399483.80      752085.60


PROJ also comes bundled with the :program:`cs2cs` utility which is used to
transform from one coordinate reference system to another. Say we want to
convert the above Mercator coordinates to UTM, we can do that with
:program:`cs2cs`:

::

    $ echo 3399483.80 752085.60 | cs2cs +proj=merc +lat_ts=56.5 +ellps=GRS80 +to +proj=utm +zone=32
    6103992.36      1924052.47 0.00

Notice the ``+to`` parameter that separates the source and destination
projection definitions.

If you happen to know the EPSG identifiers for the two coordinates reference
systems you are transforming between you can use those with :program:`cs2cs`:

::

   $ echo 56 12 | cs2cs +init=epsg:4326 +to +init=epsg:25832
   231950.54      1920310.71 0.00

In the above example we transform geodetic coordinates in the WGS84 reference
frame to UTM zone 32N coordinates in the ETRS89 reference frame.
UTM coordinates 
