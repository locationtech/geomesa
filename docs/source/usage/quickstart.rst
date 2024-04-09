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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
=======
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
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
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
=======
=======
=======
>>>>>>> locatelli-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 1048b37894 (d)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
pipe data to the utility, for instance by using the ``echo`` command:
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> location-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> locationtech-main
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
pipe data to the utility, for instance by using the ``echo`` command:
=======
pipe data to the utility, for instance by using the :program:`echo` command:
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
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
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
=======
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> location-main
=======
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
=======
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
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> locationtech-main
=======
=======
>>>>>>> locationtech-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> 153df87aaa (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> location-main
=======
<<<<<<< HEAD
<<<<<<< HEAD
=======
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> b609c280f5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 5c88d70ae3 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c14b8214ca (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 86ae7cc523 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> locatelli-main
=======
pipe data to the utility, for instance by using the ``echo`` command:
=======
pipe data to the utility, for instance by using the :program:`echo` command:
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
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
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> ebd1587dc5 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
<<<<<<< HEAD
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> locatelli-main
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 9b68b76b81 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
=======
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
>>>>>>> locatelli-main
<<<<<<< HEAD
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> 208fcbd5e7 (Merge pull request #3524 from cffk/merid-update-fix)
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 6302ff2adf (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 987375068c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 1048b37894 (d)
=======
>>>>>>> a029d873e8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> eee860d65b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> c292d42888 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
pipe data to the utility, for instance by using the ``echo`` command:
=======
pipe data to the utility, for instance by using the :program:`echo` command:
<<<<<<< HEAD
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> bb15f534d5 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 491ae81179 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 664f11a970 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 8a24938f25 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 7a0a2d29d7 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 6e4203f66c (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 0c3226c442 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> f36c8520e2 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 80ac813585 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 0676d39969 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 12260fc11b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> aa22876329 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 094787b30a (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> c63e6e91bd (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 3bd0f77d39 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 938d194c93 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 748ccdbcc6 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> a8fbb11965 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 10b5e77237 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> f00ffe609e (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
>>>>>>> c59e00e4f (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 21f069ac96 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 68b6f2f484 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 9172626758 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 4dfac16980 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 13395ba73 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
>>>>>>> 69116cc937 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> f2702b99ef (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 6a0b9804ba (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
>>>>>>> 48bc392fd3 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
=======
=======
=======
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> 360db021b (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 3771d4aec1 (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> b5f4d47b2b (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> c8fb3456cf (Merge pull request #3524 from cffk/merid-update-fix)
<<<<<<< HEAD
>>>>>>> 25ac262782 (Merge pull request #3524 from cffk/merid-update-fix)
=======
=======
=======
=======
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> eee860d65 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d55f305b24 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> d0c8565c4b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> ba006ad292 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
>>>>>>> 6f78b76c12 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> dbc4ff68b0 (Merge pull request #3524 from cffk/merid-update-fix)
=======
>>>>>>> 26891e6f2b (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> locationtech-main
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
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
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> locationtech-main
=======
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
>>>>>>> 885e4882b8 (Merge pull request #3524 from cffk/merid-update-fix)
=======
pipe data to the utility, for instance by using the :program:`echo` command:
>>>>>>> 0a2f6458d1 (Merge pull request #3524 from cffk/merid-update-fix)
=======
pipe data to the utility, for instance by using the ``echo`` command:
>>>>>>> 9df6fd0323 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main
=======
>>>>>>> locatelli-main

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
