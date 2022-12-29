.. _gnom:

********************************************************************************
Gnomonic
********************************************************************************

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
For a sphere, the gnomonic projection is a projection from the center of
the sphere onto a plane tangent to the center point of the projection.
This projects great circles to straight lines.  For an ellipsoid, it is
the limit of a doubly azimuthal projection, a projection where the
azimuths from 2 points are preserved, as the two points merge into the
center point.  In this case, geodesics project to approximately straight
lines (these are exactly straight if the geodesic includes the center
point).  For details, see Section 8 of :cite:`Karney2013`.

<<<<<<< HEAD
+---------------------+----------------------------------------------------------+
| **Classification**  | Azimuthal                                                |
+---------------------+----------------------------------------------------------+
| **Available forms** | Forward and inverse, spherical and ellipsoidal           |
+---------------------+----------------------------------------------------------+
| **Defined area**    | Within a quarter circumference of the center point       |
=======
=======
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
+---------------------+----------------------------------------------------------+
| **Classification**  | Azimuthal                                                |
+---------------------+----------------------------------------------------------+
| **Available forms** | Forward and inverse, spherical and ellipsoidal           |
+---------------------+----------------------------------------------------------+
<<<<<<< HEAD
| **Defined area**    | Global                                                   |
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
| **Defined area**    | Within a quarter circumference of the center point       |
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
+---------------------+----------------------------------------------------------+
| **Classification**  | Pseudocylindrical                                        |
+---------------------+----------------------------------------------------------+
| **Available forms** | Forward and inverse, spherical projection                |
+---------------------+----------------------------------------------------------+
| **Defined area**    | Global                                                   |
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
+---------------------+----------------------------------------------------------+
| **Alias**           | gnom                                                     |
+---------------------+----------------------------------------------------------+
| **Domain**          | 2D                                                       |
+---------------------+----------------------------------------------------------+
| **Input type**      | Geodetic coordinates                                     |
+---------------------+----------------------------------------------------------+
| **Output type**     | Projected coordinates                                    |
+---------------------+----------------------------------------------------------+


.. figure:: ./images/gnom.png
   :width: 500 px
   :align: center
   :alt:   Gnomonic

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
   proj-string: ``+proj=gnom +lat_0=90 +lon_0=-50 +R=6.4e6``
=======
   proj-string: ``+proj=gnom +lat_0=90 +lon_0=-50``
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
   proj-string: ``+proj=gnom +lat_0=90 +lon_0=-50 +R=6.4e6``
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
   proj-string: ``+proj=gnom +lat_0=90 +lon_0=-50``
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)

Parameters
################################################################################

.. note:: All parameters are optional for the Gnomonic projection.

.. include:: ../options/lon_0.rst

.. include:: ../options/lat_0.rst

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
.. include:: ../options/x_0.rst

.. include:: ../options/y_0.rst

.. include:: ../options/ellps.rst

.. include:: ../options/R.rst
=======
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
.. include:: ../options/R.rst

.. include:: ../options/x_0.rst

.. include:: ../options/y_0.rst
<<<<<<< HEAD
>>>>>>> c59e00e4fb (Merge pull request #3524 from cffk/merid-update-fix)
=======
.. include:: ../options/x_0.rst

.. include:: ../options/y_0.rst

.. include:: ../options/ellps.rst

.. include:: ../options/R.rst
>>>>>>> 13395ba739 (Fix build with -DPROJ_INTERNAL_CPP_NAMESPACE)
=======
>>>>>>> 360db021b6 (Merge pull request #3524 from cffk/merid-update-fix)
