.. _reserved-words:

Reserved Words
--------------

There are a number of GeoTools reserved words that cannot be used as attribute names in GeoMesa.
Note that this list is case insensitive.

* ``after``
* ``and``
* ``before``
* ``beyond``
* ``contains``
* ``crosses``
* ``disjoint``
* ``does-not-exist``
* ``during``
* ``dwithin``
* ``equals``
* ``exclude``
* ``exists``
* ``false``
* ``geometrycollection``
* ``id``
* ``include``
* ``intersects``
* ``is``
* ``like``
* ``linestring``
* ``multilinestring``
* ``multipoint``
* ``multipolygon``
* ``not``
* ``null``
* ``or``
* ``overlaps``
* ``point``
* ``polygon``
* ``relate``
* ``touches``
* ``true``
* ``within``

Override
^^^^^^^^

If you really, really want to use one of these words as an attribute name, you may override the check. Note that some
functionality may not work as expected.

Override the check by setting the ``SimpleFeatureType`` user data key ``override.reserved.words=true``. Alternatively,
you may set it as a system property.
