GeoMesa Native API
==================

.. warning::

  The GeoMesa Native API is deprecated and will be removed in a future version.

The GeoMesa Native API (``geomesa-native-api`` in the source distribution) is for developers who prefer not to use GeoTools interfaces and just want to geo index their
data.  It exposes a simple interface for persisting and querying GeoMesa as well as a simple integration point with
Geoserver in cases where OGC access is still desired.

Usage
-----

To interact with the GeoMesa Native API, instantiate an instance of a ``GeoMesaIndex<T>`` with your payload type ``T``.

.. code-block:: java

    GeoMesaIndex<DomainObject> index =
                    AccumuloGeoMesaIndex.build(
                        "hello",
                        "zoo1:2181",
                        "mycloud",
                        "myuser", "mypass",
                        true,
                        new DomainObjectValueSerializer(),
                        new DefaultSimpleFeatureView<DomainObject>("foo"));

In the code snippet above, we are instantiating an ``AccumuloGeoMesaIndex`` with a type parameter of ``DomainObject``, the
payload object type that is specific to each usage of the native API.  We have provided a ``DomainObjectValueSerializer``
an implementation of ``ValueSerializer`` which tells the native API how to convert the domain object into a byte array
payload.  We have also provided a ``DefaultSimpleFeatureView`` which maintains integration with the Geotools infrastructure.
The ``DefaultSimpleFeatureView`` will create a ``SimpleFeatureType`` that contains the geometry, date, and identifier of the
``DomainObject`` and enable querying based on these attributes.  If you want to enable querying on more attributes from
your domain object, you should create a class that extends ``SimpleFeatureView`` and converts the domain object into a
rich ``SimpleFeature``.  You can then leverage GeoMesa's secondary indexing optimizations on first class attributes.

Persisting data to a ``GeoMesaIndex`` requires utilizing the various ``insert`` methods.  The following code snippet demonstrates
how you can put domain objects into the index.

.. code-block:: java

    final GeometryFactory gf = JTSFactoryFinder.getGeometryFactory();
    DomainObject one = new DomainObject();
    DomainObject two = new DomainObject();
    index.insert(
        one,
        gf.createPoint(new Coordinate(-78.0, 38.0)),
        date("2016-01-01T12:15:00.000Z"));
    index.insert(
        two,
        gf.createPoint(new Coordinate(-78.0, 40.0)),
        date("2016-02-01T12:15:00.000Z"));

The code above will compute an identifier for each of the domain objects.  If you want to control the identifiers,
use the ``insert`` methods with ``id`` in the signature.  If you do not intend to query GeoMesa using the specific identifiers of
your domain object, prefer the method that generates an identifier for you.  The reason is that the identifier chosen by
GeoMesa optimizes the locality properties of the identifier and thus the caching within the underlying database.

To query data stored using the GeoMesa Native API, build up a ``GeoMesaQuery`` and pass it to the ``GeoMesaIndex``.
 For example, the following code snippet queries GeoMesa on space and time.

.. code-block:: java

    GeoMesaQuery q =
        GeoMesaQuery.GeoMesaQueryBuilder.builder()
            .within(-79.0, 37.0, -77.0, 39.0)
            .during(date("2016-01-01T00:00:00.000Z"), date("2016-03-01T00:00:00.000Z"))
            .build();
    Iterable<DomainObject> results = index.query(q);

If you have used a custom ``SimpleFeatureView``, you can query on the attributes you've lifted into the ``SimpleFeatureType``.
For instance, if you have lifted an attribute called ``age`` into the ``SimpleFeatureType``, you can query on it as follows and
GeoMesa will push the ``age`` predicate down for processing in the database.

.. code-block:: java

    FilterFactory ff = CommonFactoryFinder.getFilterFactory2();
    GeoMesaQuery q =
        GeoMesaQuery.GeoMesaQueryBuilder.builder()
            .within(-79.0, 37.0, -77.0, 39.0)
            .during(date("2016-01-01T00:00:00.000Z"), date("2016-03-01T00:00:00.000Z"))
            .filter(ff.greaterThan(ff.property("age"), 30)
            .build();
    Iterable<DomainObject> results = index.query(q);