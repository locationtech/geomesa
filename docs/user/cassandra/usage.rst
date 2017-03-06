Using the Cassandra DataStore Programmatically
==============================================

Since the Cassandra DataStore is just another Geotools DataStore, you can use it exactly as
you would any other Geotools DataStore such as the PostGIS DataStore or the Accumulo DataStore.
To get a connection to a Cassandra DataStore, use the ``DataStoreFinder``:

.. code-block:: java

    import com.google.common.collect.ImmutableMap;
    import org.geotools.data.DataStore;
    import org.geotools.data.DataStoreFinder;
    import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;

    import java.io.IOException;
    import java.util.Arrays;
    import java.util.Map;

    Map<String, ?> params = ImmutableMap.of(
       "geomesa.cassandra.catalog.table", "127.0.0.1:9142",
       "geomesa.cassandra.keyspace", "geomesa_cassandra",
       "geomesa.cassandra.contact.point", "mycatalog");
    DataStore ds = DataStoreFinder.getDataStore(params);
    ds.createSchema(SimpleFeatureTypes.createType("test", "testjavaaccess", "foo:Int,dtg:Date,*geom:Point:srid=4326"));
