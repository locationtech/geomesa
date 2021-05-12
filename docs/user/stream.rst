GeoMesa Stream Processing
=========================

.. warning::

   The GeoMesa Stream library is deprecated and will be removed in a future version.


The GeoMesa Stream library (``geomesa-stream`` in the source distribution)
provides tools to process streams of
``SimpleFeatures``. The library can be used to instantiate a
``DataStore`` either in GeoServer or in a user's application to serve as
a constant source of ``SimpleFeatures``. For example, you can
instantiate a ``DataStore`` that will connect to Twitter and show the
most recent tweets in a spatial context. The timeout for the
``DataStore`` is configurable. A stream can be defined against any
source that can be processed by Apache Camel. A
``SimpleFeatureConverter`` can be attached to the stream to translate
the underlying data into ``SimpleFeatures``.

Modules
-------

-  ``geomesa-stream-api`` - the stream source and processing APIs
-  ``geomesa-stream-generic`` - definition of the Camel generic source
-  ``geomesa-stream-datastore`` - ``DataStore`` implementation
-  ``geomesa-stream-gs-plugin`` - GeoServer hooks for stream sources

Using GeoMesa Stream
--------------------

Installing into GeoServer
~~~~~~~~~~~~~~~~~~~~~~~~~

-   Clone GeoMesa from the source distribution found on `GitHub <https://github.com/locationtech/geomesa>`_.
-   Use Maven to build the source distribution.
-   Copy ``geomesa-stream-gs-plugin_${VERSION}-install.tar.gz`` from ``geomesa-stream/geomesa-stream-gs-plugin/target`` to GeoServer's ``/webapps/geoserver/WEB-INF/lib/ directory`` and untar it.
-   In GeoServer, navigate to ``Stores`` under ``Data`` and click ``Add new Store``.
-   ``SimpleFeature Stream Source`` should be visible under  ``Vector Data Sources``.

Example Usage
~~~~~~~~~~~~~

To illustrate usage, assume we are processing a stream of Twitter data
as a csv. The configuration in GeoServer, when creating a new ``SimpleFeature Stream Source``, is as follows:

.. code-block:: javascript

    {
      type         = "generic"
      source-route = "netty4:tcp://localhost:5899?textline=true"
      sft          = {
                       type-name = "twitter"
                       fields = [
                         { name = "user",      type = "String" }
                         { name = "msg",       type = "String" }
                         { name = "geom",      type = "Point",  index = true, srid = 4326, default = true }
                         { name = "dtg",       type = "Date",   index = true }
                       ]
                     }
      converter    = {
                       id-field = "md5(stringToBytes($0))"
                       type = "delimited-text"
                       format = "DEFAULT"
                       fields = [
                         { name = "user",      transform = "$0" }
                         { name = "msg",       transform = "$1" }
                         { name = "geom",      transform = "point($2::double, $3::double)" }
                         { name = "dtg",       transform = "datetime($4)" }
                       ]
                     }
    }

This defines a stream source that will listen on port 5899 for csv
messages that have the following columns: ``user``, ``msg``, ``lon``,
``lat``, ``dtg``.

Twitter csv messages sent to the defined port over tcp will be processed by GeoMesa and sent to GeoServer. A layer can be published from the newly created store to view the data.

.. important::
    The Apache Camel route used by GeoMesa Stream defines a consumer endpoint not a producer endpoint.

Further Usage
~~~~~~~~~~~~~

To instantiate a ``DataStore`` for this type that
keeps the last 30 seconds of tweets, use the following code.

.. code-block:: scala

    val ds = DataStoreFinder.getDataStore(
      Map(
        StreamDataStoreParams.STREAM_DATASTORE_CONFIG.key -> sourceConf,
        StreamDataStoreParams.CACHE_TIMEOUT.key           -> Integer.valueOf(30)
      ))

To query this stream source, use a ``FilterFactory`` from
``org.geotools.factory.CommonFactoryFinder``. To receive notifications
on new ``SimpleFeatures``, use a ``StreamListener``:

.. code-block:: scala

    val listener = 
      new StreamListener {
        def onNext(sf: SimpleFeature) = println(s"Received a new feature: ${sf.getID}")
      }
    ds.asInstanceOf[org.locationtech.geomesa.stream.datastore.StreamDataStore].registerListener(listener)

UDP
---

The generic source can be used with UDP as well, although there are some
caveats:

-  If you are sending text, the source route must include
   '?textline=true', even though the Camel docs say that only applies to
   TCP
-  Each UDP packet data must end with a newline character
-  Each UDP packet data must contain exactly one line - everything after
   the newline will be dropped
-  Maximum text line size can be controlled by the route parameter
   'decoderMaxLineLength' with a maximum value of 2048
-  If the message is longer than the line size then the message will be
   dropped
-  Default maximum text line length is 1024
-  Note that technically the line length can be longer, but Camel does
   not expose the Netty UDP RCVBUF\_ALLOCATOR option, which causes
   messages to be truncated at 2048 bytes.
