Compatibility Across Versions
+++++++++++++++++++++++++++++

GeoMesa adheres to `semantic versioning <https://semver.org/>`__. This allows users to gauge the potential impact of
any given update. Semantic versioning makes API guarantees, but GeoMesa has several compatibility vectors to consider:

.. |y| unicode:: U+2705
.. |n| unicode:: U+274C

.. csv-table::
   :header-rows: 1
   :stub-columns: 1
   :widths: 12,20,20,20,20
   :class: centered-cells

   "", "Data", "API", "Binary", "Dependencies"
   "Patch", |y|, |y|, |y|, |y|
   "Major", |y|, |n|, |n|, |n|
   "Minor", |y|, |y|, |n|, |n|

Data Compatibility
^^^^^^^^^^^^^^^^^^

Data compatibility refers to the ability to read and write data written with older versions of GeoMesa. GeoMesa
fully supports data written with version 1.2.2 or later, and mostly supports data written with 1.1.0 or later.

Note that although later versions can read earlier data, the reverse is not necessarily true. Data written
with a newer client may not be readable by an older client.

API Compatibility
^^^^^^^^^^^^^^^^^

The GeoMesa public API is not currently well defined, so API compatibility is only guaranteed at the GeoTools
`DataStore <https://docs.geotools.org/stable/javadocs/org/geotools/api/data/DataStore.html>`__ level. In the future,
GeoMesa will clearly indicate which classes and methods are part of the public API. Non-public classes may change
without warning between minor versions.

Binary Compatibility
^^^^^^^^^^^^^^^^^^^^

Binary compatibility refers to the ability to have different GeoMesa versions in a single environment. An environment
may be a single process or span multiple servers (for example an ingest pipeline, a query client, and an analytics
platform). For data stores with a distributed component (HBase and Accumulo), the environment includes both the
client and the distributed code.

GeoMesa requires that all JARs in an environment are the same minor version, and that all JARs within a single JVM
are the same patch version.

Dependency Compatibility
^^^^^^^^^^^^^^^^^^^^^^^^

Dependency compatibility refers to the ability to update GeoMesa without updating other components
(e.g. Accumulo, HBase, Hadoop, Spark, GeoServer, etc). Generally, GeoMesa supports a range of dependency versions
(e.g. Accumulo 2.0 to 2.1). Spark versions are more tightly coupled, due to the use of private Spark APIs.
