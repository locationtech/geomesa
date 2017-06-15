Age-Off Iterators
=================

This chapter provides documentation on how to configure the alpha feature for data age-off via Accumulo iterators.
Currently the only available age off iterator is a date-based age-off iterator which allows administrators to set
retention periods on data (e.g. 3 months) in order to automatically age-off data from tables without manually deleting
features.

Installation
------------

The age-off iterators are provided as part of the GeoMesa Accumulo Distributed Runtime jar which can be found in the
:doc:`install` chapter.

Configuration
-------------

Since this feature is currently in alpha, it must be configured via the Accumulo Shell. The following example shows
how to configure a set of GeoMesa tables with a retention period of 3 months.

.. note::

    Only ``SimpleFeatureType``s with a default date field can be used with the date-based age-off iterator

There are three options that are required to configure the iterator:

* **sft** - a GeoMesa ``SimpleFeatureType`` spec string
* **index** - the GeoMesa Index identifier (usually concatenated index name, colon, and version e.g. z2:2 or records:2)
* **retention** - an ISO 8601 Durations (or Period) format string

The sft option is a GeoMesa ``SimpleFeatureType`` spec string. You need to scan the catalog table to determine
the correct spec string used by your feature type. We will be using it to set the ``sft`` opt on the age-off iterator.
It should resemble something like this::

    dtg:Date,*geom:Point:srid=4326;geomesa.index.dtg='dtg',geomesa.table.sharing='true',geomesa.indices='z3:4:3,z2:3:3,records:2:3',geomesa.table.sharing.prefix='\\\\u0001'

.. note::

    The iterator's priority should be lower than the standard GeoMesa iterators and the versioning iterator. A good starting
    place for the iterator priority is 5.

For example, the iterator can then be configured on scan, minc, and majc scopes on the table
"geomesa.mycatalog_mytype_z2_v2" in the shell:

.. highlight:: none

::

    # scan time iterator config
    config -t geomesa.mycatalog_mytype_z2_v2 -s table.iterator.scan.ageoff=5,org.locationtech.geomesa.accumulo.iterators.DtgAgeOffIterator
    config -t geomesa.mycatalog_mytype_z2_v2 -s table.iterator.scan.ageoff.opt.index=z2:2
    config -t geomesa.mycatalog_mytype_z2_v2 -s table.iterator.scan.ageoff.opt.retention=P3M
    config -t geomesa.mycatalog_mytype_z2_v2 -s "table.iterator.scan.ageoff.opt.sft=dtg:Date,*geom:Point:srid=4326;geomesa.index.dtg='dtg',geomesa.table.sharing='true',geomesa.indices='z3:4:3,z2:3:3,records:2:3',geomesa.table.sharing.prefix='\\\\u0001'"

    # minor compaction iterator config
    config -t geomesa.mycatalog_mytype_z2_v2 -s table.iterator.minc.ageoff=5,org.locationtech.geomesa.accumulo.iterators.DtgAgeOffIterator
    config -t geomesa.mycatalog_mytype_z2_v2 -s table.iterator.scan.ageoff.opt.index=z2:2
    config -t geomesa.mycatalog_mytype_z2_v2 -s table.iterator.minc.ageoff.opt.retention=P3M
    config -t geomesa.mycatalog_mytype_z2_v2 -s "table.iterator.scan.ageoff.opt.sft=dtg:Date,*geom:Point:srid=4326;geomesa.index.dtg='dtg',geomesa.table.sharing='true',geomesa.indices='z3:4:3,z2:3:3,records:2:3',geomesa.table.sharing.prefix='\\\\u0001'"

    # major compaction iterator config
    config -t geomesa.mycatalog_mytype_z2_v2 -s table.iterator.majc.ageoff=5,org.locationtech.geomesa.accumulo.iterators.DtgAgeOffIterator
    config -t geomesa.mycatalog_mytype_z2_v2 -s table.iterator.scan.ageoff.opt.index=z2:2
    config -t geomesa.mycatalog_mytype_z2_v2 -s table.iterator.majc.ageoff.opt.retention=P3M
    config -t geomesa.mycatalog_mytype_z2_v2 -s "table.iterator.majc.ageoff.opt.sft=dtg:Date,*geom:Point:srid=4326;geomesa.index.dtg='dtg',geomesa.table.sharing='true',geomesa.indices='z3:4:3,z2:3:3,records:2:3',geomesa.table.sharing.prefix='\\\\u0001'"

.. note::

    Use quotes around the entire argument to ``-s`` to handle special characters from SFT specs

This configuration must be applied to other indices (i.e. records, z3, attr) in order to completely age-off data.

Forcing Deletion of Records
---------------------------

The GeoMesa age-off iterators will not full delete records until compactions occur. To force a true deletion on disk of
data you must manually compact a table or range. When compacting an entire table you should take care not to overwhelm
your system:

.. code-block::

    compact -t geomesa.mycatalog_mytype_z2

