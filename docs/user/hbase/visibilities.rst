.. _hbase_visibilities:

HBase Visibilities
------------------

GeoMesa supports using the HBase visibility coprocessor for security SimpleFeatures with cell-level security.
Visibilities in HBase are currently available at the feature level.

See :ref:`data_security` for details on writing and reading data with visibilities.

Setup and Configuration
^^^^^^^^^^^^^^^^^^^^^^^

To configure HBase for visibility filtering follow the setup in the HBase Book under the
`Visibility Labels <https://hbase.apache.org/book.html#hbase.visibility.labels>`__ section of
the HBase book which includes enabling the visibility coprocessors in your hbase-site.xml:

.. code-block:: xml

    <property>
      <name>hbase.security.authorization</name>
      <value>true</value>
    </property>
    <property>
      <name>hbase.coprocessor.region.classes</name>
      <value>org.apache.hadoop.hbase.security.visibility.VisibilityController</value>
    </property>
    <property>
      <name>hbase.coprocessor.master.classes</name>
      <value>org.apache.hadoop.hbase.security.visibility.VisibilityController</value>
    </property>

When connecting to your datastore you'll need to enable visibilities with the following Parameter:

.. code-block:: java

    Map<String, String> parameters = ...
    parameters.put("hbase.security.enabled", "true");
    DataStore ds = DataStoreFinder.getDataStore(parameters);

Known Issues
^^^^^^^^^^^^

HBase currently does not provide a method of retrieving Cell Visibility Labels from existing data stored within HBase.
Therefore, deleting data as a non-superuser with per-feature visibility levels cannot be guaranteed.
