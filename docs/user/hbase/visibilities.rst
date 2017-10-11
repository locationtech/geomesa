.. _hbase_visibilities:

HBase Visibilities
------------------

GeoMesa supports using the HBase visibility coprocessor for security SimpleFeatures with cell-level security.
Visibilities in HBase are currently available at the data store and feature levels.

See :ref:`authorizations` for details on querying data with visibilities.

Setup and Configuration
^^^^^^^^^^^^^^^^^^^^^^^

To configure HBase for visibility filtering follow the setup in the HBase Book under the
`Visibility Labels <http://hbase.apache.org/book.html#hbase.visibility.labels>`__ section of
the HBase book which includes enabling the HFile v3 format and visibility coprocessors in your hbase-site.xml:

.. code-block:: xml

    <property>
      <name>hfile.format.version</name>
      <value>3</value>
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

Data Store Level Visibilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When creating your data store, a default visibility can be configured for all features:

.. code-block:: java

    Map<String, String> parameters = ...
    parameters.put("hbase.security.enabled", "true");
    parameters.put("geomesa.security.visibilities", "admin&user");
    DataStore ds = DataStoreFinder.getDataStore(parameters);

If present, visibilities set at the feature or attribute level will take priority over the data store configuration.


Feature Level Visibilities
^^^^^^^^^^^^^^^^^^^^^^^^^^

Visibilities can be set on individual features using the simple feature user data:

.. code-block:: java

    import org.locationtech.geomesa.security.SecurityUtils;

    SecurityUtils.setFeatureVisibility(feature, "admin&user")

or

.. code-block:: java

    feature.getUserData().put("geomesa.feature.visibility", "admin&user");

Known Issues
^^^^^^^^^^^^

HBase currently does not provide a method of retrieving Cell Visibility Labels from existing data stored within HBase.
Therefore, deleting data as a non-superuser with per-feature visibility levels cannot be guaranteed as it can be in
the AccumuloDataStore.
