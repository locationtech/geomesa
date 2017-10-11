.. _accumulo_visibilities:

Accumulo Visibilities
---------------------

GeoMesa support Accumulo visibilities for securing data. Visibilities can be set at the data store,
feature, or individual attribute levels.

See :ref:`authorizations` for details on querying data with visibilities.

Data Store Level Visibilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When creating your data store, a default visibility can be configured for all features:

.. code-block:: java

    Map<String, String> parameters = ...
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


Attribute-Level Visibilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For more advanced use cases, visibilities can be set at the attribute level.
Attribute-level visibilities must be enabled when creating your simple feature type by setting
the appropriate user data value:

.. code-block:: java

    sft.getUserData().put("geomesa.visibility.level", "attribute");
    dataStore.createSchema(sft);

When writing each feature, the per-attribute visibilities must be set in a comma-delimited string in the user data.
Each attribute must have a corresponding value in the delimited string, otherwise an error will be thrown.

For example, if your feature type has four attributes:

.. code-block:: java

    import org.locationtech.geomesa.security.SecurityUtils;

    SecurityUtils.setFeatureVisibility(feature, "admin,user,admin,user")

or

.. code-block:: java

    feature.getUserData().put("geomesa.feature.visibility", "admin,user,admin,user");