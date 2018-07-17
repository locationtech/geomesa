.. _kudu_visibilities:

Visibilities in Kudu
--------------------

Although Kudu does not support native visibilities, GeoMesa can apply them for securing SimpleFeatures with
cell-level security. Visibilities in Kudu are currently available at the feature level.

See :ref:`authorizations` for details on querying data with visibilities.

Feature Level Visibilities
^^^^^^^^^^^^^^^^^^^^^^^^^^

Visibilities can be set on individual features using the simple feature user data:

.. code-block:: java

    import org.locationtech.geomesa.security.SecurityUtils;

    SecurityUtils.setFeatureVisibility(feature, "admin&user")

or

.. code-block:: java

    feature.getUserData().put("geomesa.feature.visibility", "admin&user");
