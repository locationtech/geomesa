Cassandra Index Configuration
=============================

GeoMesa exposes a variety of configuration options that can be used to customize and optimize a given installation.
See :ref:`index_config` for the general options. This section describes options that are specific to Cassandra.

.. _cassandra_feature_expiry:

Feature Expiration
------------------

Cassandra supports setting a time-to-live for features based on ingest time. Expiration can be set in the
``SimpleFeatureType`` user data, using the key ``geomesa.feature.expiry``. See :ref:`set_sft_options` for details on
configuring the user data. Specify the time-to-live as a duration string, e.g. ``24 hours`` or ``180 days``. Cassandra
supports a time-to-live of up to 20 years. Expiration based on a feature attribute (e.g. ``dtg(24 hours)``) is not
currently supported.

Expiration is implemented with the Cassandra ``default_time_to_live`` table option, which is set on each index table
for the schema. Features expire from all indices at the same time, and expired features are not returned by queries.
Updating a feature resets its time-to-live.

Expiration can be set before calling ``createSchema``, or can be added, changed or removed (by setting it to an
empty value) for an existing schema by calling ``updateSchema``. However, note that a change made through
``updateSchema`` only applies to features written afterwards; existing features keep the time-to-live they were
written with.

Expired data is removed from disk through compaction, after the table's ``gc_grace_seconds`` have passed. For data
that expires, the ``TimeWindowCompactionStrategy`` is generally more efficient than the default compaction strategy.
See the `Cassandra documentation <https://cassandra.apache.org/doc/latest/cassandra/managing/operating/compaction/twcs.html>`__
for details.
