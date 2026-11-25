Cassandra Index Configuration
=============================

GeoMesa exposes a variety of configuration options that can be used to customize and optimize a given installation.
See :ref:`index_config` for the general options. This section describes options that are specific to Cassandra.

.. _cassandra_feature_expiry:

Feature Expiration
------------------

Cassandra supports setting a per-feature time-to-live. Expiration can be set in the ``SimpleFeatureType`` user data,
using the key ``geomesa.feature.expiry``. See :ref:`set_sft_options` for details on configuring the user data.
Expiration can be set before calling ``createSchema``, or can be added, changed or removed (by setting it to an
empty value) for an existing schema by calling ``updateSchema``. However, note that a change made through
``updateSchema`` only applies to features written afterwards; existing features keep the time-to-live they were
written with.

Expiration can be based on either ingest time or a feature attribute. To set expiration based on ingest time,
specify a time-to-live as a duration string, e.g. ``24 hours`` or ``180 days``. To set expiration based on
a feature attribute, specify the attribute along with a time-to-live in parentheses, e.g. ``dtg(24 hours)`` or
``event-time(30 days)`` (where ``dtg`` and ``event-time`` are ``Date``-type attributes in the schema).

Feature expiration is based on Cassandra's time-to-live functionality. Features expire from all indices at the same
time, and expired features are not returned by queries. Updating a feature resets its time-to-live, based on the
updated feature.

* Ingest-time expiration uses the Cassandra ``default_time_to_live`` table option, which is set on each index table
  for the schema. Cassandra supports a time-to-live of up to 20 years.
* Attribute-based expiration sets the time-to-live of each feature when it is written. Features that have already
  expired, or that don't have a value for the attribute, are not written. For features that would expire more
  than 20 years in the future, the time-to-live is capped at 20 years.

.. warning::

    Cassandra versions before 5.0, and Cassandra 5.0 running in a storage compatibility mode (the default
    ``storage_compatibility_mode`` for 5.0 is ``CASSANDRA_4``), can't store expiration dates after 2038-01-19. See
    `CASSANDRA-14227 <https://issues.apache.org/jira/browse/CASSANDRA-14227>`__. Writes that would expire after
    that date are handled according to the Cassandra expiration date overflow policy, set through the system
    property ``cassandra.expiration_date_overflow_policy``. The default policy ``REJECT`` fails the write, while
    ``CAP`` stores the feature with the latest supported expiration date. Cassandra 5.0 with
    ``storage_compatibility_mode: NONE`` supports later expiration dates.

Expired data is removed from disk through compaction, after the table's ``gc_grace_seconds`` have passed. For data
that expires, the ``TimeWindowCompactionStrategy`` is generally more efficient than the default compaction strategy.
See the `Cassandra documentation <https://cassandra.apache.org/doc/latest/cassandra/managing/operating/compaction/twcs.html>`__
for details.
