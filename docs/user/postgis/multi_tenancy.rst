.. _pg_multi_tenancy:

Multi Tenancy
=============

The partitioned PostGIS data store will create various tables, views, sequences and procedures using the database user
specified in the `data store parameters <https://docs.geotools.org/stable/userguide/library/jdbc/datastore.html>`__. In
multi-tenancy situations, the easiest way to ensure security is to use different database schemas for each tenant. If using
a single database schema, there are are several tables and functions that need to be shared. The user associated with the first
feature type that is created in a given database schema will own the shared objects, and any additional users will need to be
granted permissions to use them.

Functions
---------

If sharing a database schema, the following functions need ``EXECUTE`` granted on them to any roles used to create feature
types in the database:

* ``cron_log_cleaner``
* ``truncate_to_partition(timestamp without time zone, int)``
* ``truncate_to_ten_minutes(timestamp without time zone)``

Tables
------

If sharing a database schema, the following tables will need ``SELECT``, ``INSERT``, ``UPDATE`` and ``DELETE`` granted on them
to any roles used to create feature types:

* ``geomesa_wa_seq``
* ``geomesa_userdata``
* ``gt_pk_metadata``

With basic grants, malicious roles will be able to alter the metadata for feature types that are not owned by them, potentially
causing a denial-of-service attack. To prevent this, segregate roles into separate database schemas.

Read Access
-----------

To grant read-only access, the following tables and views need ``SELECT`` granted on them. The exact names will depend
on the feature type name, denoted here with ``<feature_type_name>``:

* View ``<feature_type_name>``
* Table ``<feature_type_name>_wa``
* Table ``<feature_type_name>_wa_partition``
* Table ``<feature_type_name>_partition``
* Table ``<feature_type_name>_spill``
* Table ``geomesa_userdata``
* Table ``gt_pk_metadata``

For convenience, read-only roles can be specified during feature type creation using the data store parameter
``read_access_roles``.
