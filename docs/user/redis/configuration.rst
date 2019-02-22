.. _redis_config_props:

Redis Data Store Configuration
==============================

This section details Redis-specific configuration properties. For general properties,
see :ref:`geomesa_site_xml`.

geomesa.redis.tx.backoff
++++++++++++++++++++++++

Sets the back-off multiplier for retrying writes that fail due to concurrent conflicting clients. For each retry
attempt, the client will wait based on ``geomesa.redis.tx.pause`` and this back-off multiplier. The multipliers
are specified as a comma-delimited list of numbers, where each number corresponds to a retry attempt. For example,
by default, the multipliers are ``1,1,2,2,5,10,20``, which means that the first and second retry attempts will use
the pause unmodified, the third and fourth attempts will double the pause, the fifth and sixth attempts will multiply
the pause by 5 and 10, respectively, and any subsequent attempts will multiply the pause by 20. There is also
a small random delay added, to prevent clients from pausing for the exact same amount of time. See also
``geomesa.redis.tx.pause`` and ``geomesa.redis.tx.retry``.

geomesa.redis.tx.pause
++++++++++++++++++++++

Sets the delay between retry attempts, for retrying writes that fail due to concurrent conflicting clients. For
each retry attempt, the client will wait based on ``geomesa.redis.tx.backoff`` and this pause value. The pause
value should be specified as a duration, and defaults to ``100ms``. See also ``geomesa.redis.tx.backoff`` and
``geomesa.redis.tx.retry``.

geomesa.redis.tx.retry
++++++++++++++++++++++

Sets the number of times to attempt to write to Redis for a given transaction. Due to multi-threading, some
writes to Redis may fail if clients are simultaneously updating the same schema. In such a scenario, the
writes will be retried several times, with increasing back-off times to allow for a busy system. By default,
writes are retried ``10`` times. See also ``geomesa.redis.tx.backoff`` and ``geomesa.redis.tx.pause``.

geomesa.redis.write.batch
+++++++++++++++++++++++++

Sets the default batch size for writing features to Redis. When using a GeoTools, ``FeatureWriter``, data will
only be sent to Redis once the batch size is reached (or with an explicit ``flush`` or ``close`` of the writer).
By default, the batch size is set to ``1000`` features.
