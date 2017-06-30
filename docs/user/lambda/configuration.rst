Lambda Data Store Configuration
===============================

This section details Lambda specific configuration properties. For Accumulo specific properties, see
:ref:`accumulo_config_props`. For general properties, see :ref:`geomesa_site_xml`.

Data Store Parameters
---------------------

See :ref:`lambda_parameters` for details on data store parameters

Miscellaneous Properties
------------------------

geomesa.kafka.replication
+++++++++++++++++++++++++

This property controls the replication factor for newly created Kafka topics. By default it is 1.

Note that this property will only be used if ``createSchema`` is called on the Lambda data store. If Kafka
auto-topic creation is used instead, then the default Kafka configuration will take precedence.

geomesa.lambda.persist.interval
+++++++++++++++++++++++++++++++

This property controls how often features will be checked for expiration, and persisted to long-term storage. It
should be specified as a duration, for example ``1 minute``. By default it is ``60 seconds``.

geomesa.lambda.persist.lock.timeout
+++++++++++++++++++++++++++++++++++

This property controls how long a data store instance will wait for a lock to persist features to long-term
storage. In general, there is no need to wait for a lock, as it indicates that another instance is performing
the writes. It should be specified as a duration, for example ``100 millis``. By default it is ``1 second``.

geomesa.lambda.load.interval
++++++++++++++++++++++++++++

This property controls how often Kafka will be polled for new features. It should be specified as a duration,
for example ``1 second``. By default it is ``100 millis``.

geomesa.zookeeper.security.enabled
++++++++++++++++++++++++++++++++++

This property should be set to ``true`` if security is enabled on the Zookeeper instance. By default it is ``false``.
