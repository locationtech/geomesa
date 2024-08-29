Lambda Index Configuration
==========================

GeoMesa exposes a variety of configuration options that can be used to customize and optimize a given installation.
The Lambda data store supports most of the general options described under :ref:`index_config`.

Kafka Topic Name
----------------

Each SimpleFeatureType (or schema) will be written to a unique Kafka topic. By default, the topic will be
named based on the persistent data store and the SimpleFeatureType name.

If desired, the topic name can be set to an arbitrary value by setting the user data key ``geomesa.lambda.topic``
before calling ``createSchema``:

.. code-block:: java

    SimpleFeatureType sft = ....;
    sft.getUserData().put("geomesa.lambda.topic", "myTopicName");

For more information on how to set schema options, see :ref:`set_sft_options`.
