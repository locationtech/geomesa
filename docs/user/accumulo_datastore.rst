Accumulo Data Store
===================

The GeoMesa Accumulo Data Store module (``geomesa-accumulo`` in the source distribution)
contains all of the Accumulo-related code for GeoMesa. This includes client code and
distributed iterator code for the Accumulo tablet servers.

Installation
------------

Use of the Accumulo data store requires that the distributed runtime JAR be installed on the tablet servers, described in more detail in :ref:`install_accumulo_runtime`.

Bootstrapping GeoMesa Accumulo on Elastic Map Reduce
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A script to bootstrap GeoMesa Accumulo on an Elastic Map Reduce cluster is provided in ``geomesa-accumulo/geomesa-accumulo-tools/emr4`` and on this public S3 bucket: `s3://elasticmapreduce-geomesa/ <http://s3.amazonaws.com/elasticmapreduce-geomesa/>`_. These rely on the EMR managed Hadoop and ZooKeeper applications. See ``geomesa-accumulo/geomesa-accumulo-tools/emr4/README.md`` for more details on using these clusters. The command below launches a GeoMesa EMR cluster:

.. code-block:: bash

    NUM_WORKERS=2
    CLUSTER_NAME=geomesa-emr
    AWS_REGION=us-east-1
    AWS_PROFILE=my_profile
    KEYPAIR_NAME=my_keypair # a keypair in the region (for which you have the private key)

    aws emr create-cluster --applications Name=Hadoop Name=ZooKeeper-Sandbox \
        --bootstrap-actions Path=s3://elasticmapreduce-geomesa/bootstrap-geomesa.sh,Name=geomesa-accumulo \
        --ec2-attributes KeyName=$KEYPAIR_NAME,InstanceProfile=EMR_EC2_DefaultRole \
        --service-role EMR_DefaultRole \
        --release-label emr-4.7.1 --name $CLUSTER_NAME \
        --instance-groups InstanceCount=$NUM_WORKERS,InstanceGroupType=CORE,InstanceType=m3.xlarge InstanceCount=1,InstanceGroupType=MASTER,InstanceType=m3.xlarge \
        --region $AWS_REGION --profile $AWS_PROFILE

Configuration
-------------

GeoMesa uses a site xml file to maintain system property configurations. This file can be found
at ``conf/geomesa-site.xml`` of the Accumulo distribution. The default settings for GeoMesa are
stored in ``conf/geomesa-site.xml.template``. Do not modify this file directly as it is never read,
instead copy the desired configurations into geomesa-site.xml.

By default, system properties set through command line parameters will take precedence over this
configuration file. If you wish a configuration item to always take precedence, even over command
line parameters, change the ``<final>`` tag to true.

By default configuration properties with empty values will not be applied, you can change this
by marking a property as final.

Zookeeper session timeout
~~~~~~~~~~~~~~~~~~~~~~~~~

The `Zookeeper session timeout <http://accumulo.apache.org/1.6/accumulo_user_manual#_instance_zookeeper_timeout>`__
for the GeoMesa Accumulo data store is exposed as the Java system property ``instance.zookeeper.timeout``:

.. code-block:: bash

    export JAVA_OPTS="-Dinstance.zookeeper.timeout=10s"

To preserve configuration set this property in ``geomesa-site.xml``.

Creating a Data Store
---------------------

An instance of an Accumulo data store can be obtained through the normal GeoTools discovery methods, assuming that the GeoMesa code is on the classpath:

.. code-block:: java

    Map<String, String> parameters = new HashMap<>;
    parameters.put("instanceId", "myInstance");
    parameters.put("zookeepers", "zoo1,zoo2,zoo3");
    parameters.put("user", "myUser");
    parameters.put("password", "myPassword");
    parameters.put("tableName", "my_table");
    org.geotools.data.DataStore dataStore = org.geotools.data.DataStoreFinder.getDataStore(parameters);

More information on using GeoTools can be found in the `GeoTools user guide <http://docs.geotools.org/stable/userguide/>`_.

Using the Accumulo Data Store in GeoServer
------------------------------------------

See :doc:`./geoserver`.

Visibilities
------------

The GeoMesa Accumulo data store supports Accumulo visibilities for securing data
at data store, feature, or individual attribute levels.

See :ref:`accumulo_visibilities` for more information.

Scan Authorizations
-------------------

The GeoMesa Accumulo data store provides a Java SPI for custom handling of user authorizations per query.

See :ref:`accumulo_authorizations` for more information.

Indexing Strategies
-------------------

GeoMesa uses several different strategies to index simple features. In the code, these strategies are
abstracted as 'tables'. For details on how GeoMesa encodes and indexes data, see :ref:`index_structure`.
The :doc:`./data_management` chapter also describes how to optimize these indexes by manipulating
:ref:`attribute_indices`, :ref:`customizing_z_index`, :ref:`customizing_xz_index`, and :ref:`customizing_index_creation`.

For details on how GeoMesa chooses and executes queries, see the `org.locationtech.geomesa.accumulo.index.QueryPlanner <https://github.com/locationtech/geomesa/blob/master/geomesa-accumulo/geomesa-accumulo-datastore/src/main/scala/org/locationtech/geomesa/accumulo/index/QueryPlanner.scala>`__ and `org.locationtech.geomesa.accumulo.index.QueryStrategyDecider <https://github.com/locationtech/geomesa/blob/master/geomesa-accumulo/geomesa-accumulo-datastore/src/main/scala/org/locationtech/geomesa/accumulo/index/QueryStrategyDecider.scala>`__ classes.

.. _explain_query:

Explaining: Query Plans
-----------------------

Given a data store and a query, you can ask GeoMesa to explain its plan for how to execute the query:

.. code-block:: java

    dataStore.getQueryPlan(query, explainer = new ExplainPrintln);

Instead of ``ExplainPrintln``, you can also use ``ExplainString`` or ``ExplainLogging`` to redirect the explainer output elsewhere.

For enabling ``ExplainLogging`` in GeoServer, see :ref:`geoserver_explain_query`. It may also be helpful to refer to GeoServer's `Advanced log configuration <http://docs.geoserver.org/stable/en/user/configuration/logging.html>`_ documentation for the specifics of how and where to manage the GeoServer logs.

Knowing the plan -- including information such as the indexing strategy -- can be useful when you need to debug slow queries.  It can suggest when indexes should be added as well as when query-hints may expedite execution times.

Iterator Stack
--------------

GeoMesa uses Accumulo iterators to push processing out to the whole cluster. The iterator stack can be considered a 'tight inner loop' - generally, every feature returned will be processed in the iterators. As such, the iterators have been written for performance over readability.

We use several techniques to improve iterator performance. For one, we only deserialize the attributes of a simple feature that we need to evaluate a given query. When retrieving attributes, we always look them up by index, instead of by name. For aggregating queries, we create partial aggregates in the iterators, instead of doing all the processing in the client. The main goals are to minimize disk reads, processing and bandwidth as much as possible.

For more details, see the `org.locationtech.geomesa.accumulo.iterators <https://github.com/locationtech/geomesa/tree/master/geomesa-accumulo/geomesa-accumulo-datastore/src/main/scala/org/locationtech/geomesa/accumulo/iterators>`__ package.