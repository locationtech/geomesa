Using the Accumulo Data Store Programmatically
==============================================

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

.. _accumulo_parameters:

Parameters
----------

The Accumulo Data Store takes
several parameters:

==================== =======================================================================================
Parameter            Description
==================== =======================================================================================
instanceId *         The instance ID of the Accumulo installation
zookeepers *         A comma separated list of zookeeper servers (e.g. "zoo1,zoo2,zoo3" or "localhost:2181")
user *               Accumulo username
password             Accumulo password
keytabPath           Path to a Kerberos keytab file containing an entry for the specified user
tableName *          The name of the GeoMesa catalog table
auths                Comma-delimited superset of authorizations that will be used for queries via Accumulo.
visibilities         Accumulo visibilities to apply to all written data
queryTimeout         The max time (in sec) a query will be allowed to run before being killed
queryThreads         The number of threads to use per query
recordThreads        The number of threads to use for record retrieval
writeThreads         The number of threads to use for writing records
collectStats         Toggle collection of statistics
caching              Toggle caching of results
==================== =======================================================================================

The required parameters are marked with an asterisk. One (but not both) of ``password`` and ``keytabPath`` must be
provided.