.. _authorizations:

Authorizations
--------------

When performing a query using the Accumulo or HBase datastore, GeoMesa delegates the retrieval of authorizations to
``service providers`` that implement the following interface:

.. code-block:: java

    package org.locationtech.geomesa.security;

    public interface AuthorizationsProvider {

        public static final String AUTH_PROVIDER_SYS_PROPERTY = "geomesa.auth.provider.impl";

        /**
         * Gets the authorizations for the current context. This may change over time
         * (e.g. in a multi-user environment), so the result should not be cached.
         *
         * @return
         */
        public List<String> getAuthorizations();

        /**
         * Configures this instance with parameters passed into the DataStoreFinder
         *
         * @param params
         */
        public void configure(Map<String, Serializable> params);
    }

When a GeoMesa data store is instantiated, it will scan for available service providers
via Java SPI. Third-party implementations can be enabled by placing them on the classpath
and including a special service descriptor file. See the
`Oracle Javadoc <http://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html>`__
for details on implementing a service provider.

The GeoMesa data store will call ``configure()`` on the ``AuthorizationsProvider``
implementation, passing in the parameter map from the call to ``DataStoreFinder.getDataStore(Map params)``.
This allows the ``AuthorizationsProvider`` to configure itself based on the environment.

To ensure that the correct ``AuthorizationsProvider`` is used, GeoMesa will throw an exception if multiple
third-party service providers are found on the classpath. In this scenario, the particular service
provider class to use can be specified by the following system property:

.. code-block:: java

    // equivalent to "geomesa.auth.provider.impl"
    org.locationtech.geomesa.security.AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY

For simple scenarios, the set of authorizations to apply to all queries can be specified when creating
the GeoMesa data store by using the ``geomesa.security.auths`` configuration parameter. This will use a
default ``AuthorizationsProvider`` implementation provided by GeoMesa.

.. code-block:: java

    // create a map containing initialization data for the GeoMesa data store
    Map<String, String> configuration = ...
    configuration.put("geomesa.security.auths", "user,admin");
    DataStore dataStore = DataStoreFinder.getDataStore(configuration);

If there are no ``AuthorizationsProvider`` implementations found on the classpath, and the ``geomesa.security.auths``
parameter is not set, GeoMesa will default to using the authorizations associated with the underlying Accumulo or HBase
connection (i.e. the ``accumulo.user`` configuration value).

.. warning::

    This is not a recommended approach for a production system.

In addition, please note that the authorizations used in any scenario cannot exceed
the authorizations of the underlying Accumulo or HBase connection.

For examples on implementing an ``AuthorizationsProvider`` see the :ref:`accumulo_tutorials_security` tutorials.
