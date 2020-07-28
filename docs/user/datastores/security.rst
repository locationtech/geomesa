.. _data_security:

Data Security
=============

GeoMesa supports the concept of visibility labels on each piece of data, which can be used to govern which users
can see which records. Visibilities were originally part of Apache Accumulo, and were later adopted by Apache HBase.
GeoMesa supports visibilities across all provided data stores, using native methods where possible or implicit
filtering where not natively supported.

See the `Apache Accumulo documentation <https://accumulo.apache.org/docs/2.x/security/authorizations>`__ for
an overview of the concepts involved.

Writing Visibility Labels
-------------------------

Visibility labels are simple Boolean expressions based on a set of arbitrary tags. For example, you may define
two tags, ``admin`` and ``user``. An expression could be ``admin``, ``user``, ``admin&user``, or ``admin|user``,
depending on how you want to grant access to your data. For more complex expressions, parentheses can be used
for operation order.

Visibility labels can be written in two different ways, either at the level of each feature, or at the level
of each attribute in a feature.

Feature Level Visibilities
^^^^^^^^^^^^^^^^^^^^^^^^^^

Visibilities can be set on individual features using the simple feature user data:

.. code-block:: java

    import org.locationtech.geomesa.security.SecurityUtils;

    SecurityUtils.setFeatureVisibility(feature, "admin&user")
    // or, equivalently:
    feature.getUserData().put("geomesa.feature.visibility", "admin&user");


Attribute-Level Visibilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    Attribute-level visibilities are currently only implemented for the Accumulo data store.

For more advanced use cases, visibilities can be set at the attribute level.
Attribute-level visibilities must be enabled when creating your simple feature type by setting
the appropriate user data value:

.. code-block:: java

    sft.getUserData().put("geomesa.visibility.level", "attribute");
    dataStore.createSchema(sft);

When writing each feature, the per-attribute visibilities must be set in a comma-delimited string in the user data.
Each attribute must have a corresponding value in the delimited string, otherwise an error will be thrown.

For example, if your feature type has four attributes:

.. code-block:: java

    import org.locationtech.geomesa.security.SecurityUtils;

    SecurityUtils.setFeatureVisibility(feature, "admin,user,admin,user")
    // or, equivalently:
    feature.getUserData().put("geomesa.feature.visibility", "admin,user,admin,user");

Reading Visibility Labels
-------------------------

When reading back data, GeoMesa supports the concept of authorizations to determine who can see what.
Authorizations are a list of tags, corresponding to the visibility label tags being used. For example, a regular
user might have an authorization of ``user``, while an admin user might have an authorization of ``admin,user``.
Authorizations are simple string matches. They can be thought of as roles, although there is no hierarchy.

When running a query, GeoMesa supports a pluggable service provider interface for determining which authorizations
to use. Since different environments have different security architectures, there is no default implementation
available; users must provide their own service provider.

Providers must implement the following interface:

.. code-block:: java

    package org.locationtech.geomesa.security;

    public interface AuthorizationsProvider {

        /**
         * Gets the authorizations for the current context. This may change over time
         * (e.g. in a multi-user environment), so the result should not be cached.
         *
         * @return authorizations
         */
        List<String> getAuthorizations();

        /**
         * Configures this instance with parameters passed into the DataStoreFinder
         *
         * @param params parameters
         */
        void configure(Map<String, ? extends Serializable> params);
    }

When a GeoMesa data store is instantiated, it will scan for available service providers
via Java SPI. Third-party implementations can be enabled by placing them on the classpath
and including a special service descriptor file. See the
`Oracle Javadoc <http://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`__
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

.. warning::

    For HBase and Accumulo, which support native visibilities, the authorizations of the underlying connection
    will be used if nothing else is configured. This is convenient for testing, but is not a recommended approach
    for a production system.

    In addition, please note that the authorizations used in any scenario cannot exceed
    the authorizations of the underlying Accumulo or HBase connection.

For examples on implementing an ``AuthorizationsProvider`` see the :ref:`accumulo_tutorials_security` tutorials.
