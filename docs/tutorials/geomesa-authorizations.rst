GeoMesa Authorizations
======================

This tutorial will show you how to:

1. Set visibilities on your data during ingestion through GeoMesa.
2. Apply authorizations to your queries through GeoMesa.
3. Implement user authorizations through the GeoMesa GeoServer plugin,
   using PKI certs to authenticate with GeoServer and LDAP to store
   authorizations.
4. Query GeoServer over WFS using a Java client using PKI certs for
   authentication.

Background
----------

Visibilities and Authorizations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One of the most powerful features of Accumulo is the implementation of
cell-level security, using **visibilities** and **authorizations**.
Data that is protected by visibilities can only be seen by users that
have the corresponding authorizations. This allows for the fine-grained
protection of data, based on arbitrary labels.

*Note: authorizations are distinct from table-level permissions, and
operate at a much finer grain.*

If you are not familiar with Accumulo authorizations, you should review
the relevant Accumulo
`documentation <http://accumulo.apache.org/1.5/accumulo_user_manual.html#_security>`__,
with more examples
`here <http://accumulo.apache.org/1.5/examples/visibility.html>`__.

Public Key Infrastructure (PKI)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Public key infrastructure can be used to securely authenticate end
users. In PKI, a **certificate authority** (CA) will issue digital
certificates that verify that a particular public key belongs to a
particular individual. Other users can then trust that certificate
because it has been digitally signed by the CA.

In this tutorial, the keys used are not provided by trusted CAs. As
such, it is necessary to import the CA's certificate into the Java
keystore, which allows Java (and by extension Tomcat) to trust any keys
verified by the CA.

PKI solves the issue of **authentication** (*who* a user is) but not
**authorization** (*what* a user can do). For this tutorial,
authorization is provided by an LDAP server.

Prerequisites
-------------

.. warning::

    You will need access to a Hadoop 2.2 installation as well as an Accumulo |accumulo_version| database.

.. warning::
    
    You will need to have ingested GDELT data using GeoMesa. Instructions are 
    available in the :doc:`geomesa-gdelt-analysis` tutorial.

You will also need:

-  an Accumulo user that has appropriate permissions to query your data,
-  Java JDK 7,
-  `Apache Maven <http://maven.apache.org/>`__ 3.2.2 or better, and
-  a `git <http://git-scm.com/>`__ client.

Visibilities in GeoMesa
-----------------------

Currently GeoMesa supports applying a single set of visibilities to all
data in a ``DataStore``. When configuring a ``DataStore``, the
visibilities can be set with the ``visibilities`` parameter:

.. code-block:: java

    // create a map containing initialization data for the GeoMesa data store
    Map<String, String> configuration = new HashMap<String, String>();
    configuration.put("visibilities", "user&admin");
    DataStore dataStore = DataStoreFinder.getDataStore(configuration);

Any data written by this ``DataStore`` will have the visibilities
"user&admin" applied.

Future versions of GeoMesa will allow visibilities to be applied at a
more granular level.

Authorizations In GeoMesa
-------------------------

When performing a query, GeoMesa delegates the retrieval of
authorizations to **service providers** that implement the following
interface:

.. code-block:: java

    package geomesa.core.security;

    public interface AuthorizationsProvider {

        /**
         * Gets the authorizations for the current context. This may change over time (e.g. in a
         * multi-user environment), so the result should not be cached.
         *
         * @return
         */
        public Authorizations getAuthorizations();

        /**
         * Configures this instance with parameters passed into the DataStoreFinder
         *
         * @param params
         */
        public void configure(Map<String, Serializable> params);
    }

When a GeoMesa ``DataStore`` is instantiated, it will scan for available
service providers. Third-party implementations can be enabled by simply
placing them in the classpath. See the Oracle
`Javadoc <http://docs.oracle.com/javase/7/docs/api/javax/imageio/spi/ServiceRegistry.html>`__
for details on implementing a service provider.

The GeoMesa ``DataStore`` will call ``configure()`` on the
``AuthorizationsProvider`` implementation, passing in the parameter map
from the call to ``DataStoreFinder.getDataStore(Map params)``. This
allows the AuthorizationsProvider to configure itself based on the
environment.

To ensure that the correct ``AuthorizationsProvider`` is used, GeoMesa
will throw an exception if multiple third-party service providers are
found on the classpath. In this scenario, the particular service
provider class to use can be specified by the following system property:

.. code-block:: java

    geomesa.core.security.AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY = "geomesa.auth.provider.impl";

For simple scenarios, the set of authorizations to apply to all queries
can be specified when creating the GeoMesa ``DataStore`` by using the
``auths`` configuration parameter. This will use the
``DefaultAuthorizationsProvider`` implementation provided by GeoMesa.

.. code-block:: java

    // create a map containing initialization data for the GeoMesa data store
    Map<String, String> configuration = new HashMap<String, String>();
    configuration.put("auths", "user,admin");
    DataStore dataStore = DataStoreFinder.getDataStore(configuration);

If there are no ``AuthorizationsProvider``\ s found on the classpath,
and the ``auths`` parameter is not set, GeoMesa will default to using
the authorizations associated with the Accumulo connection (i.e. the
``user`` configuration value).

**Note: this is not a recommended approach for a production system.**

In addition, please note that the authorizations used in any scenario
cannot exceed the authorizations of the Accumulo connection.

Ingest GDELT Data with Visibilities
-----------------------------------

The rest of this tutorial will use the GDELT data set, described in the
`GDELT Map-Reduce tutorial </geomesa-gdelt-analysis/>`__. If you have
never ingested GDELT data, or you have previously ingested it
**without** visibilities, you will need to ingest it again.

Follow the instructions `here </geomesa-gdelt-analysis/>`__, with the
following changes:

-  Ensure that you have the latest version of the GeoMesa tutorial code
   from GitHub.
-  When executing the map/reduce job, include the following parameter:

.. code-block:: bash

       -visibilities <visibilities>

The entire command will be as follows:

.. code-block:: bash

    $ hadoop jar /path/to/geomesa-examples-gdelt-$VERSION.jar \
       com.example.geomesa.gdelt.GDELTIngest                         \
       -instanceId <accumulo-instance-id>                \
       -zookeepers <zookeeper-hosts-string>              \
       -user <username> -password <password>             \
       -visibilities <visibilities>                      \
       -tableName <table> -featureName <feature>         \
       -ingestFile hdfs:///gdelt/uncompressed/gdelt.tsv

The visibility string can be anything valid for your Accumulo instance.
For the rest of this exercise, we are going to assume the visibility
string is "user", and the Accumulo table is "gdelt\_auths". You can see
the visibilities that are currently enabled for your user through the
``accumulo`` shell:

.. code-block:: bash

    $ accumulo shell -u <username> -p <password>

    Shell - Apache Accumulo Interactive Shell
    -
    - version: 1.5.1
    - instance name: mycloud
    - instance id: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    -
    - type 'help' for a list of available commands
    -
    myuser@mycloud> getauths
    user,admin

If your user does not already have authorizations, you can add them
through the Accumulo shell with the ``addauths`` command:

**Note: A user cannot set authorizations unless the user has the
System.ALTER\_USER permission.**

.. code-block:: bash

    myuser@mycloud> getauths
    user
    myuser@mycloud> addauths -s admin -u myuser
    myuser@mycloud> getauths
    user,admin

Once the GDELT data is ingested, you should see a visibility label in
square brackets when you scan the spatio-temporal index table through
the Accumulo shell:

.. code-block:: bash

    myuser@mycloud> table gdelt_auths_gdelt_st_idx
    myuser@mycloud gdelt_auths_gdelt_st_idx> scan
    00~gdelt~04e~20080125 169881494:SimpleFeatureAttribute [user]    \x02\x12169881494\x00\xAC\xBE...

Download and Build the Tutorial Code
------------------------------------

Clone the tutorial code:

.. code-block:: bash

    $ git clone https://github.com/geomesa/geomesa-tutorials.git

The authorizations tutorial is located in the ``geomesa-examples-authorizations``
directory:

.. code-block:: bash

    $ cd com.example.geomesa.authorizations./geomesa-examples-authorizations

The ``pom.xml`` file here contains an explicit list of dependent libraries
that will be bundled together into the final tutorial. You should
confirm that the versions of Accumulo and Hadoop match what you are
running; if it does not match, change the value in the POM.

.. note::

    The only reason these libraries are bundled into the final JAR is that this
    is easier for most people to do this than it is to set the classpath
    when running the tutorial. If you would rather not bundle these
    dependencies, mark them as provided in the POM, and update your
    classpath as appropriate.

From within this directory, run:

.. code-block:: bash

    $ mvn clean install

When this is complete, it will have built a JAR file that contains all
of the code you need to run the tutorial.

Run the Tutorial
----------------

On the command-line, run:

.. code-block:: bash

    $ java -cp ./target/geomesa-examples-authorizations-1.0-SNAPSHOT.jar \
       com.example.geomesa.authorizations.AuthorizationsTutorial \
       -instanceId <instance> \
       -zookeepers <zoos> \
       -user <user> \
       -password <pwd> \
       -visibilities <visibilities> \
       -tableName <table> \
       -featureName <feature>

where you provide the following arguments:

-  ``<instance>``: the name of your Accumulo instance
-  ``<zoos>``: comma-separated list of your Zookeeper nodes, e.g.
   ``zoo1:2181,zoo2:2181,zoo3:2181``
-  ``<user>``: the name of an Accumulo user that will execute the scans,
   e.g. ``root``
-  ``<pwd>``: the password for the previously-mentioned Accumulo user
-  ``<visibilities>``: the visibilities used to ingest the GDELT
   dataset, e.g. ``user``
-  ``<table>``: the name of the Accumulo table that has the GeoMesa
   GDELT dataset, e.g. ``gdelt_auths``
-  ``<feature>``: the feature name used to ingest the GeoMesa GDELT
   dataset, e.g. ``gdelt``

You should see two queries run and the results printed out to your
console. You should see output similar to the following:

.. code-block:: bash

    Executing query with AUTHORIZED data store: auths are 'user,admin'
    Results:
    1|geom=POINT (33.9744 45.2908)

    Executing query with UNAUTHORIZED data store: auths are ''
    No results

The first query should return 1 or more results. The second query should
return 0 results, since they are hidden by visibilities.

Looking Closer at the Code
--------------------------

The code for querying with authorizations is available in the class
``AuthorizationsTutorial``.

The interesting code for this tutorial is contained in the ``main``
method:

.. code-block:: java

    // get an instance of the data store that uses the default authorizations provider, which
    // will use whatever auths the connector has available
    System.setProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY,
        DefaultAuthorizationsProvider.class.getName());
    DataStore authDataStore = DataStoreFinder.getDataStore(dsConf);

    // get another instance of the data store that uses our authorizations provider that
    // always returns empty auths
    System.setProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY,
        EmptyAuthorizationsProvider.class.getName());
    DataStore noAuthDataStore = DataStoreFinder.getDataStore(dsConf);

This code snippet shows how you can specify the
``AuthorizationProvider`` to use with a system property. The
``DefaultAuthorizationsProvider`` class is provided by GeoMesa, and used
when no other implementations are found. The
``EmptyAuthorizationsProvider`` class is included in the tutorial:

.. code-block:: java

    com.example.geomesa.authorizations.EmptyAuthorizationsProvider

The ``EmptyAuthorizationsProvider`` will always return an empty
``Authorizations`` object, which means that any data stored with
visibilities will not be returned.

There is a more useful implementation of ``AuthorizationsProvider`` that
will be explored in more detail in the next section:

.. code-block:: java

    com.example.geomesa.authorizations.LdapAuthorizationsProvider
    com.example.geomesa.authorizations.LdapAuthorizationsProviderTest

There is a class that shows how to query GeoServer through WFS that will
be explored in more detail later in the tutorial:

.. code-block:: java

    com.example.geomesa.authorizations.GeoServerAuthorizationsTutorial

Additionally, there are two helper classes included in the tutorial:

-  ``com.example.geomesa.authorizations.GdeltFeature`` - Contains the attributes available
   in the GDELT data set.
-  ``com.example.geomesa.authorizations.SetupUtil`` - Handles reading command-line
   arguments

Applying Authorizations and Visibilities to GeoServer Using PKIs And LDAP
-------------------------------------------------------------------------

This section will show you how to configure GeoServer to authenticate
users with PKIs, use LDAP to store authorizations, then apply
authorizations on a per-user/per-query basis.

Basic user authentication will take place via user certificates. Each
user will have their own public/private key pair that uniquely
identifies them.

User authorizations will come from LDAP. Once a user's identity has been
verified via PKI, we will look up the user's details in LDAP.

Once we have a user's authentication and authorizations, we will apply
them to the GeoMesa query using a custom ``AuthorizationsProvider``
implementation.

.. note:: 

    It is assumed for the rest of the tutorial that you have created
    the GeoServer data stores and layers outlined in the GDELT
    tutorial </geomesa-gdelt-analysis/>

Run GeoServer in Tomcat
~~~~~~~~~~~~~~~~~~~~~~~

*Note: If you are already running GeoServer in Tomcat, you can skip this
step.*

GeoServer ships by default with an embedded Jetty servlet. In order to
use PKI login, we need to install it in Tomcat instead.

1. Download and install Tomcat 7.
2. Create an environment variable pointing to your Tomcat installation (you
   may want to add this to your bash init scripts):

.. code-block:: bash

    export CATALINA_HOME=/path/to/tomcat

3. If you want to reuse your existing GeoServer configuration, create an
   environment variable pointing to your GeoServer data directory (you may
   want to add this to your shell initialization scripts):

.. code-block:: bash

    export GEOSERVER_DATA_DIR=/path/to/geoserver/data_dir

4. Copy the GeoServer webapp from the GeoServer distribution into the
   tomcat servlet:

.. code-block:: bash

    cp -r /path/to/geoserver/webapps/geoserver/ $CATALINA_HOME/webapps/

5. Increase the memory allocated to Tomcat, which you will need for running
   complex queries in GeoServer (the values here may not be applicable for
   every installation):

.. code-block:: bash

    cd $CATALINA_HOME/bin
    echo 'CATALINA_OPTS="-Xmx2g -XX:MaxPermSize=128m"' >> setenv.sh

6. Start Tomcat, either as a service or through the startup scripts, and
   ensure that GeoServer is available at http://localhost:8080/geoserver/web/.

Create the Accumulo Data Store and Layer in GeoServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you haven't already, create an AccumuloDataStore and associated Layer
pointing to the data with visibilities, as described in the `GDELT
tutorial </geomesa-gdelt-analysis/>`__.

When configuring the DataStore, leave the **auths** field empty and set
the **visibilities** field to what you used when ingesting data above.

Configure GeoServer for PKI Login
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Follow the instructions located
`here <http://docs.geoserver.org/stable/en/user/security/tutorials/cert/index.html>`__
in order to enable PKI login to GeoServer.

In the step where you add the 'cert' filter to the 'Filter Chains', also
add it to the 'rest', 'gwc' and 'default' chains (in addition to web).
We will be using the 'rod' and 'scott' users, so be sure to install
those into your browser.

.. note::

    There is a bug in some versions of GeoServer, where it sometimes
    does not save authentication filters properly.

If, after going through the above steps, you do not get logged in
properly, do the following:

1. Shut down GeoServer.
2. Navigate to the GeoServer data directory: ``$GEOSERVER_DATA_DIR``
   or ``$GEOSERVER_HOME/data_dir``
3. Edit the file ./security/config.xml by adding the 4 lines below:

.. code-block:: xml

    <filterChain>
      <filters name="web" class="org.geoserver.security.HtmlLoginFilterChain" interceptorName="interceptor" exceptionTranslationName="exception" path="/web/**,/gwc/rest/web/**,/" disabled="false" allowSessionCreation="true" ssl="false" matchHTTPMethod="false">
        <filter>rememberme</filter>
        <filter>cert</filter> <!--add this line -->
        <filter>form</filter>
        <filter>anonymous</filter>
      </filters>
      ...
      <filters name="rest" class="org.geoserver.security.ServiceLoginFilterChain" interceptorName="restInterceptor" exceptionTranslationName="exception" path="/rest/**" disabled="false" allowSessionCreation="false" ssl="false" matchHTTPMethod="false">
        <filter>cert</filter> <!--add this line -->
        <filter>basic</filter>
        <filter>anonymous</filter>
      </filters>
      <filters name="gwc" class="org.geoserver.security.ServiceLoginFilterChain" interceptorName="restInterceptor" exceptionTranslationName="exception" path="/gwc/rest/**" disabled="false" allowSessionCreation="false" ssl="false" matchHTTPMethod="false">
        <filter>cert</filter> <!--add this line -->
        <filter>basic</filter>
      </filters>
      <filters name="default" class="org.geoserver.security.ServiceLoginFilterChain" interceptorName="interceptor" exceptionTranslationName="exception" path="/**" disabled="false" allowSessionCreation="false" ssl="false" matchHTTPMethod="false">
        <filter>cert</filter> <!--add this line -->
        <filter>basic</filter>
        <filter>anonymous</filter>
      </filters>
    </filterChain>

4. Restart GeoServer.
5. Verify that the 'web' filter chain has the 'cert' filter selected.

Install an LDAP Server for Storing Authorizations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Note: If you are already have an LDAP server set up, you can skip this
step.*

1. Download and install
   `ApacheDS <http://directory.apache.org/apacheds/>`__
2. Either run as a service, or run through the start scripts:

.. code-block:: bash

    $ cd apacheds-2.0.0-M20/bin
    $ chmod 755 *.sh
    $ ./apacheds.sh

Configure LDAP for Storing Authorizations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We want to configure LDAP with a user to match the Spring Security PKIs
we are testing with. The end result we want is to create the following
user:

``DN: cn=rod,ou=Spring Security,o=Spring Framework``

In order to do that, we will use Apache Directory Studio.

1. Download and run `Apache Directory
   Studio <http://directory.apache.org/studio/>`__.
2. Connect to the your LDAP instance (ApacheDS), using the instructions
   `here <http://directory.apache.org/apacheds/basic-ug/1.4.2-changing-admin-password.html>`__
   (note: you do not need to change the password unless you want to).
3. Create a partition for our data:

   1. Right-click the 'ApacheDS (localhost)' entry under the
      'Connection' tab and select 'Open Configuration'.
   2. Click 'Advanced Partitions Configuration...'.
   3. Click 'Add'.
   4. Set the ID field to be 'Spring Framework'.
   5. Set the Suffix field to be 'o=Spring Framework'.
   6. Uncheck 'Auto-generate context entry from suffix DN'.
   7. Set the following attributes in Context Entry:

      -  objectclass: extensibleObject
      -  objectclass: top
      -  objectclass: domain
      -  dc: Spring Framework2
      -  o: Spring Framework2

   8. Hit **Ctrl-s** to save the partition. 
         
   |ApacheDS Partition|

4. **Restart ApacheDS.** Otherwise the partition will not be available
   and the LDIF import will fail.
5. Load the LDIF file
   :download:`spring-security-rod.ldif <_static/assets/tutorials/2014-06-04-geomesa-authorizations/spring-security-rod.ldif>`,
   which will create the Spring Security OU and the 'rod' user:

   -  Right-click the 'Root DSE' node in the LDAP browser, and select
      'Import->LDIF import...'

Test LDAP Connection Using Tutorial Code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The tutorial code includes an ``AuthorizationsProvider`` implementation
that will connect to LDAP to retrieve authorizations, in the class
``com.example.geomesa.authorizations.LdapAuthorizationsProvider``.

The provider will configure itself based on the
``geomesa-ldap.properties`` file on the classpath (under
``src/main/resources``):

.. code-block:: properties

    # ldap connection properties
    java.naming.factory.initial=com.sun.jndi.ldap.LdapCtxFactory
    java.naming.provider.url=ldap://localhost:10389
    java.naming.security.authentication=simple
    java.naming.security.principal=uid=admin,ou=system
    java.naming.security.credentials=secret

    # the ldap node to start the query from
    geomesa.ldap.search.root=o=Spring Framework
    # the query that will be applied to find the user's record
    # the '{}' will be replaced with the common name from the certificate the user has logged into geoserver with
    geomesa.ldap.search.filter=(&(objectClass=person)(cn={}))
    # the ldap attribute that holds the comma-delimited authorizations for the user
    geomesa.ldap.auths.attribute=employeeType

The default file included with the tutorial will connect to the LDAP
instance we set up in the previous steps. If you are using a different
LDAP configuration, you will need to modify the file appropriately.

The ``LdapAuthorizationsProvider`` will look for a particular LDAP
attribute that stores the user's authorizations in a comma-delimited
list. For simplicity, in this tutorial we have re-purposed an existing
attribute, ``employeeType``. The attribute to use can be modified
through the property file.

When we inserted the 'rod' record into LDAP, we set his ``employeeType``
to 'user,admin', corresponding to our Accumulo authorizations. If you
are using different authorizations, you will need to update the
attribute to match.

The tutorial code includes a test case for connecting to LDAP, in the
class ``com.example.geomesa.authorizations.LdapAuthorizationsProviderTest``.

Once you have modified ``geomesa-ldap.properties`` to connect to your
LDAP, you can test the connection by running this test class:

.. code-block:: bash

    $ java -cp ./target/geomesa-examples-authorizations-$VERSION.jar \
       com.example.geomesa.authorizations.LdapAuthorizationsProviderTest rod

The argument to the program ('rod') is the user to retrieve
authorizations for. You should get the following output:

.. code-block:: bash

    Checking auths from LDAP for user 'rod'
    Retrieved auths: user,admin

Installing the LDAP AuthorizationProvider in GeoServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to use the ``LdapAuthorizationsProvider``, we need to install
it as a service provider into GeoServer, where it will automatically be
picked up by GeoMesa.

The tutorial code includes a service provider registry in the
``META-INF/services`` folder. By default, the provider class is
specified as the ``EmptyAuthorizationsProvider``.

1. Ensure that your LDAP configuration is correct by running
   ``LdapAuthorizationsProviderTest``, as described above.

2. Change the provider class in
   ``src/main/resources/META-INF/services/geomesa.core.security.AuthorizationsProvider``
   to be ``com.example.geomesa.authorizations.LdapAuthorizationsProvider``.

3. Rebuild the tutorial JAR and install the unshaded original jar in
   GeoServer:

.. code-block:: bash

    $ mvn clean install
    $ cp ./target/original-geomesa-examples-authorizations-$VERSION.jar \
       /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

.. note::

    We want to use the unshaded jar since all the required
    dependencies are already installed in GeoServer.

4. Restart GeoServer (or start it if it is not running).

At this point you should have everything configured and in-place.

Verifying the LDAP Authorizations in GeoServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to verify that the authorizations are working correctly,
execute a query against GeoMesa by calling the WMS provider over HTTPS
in your browser:

.. code-block:: bash

    https://localhost:8443/geoserver/wms?service=WMS&version=1.1.0&request=GetMap&layers=geomesa:gdelt_auths&styles=&bbox=31.6,44,37.4,47.75&width=1200&height=600&srs=EPSG:4326&format=application/openlayers&TIME=2013-01-01T00:00:00.000Z/2014-04-30T23:00:00.000Z

When prompted, select the 'rod' certificate.

You should see the normal data come back, with many red points
indicating the data:

.. figure:: _static/img/tutorials/2014-06-04-geomesa-authorizations/Ukraine_Unfiltered.png
   :alt: Authorized Results

   Authorized Results

Now try the same query, but use the 'scott' certificate. This time,
there should be no data returned, as the 'scott' user does not have any
authorizations set up in LDAP.

.. note::

    A simple way to use different certificates at once is to open
    multiple 'incognito' or 'private' browser windows.

Querying GeoServer through a Web Feature Service (WFS) with a Java Client
-------------------------------------------------------------------------

GeoServer provides the ability to query data through a Web Feature
Service (WFS). Using GeoTools, we can create a client in Java through a
WFSDataStore. More details are available
`here <http://docs.geotools.org/latest/userguide/library/data/wfs.html>`__
and
`here <http://docs.geoserver.org/stable/en/user/services/wfs/reference.html>`__,
although some of the documentation is out of date.

We can leverage the same PKI and LDAP setup that we used through the web
interface to authenticate our client.

Go back to the tutorial folder, and execute the following command:

.. code-block:: bash

    $ java -cp ./target/geomesa-examples-authorizations-1.0-SNAPSHOT.jar \
       -Djavax.net.ssl.keyStore=/path/to/certs/rod.p12 \
       -Djavax.net.ssl.keyStorePassword=password \
       -Djavax.net.ssl.keyStoreType=PKCS12 \
       -Djavax.net.ssl.trustStore=/path/to/certs/server.jks \
       -Djavax.net.ssl.trustStorePassword=password \
       -Djavax.net.ssl.trustStoreType=JKS
       com.example.geomesa.authorizations.GeoServerAuthorizationsTutorial \
       -geoserverUrl <url> \
       -featureStore <featureStore> \

where you provide the following arguments:

-  ``<url>``: the **HTTPS** path to GeoServer, e.g.
   ``https://localhost:8443/geoserver/``
-  ``<featureStore>``: the name of the data store created in GeoServer,
   including the workspace, e.g. ``geomesa:gdelt``
-  ``javax.net.ssl.*``: SSL configuration system properties. Note that
   these need to be before the class name, otherwise they will be
   treated as arguments to the program.

.. note::

    **Ensure that the URL for GeoServer is using HTTPS.**

.. note::

    The feature store needs to be namespaced with the GeoServer
    workspace. The workspace and store name are separated with a colon.

.. note::

    If you happen to have two GeoServer data stores with the same
    name but different workspaces, you will need to delete or rename one of
    them. There is a bug in GeoServer where it might return the wrong
    features if there are two data stores with the same name.

The system properties will control the keystore that is used for
authentication. For the first command, we are using the ``rod.p12``
certificate. Upon execution, you should see the following output:

.. code-block:: bash

    Executing query against 'https://localhost:8443/geoserver/wfs?request=GetCapabilities&version=1.0.0' with client keystore '/path/to/certs/rod.p12'
    INFO: Cached XML schema: https://localhost:8443/geoserver/wfs?service=WFS&version=1.0.0&request=DescribeFeatureType&typeName=geomesa%3Agdelt
    Results:
    1|geom=POINT (33.9744 45.2908)

If you re-execute the command, but use the ``scott.p12`` cert instead,
you should get no results:

.. code-block:: bash

    Executing query against 'https://localhost:8443/geoserver/wfs?request=GetCapabilities&version=1.0.0' with client keystore '/path/to/certs/scott.p12'
    INFO: Cached XML schema: https://localhost:8443/geoserver/wfs?service=WFS&version=1.0.0&request=DescribeFeatureType&typeName=geomesa%3Agdelt
    No results

Looking Closer at the Code
--------------------------

The code for querying through WFS is available in the class
``com.example.geomesa.authorizations.GeoServerAuthorizationsTutorial``. The interesting
code for this tutorial is contained in the ``main`` method:

.. code-block:: java
    :linenos:

    // create the URL to GeoServer. Note that we need to point to the 'GetCapabilities' request,
    // and that we are using WFS version 1.0.0
    String geoserverUrl = geoserverHost + "wfs?request=GetCapabilities&version=1.0.0";

    // create the geotools configuration for a WFS data store
    Map<String, String> configuration = new HashMap<String, String>();
    configuration.put(WFSDataStoreFactory.URL.key, geoserverUrl);
    configuration.put(WFSDataStoreFactory.WFS_STRATEGY.key, "geoserver");
    configuration.put(WFSDataStoreFactory.TIMEOUT.key, cmd.getOptionValue(SetupUtil.TIMEOUT, "99999"));

    //...

    // verify we have gotten the correct datastore
    WFSDataStore wfsDataStore = (WFSDataStore) DataStoreFinder.getDataStore(configuration);

This code snippet shows how you can get a GeoTools ``DataStore`` that
connects to GeoServer through WFS. Once you have obtained the data
store, you can query it just like any other data store, and the
implementation details will be transparent.

.. |ApacheDS Partition| image:: _static/img/tutorials/2014-06-04-geomesa-authorizations/apache-ds-partition.png
