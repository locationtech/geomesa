Using the FileSystem Data Store in GeoServer
============================================

.. note::

    For general information on working with GeoMesa GeoServer plugins,
    see :doc:`/user/geoserver`.


Installing the FileSystem Data Store Plugin
-------------------------------------------

The FileSystem GeoServer plugin is bundled by default in a GeoMesa FS binary distribution. To install, extract
``$GEOMESA_FS_HOME/dist/gs-plugins/geomesa-fs-gs-plugin_2.11-$VERSION-install.tar.gz`` into GeoServer's
``WEB-INF/lib`` directory. Note that this plugin contains a shaded JAR with Parquet 1.9.0
bundled. If you require a different version, modify the ``pom.xml`` and build the GeoMesa FileSystem geoserver plugin
project from scratch with Maven.

This distribution does not include the Hadoop JARs; the following JARs should be copied from the ``lib`` directory of
your Hadoop installations into GeoServer's ``WEB-INF/lib`` directory:

(Note the versions may vary depending on your installation.)

  * hadoop-annotations-2.7.3.jar
  * hadoop-auth-2.7.3.jar
  * hadoop-common-2.7.3.jar
  * hadoop-mapreduce-client-core-2.7.3.jar
  * hadoop-yarn-api-2.7.3.jar
  * hadoop-yarn-common-2.7.3.jar
  * commons-configuration-1.6.jar

You can use the bundled ``$GEOMESA_FS_HOME/bin/install-hadoop.sh`` script to install these JARs.

The FileSystem data store requires the configuration file ``core-site.xml`` to be on the classpath. This can
be accomplished by placing the file in ``geoserver/WEB-INF/classes`` (you should make the directory if it
doesn't exist). Utilizing a symbolic link will be useful here so any changes are reflected in GeoServer.

.. code-block:: bash

    $ ln -s /path/to/core-site.xml /path/to/geoserver/WEB-INF/classes/core-site.xml

Restart GeoServer after the JARs are installed.

Adding a new Data Store in GeoServer
------------------------------------

From the main GeoServer page, create a new store by either clicking
"Add stores" in the middle of the **Welcome** page, or anywhere in the
interface by clicking "Data > Stores" in the left-hand menu and then
clicking "Add new Store".

If you have properly installed the GeoMesa FileSystem GeoServer plugin as described
above "FileSystem (GeoMesa)" should be included in the list
under **Vector Data Sources**. If you do not see this, check that you unpacked the
plugin JARs into in the right directory and restart GeoServer.

On the "Add Store" page, select "FileSystem (GeoMesa)". The data store requires two parameters:

* **fs.encoding** - the encoding of the files you have stored (e.g. parquet)
* **fs.path** - the path to the root of the datastore (e.g. s3a://mybucket/datastores/testds)

Click "Save", and GeoServer will search the filesystem path for any GeoMesa-managed feature types.
