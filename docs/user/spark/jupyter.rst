Deploying GeoMesa Spark with Jupyter Notebook
=============================================

`Jupyter Notebook`_ is a web-based application for creating interactive documents containing runnable code, visualizations, and text. Via the `Apache Toree`_ kernel, Jupyter can be used for preparing spatio-temporal analyses in Scala and submitting them in `Spark`_. The guide below describes how to configure Jupyter with Spark 2.0 and GeoMesa.

.. _jupyter_prerequisites:

Prerequisites
-------------

`Spark`_ 2.0 should be installed, and the environment variable ``SPARK_HOME`` should be set. Spark 2.0 and above requires Scala version 2.11.

`Python`_ 2.7 or 3.x should be installed, along with the Python development tools. For example, for Python 2.7 on Ubuntu:

.. code-block:: bash

    $ sudo apt-get install build-essential python-dev

For Python 2.7 on CentOS:

.. code-block:: bash

   $ sudo yum groupinstall 'Development Tools'
   $ sudo yum install python-devel

Building the Toree kernel for Spark 2.0 requires Git, GNU Make, `Docker`_, and `SBT`_.

Installing Jupyter
------------------

Jupyter may be installed via ``pip`` (for Python 2.7) or ``pip3`` (for Python 3.x):

.. code-block:: bash

    $ pip install --upgrade jupyter

or

.. code-block:: bash

    $ pip3 install --upgrade jupyter

Installing the Toree Kernel
---------------------------

Toree version 0.2.0-dev1 supports Spark 2.0 and above, and should be built from source as described via its `README file`_. Check out the project from GitHub:

.. _README file: https://github.com/apache/incubator-toree/blob/master/README.md

.. code-block:: bash

    $ git clone https://github.com/apache/incubator-toree.git
    $ cd incubator-toree
    $ git checkout v0.2.0-dev1

Build the project using ``make``; as described in :ref:`jupyter_prerequisites`, `Docker`_ and `SBT`_ are required:

.. code-block:: bash

    $ make clean
    $ make release

The built Toree distribution can then be installed via ``pip`` (or ``pip3``):

.. code-block:: bash

    $ pip install --upgrade --pre ./dist/toree-pip/toree-0.2.0.dev1.tar.gz
    $ pip freeze | grep toree
    toree==0.2.0.dev1

Configure Toree and GeoMesa
---------------------------

If you have the GeoMesa Accumulo distribution installed at ``GEOMESA_ACCUMULO_HOME`` as described in :ref:`setting_up_accumulo_commandline`, you can run the following example script to configure Toree with GeoMesa version ``VERSION``:

.. code-block:: bash

    #!/bin/sh

    # bundled GeoMesa Accumulo Spark and Spark SQL runtime JAR
    # (contains geomesa-accumulo-spark, geomesa-spark-core, geomesa-spark-sql, and dependencies)
    jars="file://$GEOMESA_ACCUMULO_HOME/dist/spark/geomesa-accumulo-spark-runtime_2.11-$VERSION.jar"

    # uncomment to use the converter or GeoTools RDD providers
    #jars="$jars,file://$GEOMESA_ACCUMULO_HOME/lib/geomesa-spark-converter_2.11-$VERSION.jar"
    #jars="$jars,file://$GEOMESA_ACCUMULO_HOME/lib/geomesa-spark-geotools_2.11-$VERSION.jar"

    # uncomment to work with shapefiles (requires $GEOMESA_ACCUMULO_HOME/bin/install-jai.sh)
    #jars="$jars,file://$GEOMESA_ACCUMULO_HOME/lib/jai_codec-1.1.3.jar"
    #jars="$jars,file://$GEOMESA_ACCUMULO_HOME/lib/jai_core-1.1.3.jar"
    #jars="$jars,file;//$GEOMESA_ACCUMULO_HOME/lib/jai_imageio-1.1.jar"

    jupyter toree install \
        --replace \
        --user \
        --kernel_name "GeoMesa Spark $VERSION" \
        --spark_home=${SPARK_HOME} \
        --spark_opts="--master yarn --jars $jars"

.. note::

    The JARs specified will be in the respective ``target`` directory of each module of the source distribution if you built GeoMesa from source.

You may also consider adding ``geomesa-tools-2.11-$VERSION-data.jar`` to include prepackaged converters for publically available data sources (as described in :ref:`prepackaged_converters`), or ``geomesa-jupyter-2.11-$VERSION.jar`` to include an interface for the `Leaflet`_ visualization library.

Running Jupyter
---------------

For public notebooks, you should `configure Jupyter`_ to use a password and bind to a public IP address. To run Jupyter with the GeoMesa Spark kernel:

.. _configure Jupyter: http://jupyter-notebook.readthedocs.io/en/latest/public_server.html#running-a-notebook-server

.. code-block:: bash

    $ jupyter notebook

.. note::

    Long-lived processes should probably be hosted in ``screen``, ``systemd``,
    or ``supervisord``.

Your notebook server should launch and be accessible at http://localhost:8888/ (or the address and port you bound the server to).

.. _Apache Toree: https://toree.apache.org/
.. _Docker: https://www.docker.com/
.. _Jupyter Notebook: http://jupyter.org/
.. _Leaflet: http://leafletjs.com/
.. _Python: https://www.python.org/
.. _SBT: http://www.scala-sbt.org/
.. _Spark: http://spark.apache.org/