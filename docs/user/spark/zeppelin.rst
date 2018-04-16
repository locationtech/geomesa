Deploying GeoMesa Spark with Zeppelin
=====================================

Apache `Zeppelin`_ is a web-based notebook server for interactive data analytics, which includes built-in
support for `Spark`_.

.. note::

    The instructions below have been tested with Zeppelin version |zeppelin_version|.

Installing Zeppelin
-------------------

Follow the `Zeppelin installation instructions`_, as well as the instructions for `configuring the Zeppelin Spark interpreter`_.

Configuring Zeppelin with GeoMesa
---------------------------------

The GeoMesa Accumulo Spark runtime JAR may be found either in the ``dist/spark`` directory of the GeoMesa Accumulo
binary distribution, or (after building) in the ``geomesa-accumulo/geomesa-accumulo-spark-runtime/target`` directory
of the GeoMesa source distribution.

#. In the Zeppelin web UI, click on the downward-pointing triangle next to the username in the upper-right hand corner and select "Interpreter".
#. Scroll to the bottom where the "Spark" interpreter configuration appears.
#. Click on the "edit" button next to the interpreter name (on the right-hand side of the UI).
#. In the "Dependencies" section, add the GeoMesa JAR, either as
     a. the full local path to the ``geomesa-accumulo-spark-runtime_2.11-$VERSION.jar`` described above, or
     b. the Maven groupId:artifactId:version coordinates (``org.locationtech.geomesa:geomesa-accumulo-spark-runtime_2.11:$VERSION``)
#. Click "Save". When prompted by the pop-up, click to restart the Spark interpreter.

It is not necessary to restart Zeppelin.

Data Plotting
-------------

Zeppelin provides built-in tools for visualizing quantitative data, which can be invoked by prepending
"%table\\n" to tab-separated output (see the `Zeppelin table display system`_). For example, the following method
may be used to print a ``DataFrame`` via this display system:

.. code-block:: scala

    def printDF(df: DataFrame) = {
      val dfc = df.collect
      println("%table")
      println(df.columns.mkString("\t"))
      dfc.foreach(r => println(r.mkString("\t")))
    }

It is also possible to use third-party libraries such as `Vegas`_, as described in :ref:`jupyter_vegas`. For
Zeppelin, the following implicit ``displayer`` method should be used:

.. code-block:: scala

    implicit val displayer: String => Unit = s => println("%html\n"+s)

.. |zeppelin_version| replace:: 0.7.0

.. _configuring the Zeppelin Spark interpreter: https://zeppelin.apache.org/docs/0.7.0/interpreter/spark.html
.. _Spark: http://spark.apache.org/
.. _Vegas: https://github.com/vegas-viz/Vegas/
.. _Zeppelin: https://zeppelin.apache.org/
.. _Zeppelin installation instructions: https://zeppelin.apache.org/docs/0.7.0/install/install.html
.. _Zeppelin table display system: https://zeppelin.apache.org/docs/0.7.0/displaysystem/basicdisplaysystem.html#table
