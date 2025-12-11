Extending the Converter Library
-------------------------------

There are two ways to extend the converter library - adding new transformation functions and adding new data formats.

Adding Scripting Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~

The easiest way to extend functionality is by defining custom Javascript functions. To define a JavaScript function, create
a ``.js`` file with the function definition as the contents of the file. For example, if you have defined a function such as

.. code-block:: javascript

    function hello(s) {
       return "hello: " + s;
    }

you can reference that function in a transform expression as ``js:hello($2)``

Installing Custom Scripts
^^^^^^^^^^^^^^^^^^^^^^^^^

For local usage, geomesa defines the system property ``geomesa.convert.scripts.path`` to be a colon-separated list of script
files and/or directories containing scripts. This system property can be set when using the command line tools by setting the
``CUSTOM_JAVA_OPTS`` environmental variable:

.. code-block:: bash

    CUSTOM_JAVA_OPTS="-Dgeomesa.convert.scripts.path=/path/to/script.js:/path/to/script-dir/"

A more resilient method of including custom scripts is to package them as a JAR file and add it to the ``lib`` directory in
the command-line tools. The scripts need to be placed in the JAR under the path ``geomesa-convert-scripts/``. You can manually
create a JAR using the ``zip`` command:

.. code-block:: bash

    $ mkdir geomesa-convert-scripts/
    $ mv my-script.js geomesa-convert-scripts/
    $ zip -r my-scripts.jar geomesa-convert-scripts/
      adding: geomesa-convert-scripts/ (stored 0%)
      adding: geomesa-convert-scripts/my-script.js (deflated 2%)
    $ unzip -c my-scripts.jar
    Archive:  my-scripts.jar
     extracting: geomesa-convert-scripts/
      inflating: geomesa-convert-scripts/my-script.js
    function hello(s) {
      return "hello: " + s;
    }

Adding New Transformation Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To add new transformation functions, create a ``TransformationFunctionFactory`` and register it in a
``META-INF/services/org.locationtech.geomesa.convert2.transforms.TransformerFunctionFactory`` file.
For example, here's how to add a new transformation function that computes a SHA-256 hash:

.. code-block:: scala

    import org.locationtech.geomesa.convert2.transforms.TransformerFunctionFactory
    import org.locationtech.geomesa.convert2.transforms.TransformerFunction

    class SHAFunctionFactory extends TransformerFunctionFactory {
      override def functions = Seq(sha256fn)
      val sha256fn = TransformerFunction("sha256") { args =>
        Hashing.sha256().hashBytes(args(0).asInstanceOf[Array[Byte]])
      }
    }

The ``sha256`` function can then be used in a field as shown.

::

       fields: [
          { name = "hash", transform = "sha256(stringToBytes($0))" }
       ]

Adding New Data Formats
~~~~~~~~~~~~~~~~~~~~~~~

To add new data formats, implement the ``SimpleFeatureConverterFactory`` and ``SimpleFeatureConverter`` interfaces and register
them in ``META-INF/services`` appropriately. See ``org.locationtech.geomesa.convert.text.DelimitedTextConverter`` for an example.

Adding Functions to the Geomesa Classpath
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After creating a JAR file with your transformation function and factory, you can add these to the ``lib`` directory in
the command-line tools.

You can verify the classpath is properly configured with the ``classpath`` command:

.. code-block:: bash

    bin/geomesa-accumulo classpath
