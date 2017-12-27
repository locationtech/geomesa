.. _cli_converter_conf:

Defining Simple Feature Converters
==================================

A converter defines the mapping between input data and a ``SimpleFeatureType``. Several of the command-line tools
require the definition of a converter, for example to ingest data. See :ref:`converters` for full details
on creating converters.

GeoMesa commands support several different ways to define one (in order of priority):

#. the well-known name of a converter available on the classpath
#. the name of a file containing a TypeSafe configuration
#. a TypeSafe configuration as a string

Included Converters
-------------------

GeoMesa distributions ship with converter definitions for several common data types
including Twitter, GeoNames, T-drive, and more. See :ref:`prepackaged_converters` for full details.

These converters are available via classpath loading, as described next. Available converters can
be examined using the :ref:`cli_env` command.

Classpath Loading
-----------------

GeoMesa uses the `TypeSafe Config <https://github.com/lightbend/config>`_ library for loading
converters from the classpath. Following convention, GeoMesa will load the default
configuration, which is defined by the files ``reference.conf`` and/or ``application.conf``. In
the binary distributions, those files are included in the ``conf`` directory, with a variety of
pre-defined types. See :ref:`prepackaged_converters` for more information on the provided types.

Users can modify these files to define their own converters. By default, converters should be
defined as objects under the path ``geomesa.converters``. They are identified by their key.

See :ref:`converters` for details on creating converters.

Once a converter has been defined on the classpath, it can be referenced by its well-known name, i.e.
the object key under ``geomesa.converters``. Available converters can be examined using the
:ref:`cli_env` command.

Configuration String
--------------------

Instead of defining a converter configuration on the classpath, the same configuration string can
be passed directly as an argument. Be careful to ensure the definition is still nested under
``geomesa.converters``.

File Name
---------

Instead of defining them directly on the command line, a converter can be defined in a file containing
a configuration string. The file can then be referenced by name.
