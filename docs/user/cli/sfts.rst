.. _cli_sft_conf:

Defining Simple Feature Types
=============================

A ``SimpleFeatureType`` defines the names and types of attributes in a schema. Several of the command-line tools
require the definition of a ``SimpleFeatureType``, for example to create a new schema. GeoMesa commands support
several different ways to define one (in order of priority):

#. the well-known name of a ``SimpleFeatureType`` available on the classpath
#. a specification string
#. a TypeSafe configuration
#. the name of a file containing either a specification string or a TypeSafe configuration

Included Feature Types
----------------------

GeoMesa distributions ship with ``SimpleFeatureType`` definitions for several common data types
including Twitter, GeoNames, T-drive, and more. See :ref:`prepackaged_converters` for full details.

These types are available via classpath loading, as described next. Available types can
be examined using the :ref:`cli_env` command.

Classpath Loading
-----------------

GeoMesa uses the `TypeSafe Config <https://github.com/lightbend/config>`_ library for loading
``SimpleFeatureType``\ s from the classpath. Following convention, GeoMesa will load the default
configuration, which is defined by the files ``reference.conf`` and/or ``application.conf``. In
the binary distributions, those files are included in the ``conf`` directory, with a variety of
pre-defined types. See :ref:`prepackaged_converters` for more information on the provided types.

Users can modify these files to define their own ``SimpleFeatureType``\ s. By default,
``SimpleFeatureType``\ s should be defined as objects under the path ``geomesa.sfts``. They are
identified by their key.

The ``SimpleFeatureType`` object allows for the following keys:

* ``type-name`` - a string to specify the type name. If omitted, type name will default to the object key.
* ``attributes`` (required) - an array of fields (see below)
* ``user-data`` - arbitrary key-value pairs that will be set as user data in the ``SimpleFeatureType``

The ``attributes`` object allows for the definition of fields, which allow for the following keys:

* ``name`` (required) - the simple name for the attribute
* ``type`` (required) - the type of the attribute. See :ref:`attribute_types` for more details.

Other attribute keys will be set as user data on the attribute, and can be used to enabled attribute indexing,
indicate the default date field, etc. See :ref:`attribute_options` for more details.

Once a ``SimpleFeatureType`` has been defined on the classpath, it can be referenced by its well-known name, i.e.
the object key under ``geomesa.sfts``. Available types can be examined using the :ref:`cli_env` command.

Example
^^^^^^^

The following file defines a ``SimpleFeatureType`` called ``example``, with four attributes::

    geomesa = {
      sfts = {
         # other SFTs
         # ...
        example = {
          attributes = [
            { name = "name", type = "String", index = true                  }
            { name = "age",  type = "Integer"                               }
            { name = "dtg",  type = "Date",   default = true,  index = true }
            { name = "geom", type = "Point",  default = true , srid = 4326  }
          ]
        }
      }
    }

Specification String
--------------------

A ``SimpleFeatureType`` can also be defined as a string. The format for each attribute is ``name:type:foo=bar``,
where ``name`` is the attribute name, ``type`` is the attribute binding, and ``foo=bar`` is optional user data.
A full type is defined as 1-n attributes separated by commas, then (optionally), a semi-colon followed
by type-level user data separated with commas. The default geometry is specified with ``*``.

See :ref:`attribute_types` and :ref:`attribute_options` for more details on attribute types and user data.

Since a specification string does not contain a feature type name, the name has to be provided separately.

Example
^^^^^^^

This string corresponds to the configuration example above::

  name:String:index=true,age:Integer,dtg:Date:index=true:default=true,*geom:Point:srid=4326

Configuration String
--------------------

Instead of defining a ``SimpleFeatureType`` configuration on the classpath, the same configuration string
can be passed directly as an argument. Be careful to ensure the definition is still nested under
``geomesa.sfts``.

File Name
---------

Instead of defining them directly on the command line, a type can be defined in a file containing either a
specification string or a configuration string. The file can then be referenced by name.
