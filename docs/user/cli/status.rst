Environment and Status Commands
===============================

These commands are used to check the status of your environment. Required parameters are indicated with a ``*``.

``classpath``
-------------

Displays the runtime classpath. This can be useful for debugging class-loading issues.

.. _cli_env:

``env``
-------

Displays ``SimpleFeatureType``\ s and GeoMesa converters available on the classpath. See :ref:`cli_sft_conf`
and :ref:`cli_converter_conf` for more information on adding ``SimpleFeatureType``\ s and converters to the classpath.

========================= ==========================================================================
Argument                  Description
========================= ==========================================================================
``-s, --sfts``            Describe a specific ``SimpleFeatureType``
``-c, --converters``      Describe a specific converter
``--describe-sfts``       Describe all ``SimpleFeatureType``\ s
``--describe-converters`` Describe all converters
``--list-converters``     List all available converter names
``--list-sfts``           List all available type names
``--format``              Format to output ``SimpleFeatureType``\ s, one of ``typesafe`` or ``spec``
``--concise``             Render the output without unnecessary whitespace
``--exclude-user-data``   Don't include user data in output
========================= ==========================================================================

``help``
--------

Lists available commands. The arguments for specific commands can be shown using ``help <command>``.

``version``
-----------

Displays the version of the GeoMesa distribution.

``version-remote``
------------------

Displays the version of GeoMesa installed on the remote cluster, if available. This can be useful
to ensure GeoMesa versions match across an install.
