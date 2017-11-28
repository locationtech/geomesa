.. _explain_query:

Explaining Query Plans
----------------------

GeoMesa will automatically log explain plans during query execution. This can be useful when debugging
query issues, and can inform decisions to speed up execution time, such as when to add additional indices
or when query hints may be helpful.

In order to show explain logging, configure your logging system to set
``org.locationtech.geomesa.index.utils.Explainer`` to ``trace`` level. For example, in log4j use:

.. code-block:: bash

    log4j.category.org.locationtech.geomesa.index.utils.Explainer=TRACE

Instead of passively logging, you can also generate explain logging explicitly without actually executing a query.
Given a GeoMesa data store and a query, use the following method:

.. code-block:: scala

    import org.locationtech.geomesa.index.utils.ExplainString

    dataStore.getQueryPlan(query, explainer = new ExplainPrintln)

``ExplainPrintln`` will write to ``System.out``. Alternatively, you can use ``ExplainString`` or
``ExplainLogging`` to redirect the output elsewhere.

GeoServer
^^^^^^^^^

For enabling explain loggingn in GeoServer, see :ref:`geoserver_explain_query`. It may also be helpful to
refer to GeoServer's `Advanced log configuration`__ documentation for the specifics of how and where to
manage the GeoServer logs.

__ http://docs.geoserver.org/stable/en/user/configuration/logging.html
