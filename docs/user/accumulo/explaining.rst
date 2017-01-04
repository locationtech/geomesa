.. _explain_query:

Explaining Query Plans
----------------------

Given a data store and a query, you can ask GeoMesa to explain its plan for how to execute the query:

.. code-block:: java

    dataStore.getQueryPlan(query, explainer = new ExplainPrintln);

Instead of ``ExplainPrintln``, you can also use ``ExplainString`` or ``ExplainLogging`` to redirect the explainer output elsewhere.

For enabling ``ExplainLogging`` in GeoServer, see :ref:`geoserver_explain_query`. It may also be helpful to refer to GeoServer's `Advanced log configuration <http://docs.geoserver.org/stable/en/user/configuration/logging.html>`_ documentation for the specifics of how and where to manage the GeoServer logs.

Knowing the plan -- including information such as the indexing strategy -- can be useful when you need to debug slow queries.  It can suggest when indexes should be added as well as when query-hints may expedite execution times.