.. _nifi_metrics:

Metrics Registries
------------------

Various GeoMesa processors will publish metrics using the `Micrometer <https://docs.micrometer.io/micrometer/reference/>`__
framework. Registries can be managed through controller services that implement
``org.geomesa.nifi.datastore.services.MetricsRegistryService``, which can then be referenced by a processor. Currently
only the Prometheus registry is supported.

Prometheus Registry
~~~~~~~~~~~~~~~~~~~

The Prometheus registry runs a server that can be scraped for metrics.

+-------------------------------+-----------------------------------------------------------------------------------------+
| Property                      | Description                                                                             |
+===============================+=========================================================================================+
| ``Server port``               | The port used to host Prometheus metrics                                                |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``Rename Prometheus metrics`` | Rename metrics according to Prometheus standard names, instead of Micrometer names      |
+-------------------------------+-----------------------------------------------------------------------------------------+
| ``Application name tag``      | Add an 'application' tag to all metrics                                                 |
+-------------------------------+-----------------------------------------------------------------------------------------+
