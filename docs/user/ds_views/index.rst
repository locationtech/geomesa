Combined Data Store Views
=========================

GeoMesa provides two generic options for combining multiple data stores into a single view. The first option
is a 'merged' view, where each data store contain a different set of features and they are queried concurrently.
This can be used, for example, to keep a fast, small cache of the most recent data, or to archive off older data
into lower-cost storage. The second option is a 'routed' view, where each data store contains the same set of
features and each query is sent to only one data store. This can be used, for example, to support secondary indexing.

In order to use a layer through the a data store view, the SimpleFeatureType must exist (and match) in all of
the underlying stores. If a schema exists in some stores but not all of them, it will not show up in the
combined view.

.. toctree::
    :maxdepth: 1

    merged_view
    routed_view
