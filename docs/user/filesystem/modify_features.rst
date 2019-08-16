Modifying Features
==================

The FileSystem data store supports modifying and deleting features through the regular GeoTools API methods,
however modifications introduce additional overhead at query time.

When a feature is modified or deleted, a new file is created containing the update, but, in order to ensure atomic
operations, the original file is not modified. Because of this, during a query the files in the affected partition(s)
must be read sequentially, so that modifications can be handled correctly. In Spark, this requires an extra sort and
merge step. Both of these things will slow down a query, and for very large modification sets can even exceed the
available memory due to tracking updates.

When a partition is compacted, the modifications will be merged back in with the original files, removing the
performance penalty. See :ref:`fsds_compact_command` for more information on compaction.
