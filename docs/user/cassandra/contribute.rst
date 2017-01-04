How you can contribute
======================

Here's a list of items that we will be adding to optimize the Cassandra DataStore:

  * Pushdown predicates - push attribute predicates down into Cassandra rather than applying them in the post-processing
    phase
  * Configurable periodicity - utilizing one-week bounds as the coarse temporal part of the row key is not optimal for
    all data sets and workloads.  Make the coarse temporal part of the primary key configurable - i.e. day, year, etc
  * Configurable precision in the z3 clustering key - the full resolution z3 index results in a lot of range scans.
    We can limit the range scans by accommodating lower precision z3 indexes and pruning false positives in a
    post-processing step.
  * Prune false positives with push-down predicates - if we add the latitude and longitude as columns, we can prune
    false positives by having two predicates - the first specifying the range on z3 and the second specifying the bounds on x and y.
  * Non-point geometries - support linestrings and polygons