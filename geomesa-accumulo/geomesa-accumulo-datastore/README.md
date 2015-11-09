# GeoMesa Accumulo
==================


### Building Instructions

If you wish to build this project separately, you can with mvn clean install

    geomesa/geomesa-accumulo> mvn clean install

This will produce jars in target/ which are intended to be used by Accumulo tablet servers to provide
functionality needed for the rest of GeoMesa.

### Iterator Stack

The accumulo iterator stack is the where most of our query processing is done. Because every single feature
we return from a query has to go through the iterator stack, it's akin to a 'tight inner loop' - e.g. we want
to optimize it as much as possible. To improve execution speed, we don't use the common scala conventions
such as Option or foreach in the stack. Instead, we use null values and while loops, which should be more
performant.

In order to optimize our processing, each scan to accumulo ideally will use a single iterator that will
perform all the required filtering and transformation. In order to allow re-use across different iterators,
common functionality has been broken out into a series of traits and functions that can be mixed in to
provide the required mix of functionality.

Iterator features are contained in `org.locationtech.geomesa.core.iterators.IteratorExtensions`. In order
to simplify initialization, they each have an overloaded abstract init method. This allows an iterator
implementation to call a single init, which will chain to all the mixed in traits. The different features are
exposed as vars that get initialized during the iterator initialization, but they shouldn't change (or be
changed) after initialization.

Iterator functions are contained in `org.locationtech.geomesa.core.iterators.IteratorFunctions`. In order
to avoid unnecessary checks each time through the loop, we create a function for each path our execution
could take. In general, this means a separate function for each combination of filtering, transforming and
uniqueness. During the iterator initialization, the appropriate function will be selected based on the query.
