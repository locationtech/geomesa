.. _converter_caches:

Enrichment Caches
-----------------

A cache can be used to populate attributes based on the input data. This can be useful for enriching records with static
information from an external source. For example, a user ID could be used to lookup an email address that isn't present in the
input data.

Caches are defined under the key ``caches`` in the converter definition. Each cache is a named object.

Caches are accessed through the ``cacheLookup`` transform function, which takes three arguments - the name of the cache (i.e.
its key in the ``caches`` definition), an identifier used to retrieve a cached record, and a field to retrieve from the cached
record.

Static Caches
^^^^^^^^^^^^^

A simple, static cache can be defined in the converter definition itself. This may be appropriate for smaller use cases, or for
testing.

Static caches are defined with the following keys:

=============== ======== ======= =================================================================================================
Key             Required Type    Description
=============== ======== ======= =================================================================================================
``type``        yes      String  Must be the string ``simple``
``data``        yes      Object  Each key in the ``data`` object is a cached record.
=============== ======== ======= =================================================================================================

For example, the following definition shows a cache of user information::

    caches = {
      users = {
        type = "simple"
        data = {
          123 = {
            name = "Jane"
            email = "jane@example.com"
          }
          456 = {
            name = "Mary"
            email = "mary@example.com"
          }
        }
      }
    }

Redis Caches
^^^^^^^^^^^^

Redis can be used for a more robust cache. A Redis cache is defined with the following keys:

=============== ======== ======= =================================================================================================
Key             Required Type    Description
=============== ======== ======= =================================================================================================
``type``        yes      String  Must be the string ``redis``
``redis-url``   yes      String  URL for a Redis instance
``expiration``  no       Integer If defined, values will be cached locally for the given period. Specified as milliseconds.
=============== ======== ======= =================================================================================================

The Redis values are expected to be a hash. For example, the following Redis CLI commands would create the same user cache defined
in the static cache example, above::

    > HSET 123 name "Jane" email "jane@example.com"
    > HSET 456 name "Mary" email "mary@example.com"

The cache would be defined as::

    caches = {
      users = {
        type = "redis"
        redis-url = "redis://localhost:6379"
        expiration = 30000
      }
    }

Example
^^^^^^^

In the following example, we use the ``cacheLookup`` function to resolve the email of a user based on the ``userId`` in each
record::

    geomesa.converters.myconverter = {
      fields = [
        { name = "userId", ... }
        { name = "email", transform = "cacheLookup('users', $userId, 'email')" }
      ]
      caches = {
        users = {
          type = "redis"
          redis-url = "redis://localhost:6379"
          expiration = 30000
        }
      }
    }
