JoinProcess
^^^^^^^^^^^

The ``JoinProcess`` merges features from two different schemas using a common attribute field. It accepts the following
parameters:

=============  ===========
Parameter      Description
=============  ===========
primary        Primary feature collection being queried
secondary      Secondary feature collection to be joined
joinAttribute  Attribute field to join on
joinFilter     Additional filter to apply to joined features
attributes     Attributes to return. Attribute names should be qualified with the schema name, e.g. foo.bar
=============  ===========
