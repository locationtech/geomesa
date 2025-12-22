ArrowConversionProcess
^^^^^^^^^^^^^^^^^^^^^^

The ``ArrowConversionProcess`` converts an input feature collection to an `Apache Arrow <https://arrow.apache.org/>`_ stream. It
accepts the following parameters:

=====================  ===========
Parameter              Description
=====================  ===========
features               Input feature collection to encode
includeFids            Include feature IDs in arrow file
proxyFids              Proxy feature IDs to integers instead of strings
formatVersion          Arrow IPC format version
dictionaryFields       Attributes to dictionary encode
sortField              Attribute to sort by
sortReverse            Reverse the default sort order
batchSize              Number of features to include in each record batch
flipAxisOrder          Flip the axis order of returned coordinates from latitude first (default) to longitude first
=====================  ===========
