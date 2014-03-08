Shell Commands
==============

As of Accumulo version 1.6.x and greater, developers can
implement commands that extend the Accumulo shell.  GeoMesa
supports this functionality against Accumulo 1.5.x using
a custom branch of Accumulo 1.5.1 that can be found at
https://github.com/ccri/accumulo/tree/extensible_shell

To use, build the Accumulo branch and drop the jar in the 
Accumulo distribution's lib directory.  Drop the GeoMesa
runtime distributed jar in the Accumulo distribution's lib/ext
directory.
