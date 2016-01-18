# GeoMesa Accumulo Distributed Runtime

The distributed runtime module produces a jar containing server-side code for Accumulo
that must be available on each of the Accumulo tablet servers in the cluster. This jar
contains GeoMesa code and Accumulo iterators required for querying GeoMesa.

The version of the distributed runtime jar must match the version of the GeoMesa data store client jar
(usually in GeoServer). If not, queries might not work correctly (or at all).

### Installation - Accumulo 1.5

To install the distributed runtime jar in Accumulo 1.5, simply copy the jar into the
`$ACCUMULO_HOME/lib/ext` folder *on each* of the tablet servers of the Accumulo cluster. 
Note that you do not need the jar on the Accumulo master server, and including it there may cause
classpath issues later.

### Installation - Accumulo 1.6+

In Accumulo 1.6, we can leverage namespaces to isolate the GeoMesa classpath from the rest of Accumulo.
First, you have to create the namespace, using the shell:

```
> createnamespace myNamespace
> grant NameSpace.CREATE_TABLE -ns myNamespace -u myUser
> config -s general.vfs.context.classpath.myNamespace=hdfs://NAME_NODE_FQDN:8020/accumulo/classpath/myNamespace/[^.].*.jar
> config -ns myNamespace -s table.classpath.context=myNamespace
```

Then copy the distributed runtime jar into hdfs under the path you specified. The path above is
just an example; you can include nested folders with project names, version numbers, etc in order
to have different versions of GeoMesa on the same Accumulo instance. You should remove any GeoMesa jars under
`$ACCUMULO_HOME/lib/ext` to prevent any classpath conflicts.

*NOTE*: When connecting to a data store, you must prefix the table name parameter with the namespace:
```"tableName" -> "myNamespace.my_catalog"```
