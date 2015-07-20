package org.locationtech.geomesa.accumulo.metadata

import org.apache.accumulo.core.metadata.MetadataTable
import org.apache.accumulo.core.metadata.schema.MetadataSchema
import org.apache.accumulo.core.security.Authorizations

object Metadata {
   val AccumuloMetadataTableName = MetadataTable.NAME
   val AccumuloMetadataCF = MetadataSchema.TabletsSection.DataFileColumnFamily.NAME
   val EmptyAuths = Authorizations.EMPTY
 }
