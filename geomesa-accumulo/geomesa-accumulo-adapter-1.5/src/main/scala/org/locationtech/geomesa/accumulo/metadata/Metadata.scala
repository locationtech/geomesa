package org.locationtech.geomesa.accumulo.metadata

import org.apache.accumulo.core.Constants

object Metadata {
  val AccumuloMetadataTableName = Constants.METADATA_TABLE_NAME
  val AccumuloMetadataCF = Constants.METADATA_DATAFILE_COLUMN_FAMILY
  val EmptyAuths = Constants.NO_AUTHS
}
