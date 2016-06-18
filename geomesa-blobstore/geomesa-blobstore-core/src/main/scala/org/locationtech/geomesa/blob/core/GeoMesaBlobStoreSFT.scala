package org.locationtech.geomesa.blob.core

import org.locationtech.geomesa.utils.geotools.{SftBuilder, SimpleFeatureTypes}

object GeoMesaBlobStoreSFT {
  val BlobFeatureTypeName = "blob"
  val IdFieldName         = "storeId"
  val GeomFieldName       = "geom"
  val FilenameFieldName   = "filename"
  val DtgFieldName        = "dtg"
  val ThumbnailFieldName  = "thumbnail"

  // TODO: Add metadata hashmap?
  // TODO GEOMESA-1186 allow for configurable geometry types
  val sft = new SftBuilder()
    .stringType(FilenameFieldName)
    .stringType(IdFieldName, index = true)
    .geometry(GeomFieldName, default = true)
    .date(DtgFieldName, default = true)
    .stringType(ThumbnailFieldName)
    .userData(SimpleFeatureTypes.MIXED_GEOMETRIES, "true")
    .build(BlobFeatureTypeName)

}
