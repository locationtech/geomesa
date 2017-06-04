/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.blob.api

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
    .userData(SimpleFeatureTypes.Configs.MIXED_GEOMETRIES, "true")
    .build(BlobFeatureTypeName)

}
