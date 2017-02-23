/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.process

import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder.{EncodingOptions, GeometryAttribute}
import org.opengis.feature.simple.SimpleFeatureType

trait BinProcessOutput {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  /**
    * Configure BIN output. Should be called from any WPS process that uses BINs. Parameters
    * should be passed in directly from the WPS execute method, so they may be null, etc.
    *
    * @param sft simple feature type
    * @param features output feature collection that will be BIN encoded
    * @param bins whether to output BINs or not
    * @param binGeom optional geometry field for BIN records
    * @param binDtg optional date field for BIN records
    * @param binTrackId optional track field for BIN records
    * @param binLabel optional label field for BIN records
    */
  def configureOutput(sft: SimpleFeatureType,
                      features: SimpleFeatureCollection,
                      bins: java.lang.Boolean,
                      binGeom: String,
                      binDtg: String,
                      binTrackId: String,
                      binLabel: String): Unit = {
    if (bins == null || !bins) {
      BinaryOutputEncoder.CollectionEncodingOptions.remove(features.getID)
    } else {
      val geom  = Option(binGeom).map(GeometryAttribute.apply(_))
      val track = Option(binTrackId).orElse(sft.getBinTrackId)
      val label = Option(binLabel)
      val dtg   = Option(binDtg)

      // pass bin parameters off to the output format
      val options = EncodingOptions(geom, dtg, track, label)
      BinaryOutputEncoder.CollectionEncodingOptions.put(features.getID, options)
    }
  }
}
