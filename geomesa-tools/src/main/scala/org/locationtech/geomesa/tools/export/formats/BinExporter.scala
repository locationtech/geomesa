/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.OutputStream

import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.factory.Hints
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder.{EncodingOptions, GeometryAttribute}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.simple.SimpleFeatureType

class BinExporter(hints: Hints, os: OutputStream) extends FeatureExporter {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  override def export(fc: SimpleFeatureCollection): Option[Long] = {
    val sft = fc.getSchema
    val features = SelfClosingIterator(fc.features())
    if (sft == BinaryOutputEncoder.BinEncodedSft) {
      var numBytes = 0L
      // just copy bytes directly out
      features.foreach { f =>
        val bytes = f.getAttribute(0).asInstanceOf[Array[Byte]]
        os.write(bytes)
        numBytes += bytes.length
      }
      Some(numBytes / (if (hints.getBinLabelField.isEmpty) { 16 } else { 24 }))
    } else {
      import org.locationtech.geomesa.index.conf.QueryHints.RichHints
      // do the encoding here
      val geom = hints.getBinGeomField.map(GeometryAttribute(_))
      val options = EncodingOptions(geom, hints.getBinDtgField, Option(hints.getBinTrackIdField), hints.getBinLabelField)
      val count = BinaryOutputEncoder.encodeFeatureCollection(fc, os, options)
      Some(count)
    }
  }

  override def close(): Unit = os.close()
}

object BinExporter {

  def getAttributeList(sft: SimpleFeatureType, hints: Hints): Seq[String] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val geom = hints.getBinGeomField.orElse(Option(sft.getGeomField))
    val dtg = hints.getBinDtgField.orElse(sft.getDtgField)
    (Seq(hints.getBinTrackIdField) ++ geom ++ dtg ++ hints.getBinLabelField).filter(_ != "id")
  }
}
