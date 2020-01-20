/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.File
import java.net.URL

import org.geotools.data.Transaction
import org.geotools.data.shapefile.files.ShpFiles
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.util.URLs
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class ShapefileExporter(file: File) extends FeatureExporter {

  import scala.collection.JavaConverters._

  private val url = URLs.fileToUrl(file)
  private var ds: ShapefileDataStore = _
  private var mappings: Map[Int, Int] = _

  override def start(sft: SimpleFeatureType): Unit = {
    // ensure that the parent directory exists, otherwise the data store will error out
    Option(file.getParentFile).filterNot(_.exists()).foreach(_.mkdirs())
    ds = new ShapefileDataStoreFactory().createDataStore(url).asInstanceOf[ShapefileDataStore]
    ds.createSchema(sft)

    var i = -1
    var j = 0
    // map attributes according to the shapefile data store
    // default geometry goes to attribute 0, other geometries are dropped
    // byte arrays are dropped, but everything else is kept or transformed
    // see: `org.geotools.data.shapefile.ShapefileDataStore.createDbaseHeader()`
    val attributes = sft.getAttributeDescriptors.asScala.flatMap { d =>
      i += 1
      val binding = d.getType.getBinding
      if (classOf[Geometry].isAssignableFrom(binding) || binding == classOf[Array[Byte]] ) { None } else {
        j += 1
        Some(i -> j)
      }
    }
    mappings = attributes.toMap + (sft.indexOf(sft.getGeometryDescriptor.getLocalName) -> 0)
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L

    WithClose(ds.getFeatureWriterAppend(Transaction.AUTO_COMMIT)) { writer =>
      features.foreach { feature =>
        val toWrite = writer.next()
        mappings.foreach { case (from, to) => toWrite.setAttribute(to, feature.getAttribute(from)) }
        // copy over the user data
        // note: shapefile doesn't support provided fid
        toWrite.getUserData.putAll(feature.getUserData)
        writer.write()
        count += 1L
      }
    }

    Some(count)
  }

  override def bytes: Long = {
    val files = new ShpFiles(url)
    try {
      var sum = 0L
      files.getFileNames.asScala.values.foreach { file =>
        sum += URLs.urlToFile(new URL(file)).length()
      }
      sum
    } finally {
      files.dispose()
    }
  }

  override def close(): Unit = Option(ds).foreach(_.dispose)
}
