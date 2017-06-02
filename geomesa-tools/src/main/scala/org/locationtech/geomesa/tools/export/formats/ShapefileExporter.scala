/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.File

import org.geotools.data.DataUtilities
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureStore}
import org.opengis.feature.simple.SimpleFeatureType

class ShapefileExporter(file: File) extends FeatureExporter {

  override def export(features: SimpleFeatureCollection): Option[Long] = {
    val url = DataUtilities.fileToURL(file)
    val factory = new ShapefileDataStoreFactory()
    val newShapeFile = factory.createDataStore(url).asInstanceOf[ShapefileDataStore]

    newShapeFile.createSchema(features.getSchema)
    val store = newShapeFile.getFeatureSource.asInstanceOf[SimpleFeatureStore]
    store.addFeatures(features)
    None
  }

  override def close(): Unit = {}

}

object ShapefileExporter {

  // When exporting to Shapefile, we must rename the Geometry Attribute Descriptor to "the_geom", per
  // the requirements of Geotools' ShapefileDataStore and ShapefileFeatureWriter. The easiest way to do this
  // is transform the attribute when retrieving the SimpleFeatureCollection.
  def modifySchema(sft: SimpleFeatureType): Seq[String] = {
    import scala.collection.JavaConversions._
    replaceGeom(sft, sft.getAttributeDescriptors.map(_.getLocalName))
  }

  /**
    * If the attribute string has the geometry attribute in it, we will replace the name of the
    * geom descriptor with "the_geom," since that is what Shapefile expect the geom to be named.
    *
    * @param attributes attributes
    * @param sft simple feature type
    * @return
    */
  def replaceGeom(sft: SimpleFeatureType, attributes: Seq[String]): Seq[String] = {
    if (attributes.exists(_.startsWith("the_geom"))) { attributes } else {
      val geom = Option(sft.getGeometryDescriptor).map(_.getLocalName).orNull
      val index = attributes.indexOf(geom)
      if (index == -1) {
        attributes :+ s"the_geom=$geom"
      } else {
        attributes.updated(index, s"the_geom=$geom")
      }
    }
  }
}
