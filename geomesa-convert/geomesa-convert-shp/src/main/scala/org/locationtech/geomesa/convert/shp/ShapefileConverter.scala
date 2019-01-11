/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.shp

import java.io.{File, InputStream}
import java.net.URL
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.{DataStoreFinder, FeatureReader, Query}
import org.locationtech.geomesa.convert.shp.ShapefileConverter.ShapefileIterator
import org.locationtech.geomesa.convert.{Counter, EnrichmentCache, EvaluationContext}
import org.locationtech.geomesa.convert2.AbstractConverter
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicField, BasicOptions}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326
import org.locationtech.geomesa.utils.io.{CloseWithLogging, PathUtils}
import org.locationtech.geomesa.utils.text.TextTools
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class ShapefileConverter(targetSft: SimpleFeatureType, config: BasicConfig, fields: Seq[BasicField], options: BasicOptions)
    extends AbstractConverter(targetSft, config, fields, options)  {

  import org.locationtech.geomesa.convert.shp.ShapefileFunctionFactory.{InputSchemaKey, InputValuesKey}

  override def createEvaluationContext(globalParams: Map[String, Any],
                                       caches: Map[String, EnrichmentCache],
                                       counter: Counter): EvaluationContext = {
    // inject placeholders for shapefile attributes into the evaluation context
    // used for accessing shapefile properties by name in ShapefileFunctionFactory
    val shpParams = Map(InputSchemaKey -> Array.empty[String], InputValuesKey -> Array.empty[Any])
    super.createEvaluationContext(globalParams ++ shpParams, caches, counter)
  }

  override protected def read(is: InputStream, ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    CloseWithLogging(is) // we don't use the input stream, just close it

    val path = ec.getInputFilePath.getOrElse {
      throw new IllegalArgumentException(s"Shapefile converter requires '${EvaluationContext.InputFilePathKey}' " +
          "to be set in the evaluation context")
    }
    val ds = ShapefileConverter.getDataStore(path)
    val schema = ds.getSchema()
    val array = Array.ofDim[Any](schema.getAttributeCount + 1)
    val names = Array.tabulate(schema.getAttributeCount)(i => schema.getDescriptor(i).getLocalName)

    val i = ec.indexOf(InputSchemaKey)
    val j = ec.indexOf(InputValuesKey)

    if (i == -1 || j == -1) {
      logger.warn("Input schema not found in evaluation context, shapefile functions " +
          s"${TextTools.wordList(new ShapefileFunctionFactory().functions.map(_.names.head))} will not be available")
    } else {
      ec.set(i, names)
      ec.set(j, array)
    }

    new ShapefileIterator(ds, array, ec.counter)
  }
}

object ShapefileConverter {

  private val factorySet = new AtomicBoolean(false)

  /**
    * Creates a URL, needed for the shapefile data store
    *
    * @param path input path
    * @return
    */
  def getDataStore(path: String): ShapefileDataStore = {
    val url = PathUtils.getUrl(path)
    val params = Collections.singletonMap(ShapefileDataStoreFactory.URLP.key, url)
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[ShapefileDataStore]
    if (ds == null) {
      throw new IllegalArgumentException(s"Could not read shapefile using path '$path'")
    }
    ds
  }

  /**
    * Iterator for reading a shapefile
    *
    * @param ds data store to read from
    * @param array attributes array
    * @param counter line counter
    */
  class ShapefileIterator(ds: ShapefileDataStore, array: Array[Any], counter: Counter)
      extends CloseableIterator[Array[Any]] with LazyLogging {

    val q = new Query
    // Only ask to reproject if the Shapefile has a CRS
    if (ds.getSchema.getCoordinateReferenceSystem != null) {
      q.setCoordinateSystemReproject(CRS_EPSG_4326)
    } else {
      logger.warn(s"Shapefile does not have CRS info.")
    }
    private val features: FeatureReader[SimpleFeatureType, SimpleFeature] =
      ds.getFeatureSource.getReader(q)

    override def hasNext: Boolean = features.hasNext

    override def next(): Array[Any] = {
      counter.incLineCount()

      val feature = features.next
      var i = 1
      while (i < array.length) {
        array(i) = feature.getAttribute(i - 1)
        i += 1
      }
      array(0) = feature.getID
      array
    }

    override def close(): Unit = {
      CloseWithLogging(features)
      ds.dispose()
    }
  }
}
