/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.process.transform

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.arrow.ArrowProperties
import org.locationtech.geomesa.arrow.io.FormatVersion
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.process.ArrowVisitor
import org.locationtech.geomesa.process.GeoMesaProcess

@DescribeProcess(
  title = "Arrow Conversion",
  description = "Converts a feature collection to arrow format"
)
class ArrowConversionProcess extends GeoMesaProcess with LazyLogging {

  /**
    * Converts an input feature collection to arrow format
    *
    * @param features input features
    * @param dictionaryFields attributes to dictionary encode, optional
    * @return
    */
  @DescribeResult(description = "Encoded feature collection")
  def execute(
              @DescribeParameter(name = "features", description = "Input feature collection to encode")
              features: SimpleFeatureCollection,
              @DescribeParameter(name = "includeFids", description = "Include feature IDs in arrow file", min = 0)
              includeFids: java.lang.Boolean,
              @DescribeParameter(name = "proxyFids", description = "Proxy feature IDs to ints instead of strings", min = 0)
              proxyFids: java.lang.Boolean,
              @DescribeParameter(name = "formatVersion", description = "Arrow IPC format version", min = 0)
              formatVersion: String,
              @DescribeParameter(name = "dictionaryFields", description = "Attributes to dictionary encode", min = 0, max = 128, collectionType = classOf[String])
              dictionaryFields: java.util.List[String],
              @DescribeParameter(name = "sortField", description = "Attribute to sort by", min = 0)
              sortField: String,
              @DescribeParameter(name = "sortReverse", description = "Reverse the default sort order", min = 0)
              sortReverse: java.lang.Boolean,
              @DescribeParameter(name = "batchSize", description = "Number of features to include in each record batch", min = 0)
              batchSize: java.lang.Integer,
              @DescribeParameter(name = "flattenStruct", description = "Removes the outer SFT struct yielding top level feature access", min = 0)
              flattenStruct: java.lang.Boolean,
              @DescribeParameter(name = "flipAxisOrder", description = "Flip the axis order of returned coordinates from latitude/longitude (default) to longitude/latitude", min = 0, defaultValue = "false")
              flipAxisOrder: java.lang.Boolean = false
             ): java.util.Iterator[Array[Byte]] = {

    import scala.collection.JavaConverters._

    logger.debug(s"Running arrow encoding for ${features.getClass.getName}")

    val sft = features.getSchema

    // validate inputs
    val toEncode: Seq[String] = Option(dictionaryFields).map(_.asScala.toSeq).getOrElse(Seq.empty)
    toEncode.foreach { attribute =>
      if (sft.indexOf(attribute) == -1) {
        throw new IllegalArgumentException(s"Attribute $attribute doesn't exist in $sft")
      }
    }

    val encoding = SimpleFeatureEncoding.min(includeFids == null || includeFids, proxyFids != null && proxyFids, Option(flipAxisOrder).exists(_.booleanValue))
    val ipcVersion = Option(formatVersion).getOrElse(FormatVersion.ArrowFormatVersion.get)
    val reverse = Option(sortReverse).map(_.booleanValue())
    val batch = Option(batchSize).map(_.intValue).getOrElse(ArrowProperties.BatchSize.get.toInt)

    val visitor =
      new ArrowVisitor(sft, encoding, ipcVersion, toEncode, Option(sortField), reverse, false, batch, flattenStruct)
    GeoMesaFeatureCollection.visit(features, visitor)
    visitor.getResult.results
  }
}
