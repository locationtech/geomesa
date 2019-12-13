/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.transform

import java.util.Locale

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.feature.visitor._
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.process.{GeoMesaProcess, GeoMesaProcessVisitor}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.{BIN_ATTRIBUTE_INDEX, EncodingOptions}
import org.locationtech.geomesa.utils.bin.{AxisOrder, BinaryOutputEncoder}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.Feature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

@DescribeProcess(
  title = "Binary Conversion",
  description = "Converts a feature collection to binary format"
)
class BinConversionProcess extends GeoMesaProcess with LazyLogging {

  /**
    * Converts an input feature collection to BIN format
    *
    * @param features input features
    * @param geom geometry attribute to encode in the bin format, optional
    *             will use default geometry if not provided
    * @param dtg date attribute to encode in the bin format, optional
    *            will use default date if not provided
    * @param track track id attribute to encode in the bin format, optional
    *              will use feature id if not provided
    * @param label label attribute to encode in the bin format, optional
    * @param axisOrder axis order to encode points, optional
    * @return
    */
  @DescribeResult(description = "Encoded feature collection")
  def execute(
              @DescribeParameter(name = "features", description = "Input feature collection to query ")
              features: SimpleFeatureCollection,
              @DescribeParameter(name = "track", description = "Track field to use for BIN records", min = 0)
              track: String,
              @DescribeParameter(name = "geom", description = "Geometry field to use for BIN records", min = 0)
              geom: String,
              @DescribeParameter(name = "dtg", description = "Date field to use for BIN records", min = 0)
              dtg: String,
              @DescribeParameter(name = "label", description = "Label field to use for BIN records", min = 0)
              label: String,
              @DescribeParameter(name = "axisOrder", description = "Axis order - either latlon or lonlat", min = 0)
              axisOrder: String
             ): java.util.Iterator[Array[Byte]] = {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    logger.debug(s"Running BIN encoding for ${features.getClass.getName}")

    val sft = features.getSchema

    def indexOf(attribute: String): Int = {
      val i = sft.indexOf(attribute)
      if (i == -1) {
        throw new IllegalArgumentException(s"Attribute $attribute doesn't exist in ${sft.getTypeName} " +
            SimpleFeatureTypes.encodeType(sft))
      }
      i
    }

    val geomField  = Option(geom).map(indexOf)
    val dtgField   = Option(dtg).map(indexOf).orElse(sft.getDtgIndex)
    val trackField = Option(track).filter(_ != "id").map(indexOf)
    val labelField = Option(label).map(indexOf)

    val axis = Option(axisOrder).map {
      case o if o.toLowerCase(Locale.US) == "latlon" => AxisOrder.LatLon
      case o if o.toLowerCase(Locale.US) == "lonlat" => AxisOrder.LonLat
      case o => throw new IllegalArgumentException(s"Invalid axis order '$o'. Valid values are 'latlon' and 'lonlat'")
    }

    val visitor = new BinVisitor(sft, EncodingOptions(geomField, dtgField, trackField, labelField, axis))
    GeoMesaFeatureCollection.visit(features, visitor)
    visitor.getResult.results
  }
}

class BinVisitor(sft: SimpleFeatureType, options: EncodingOptions)
    extends GeoMesaProcessVisitor with LazyLogging {

  import scala.collection.JavaConversions._

  // for collecting results manually
  private val manualResults = scala.collection.mutable.Queue.empty[Array[Byte]]
  private val manualConversion = BinaryOutputEncoder(sft, options)

  private var result = new Iterator[Array[Byte]] {
    override def next(): Array[Byte] = manualResults.dequeue()
    override def hasNext: Boolean = manualResults.nonEmpty
  }

  override def getResult: BinResult = BinResult(result)

  // manually called for non-accumulo feature collections
  override def visit(feature: Feature): Unit =
    manualResults += manualConversion.encode(feature.asInstanceOf[SimpleFeature])

  override def execute(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug(s"Visiting source type: ${source.getClass.getName}")

    query.getHints.put(QueryHints.BIN_TRACK, options.trackIdField.map(sft.getDescriptor(_).getLocalName).getOrElse("id"))
    options.geomField.foreach(i => query.getHints.put(QueryHints.BIN_GEOM, sft.getDescriptor(i).getLocalName))
    options.dtgField.foreach(i => query.getHints.put(QueryHints.BIN_DTG, sft.getDescriptor(i).getLocalName))
    options.labelField.foreach(i => query.getHints.put(QueryHints.BIN_LABEL, sft.getDescriptor(i).getLocalName))

    val features = SelfClosingIterator(source.getFeatures(query))
    result ++= features.map(_.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]])
  }
}

case class BinResult(results: java.util.Iterator[Array[Byte]]) extends AbstractCalcResult {
  override def getValue: AnyRef = results
}
