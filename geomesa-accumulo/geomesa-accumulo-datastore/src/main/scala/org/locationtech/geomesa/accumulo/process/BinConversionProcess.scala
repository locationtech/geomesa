/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.process

import java.util.Locale

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.feature.visitor._
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.process.vector.VectorProcess
import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator
import org.locationtech.geomesa.filter.ff
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder.{EncodingOptions, GeometryAttribute}
import org.locationtech.geomesa.filter.function.{AxisOrder, BinaryOutputEncoder}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.Feature
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.Expression

@DescribeProcess(
  title = "Binary Conversion",
  description = "Converts a feature collection to binary format"
)
class BinConversionProcess extends VectorProcess with LazyLogging {

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

    val geomField  = {
      val name = Option(geom).getOrElse(sft.getGeomField)
      val axis = Option(axisOrder).map {
        case o if o.toLowerCase(Locale.US) == "latlon" => AxisOrder.LatLon
        case o if o.toLowerCase(Locale.US) == "lonlat" => AxisOrder.LonLat
        case o => throw new IllegalArgumentException(s"Invalid axis order '$o'. Valid values are 'latlon' and 'lonlat'")
      }
      Some(GeometryAttribute(name, axis.getOrElse(AxisOrder.LonLat))) // note: wps seems to always be lon first
    }
    val dtgField   = Option(dtg).orElse(sft.getDtgField)
    val trackField = Option(track).orElse(sft.getBinTrackId)
    val labelField = Option(label)

    // validate inputs
    (geomField.map(_.geom).toSeq ++ dtgField ++ trackField ++ labelField).foreach { attribute =>
      if (attribute != "id" && sft.indexOf(attribute) == -1) {
        throw new IllegalArgumentException(s"Attribute $attribute doesn't exist in $sft")
      }
    }

    val visitor = new BinVisitor(sft, EncodingOptions(geomField, dtgField, trackField, labelField))
    features.accepts(visitor, null)
    visitor.getResult.results
  }
}

class BinVisitor(sft: SimpleFeatureType, options: EncodingOptions)
    extends FeatureCalc with FeatureAttributeVisitor with LazyLogging {

  import scala.collection.JavaConversions._

  // for collecting results manually
  private val manualResults = scala.collection.mutable.Queue.empty[Array[Byte]]
  private val manualConversion = BinaryOutputEncoder.encodeFeatures(sft, options)

  private var result = new Iterator[Array[Byte]] {
    override def next(): Array[Byte] = manualResults.dequeue()
    override def hasNext: Boolean = manualResults.nonEmpty
  }

  override def getResult: BinResult = BinResult(result)

  // manually called for non-accumulo feature collections
  override def visit(feature: Feature): Unit =
    manualResults += manualConversion.apply(feature.asInstanceOf[SimpleFeature])

  // allows us to accept visitors from retyping feature collections
  override def getExpressions: java.util.List[Expression] = {
    val geom = options.geomField.toSeq.map { case GeometryAttribute(g, _) => g }
    (geom ++ options.dtgField ++ options.trackIdField ++ options.labelField).collect {
      case a if a != "id" => ff.property(a)
    }
  }

  /**
    * Optimized method to run distributed query. Sets the result, available from `getResult`
    *
    * @param source simple feature source
    * @param query may contain additional filters to apply
    */
  def binQuery(source: SimpleFeatureSource, query: Query): Unit = {
    logger.debug(s"Visiting source type: ${source.getClass.getName}")

    query.getHints.put(QueryHints.BIN_TRACK, options.trackIdField.getOrElse("id"))
    options.geomField.foreach { case GeometryAttribute(g, _) => query.getHints.put(QueryHints.BIN_GEOM, g) }
    options.dtgField.foreach(query.getHints.put(QueryHints.BIN_DTG, _))
    options.labelField.foreach(query.getHints.put(QueryHints.BIN_LABEL, _))

    val features = SelfClosingIterator(source.getFeatures(query))
    result = result ++
        features.map(_.getAttribute(BinAggregatingIterator.BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]])
  }
}

case class BinResult(results: java.util.Iterator[Array[Byte]]) extends AbstractCalcResult {
  override def getValue: AnyRef = results
}
