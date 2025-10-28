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
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection
import org.locationtech.geomesa.index.process.BinVisitor
import org.locationtech.geomesa.process.GeoMesaProcess
import org.locationtech.geomesa.utils.bin.AxisOrder
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import java.util.Locale

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
