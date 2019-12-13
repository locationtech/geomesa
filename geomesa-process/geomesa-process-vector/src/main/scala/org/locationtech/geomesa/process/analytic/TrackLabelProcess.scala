/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import java.util.Date

import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.locationtech.geomesa.process.GeoMesaProcess
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.util.ProgressListener

/**
 * Returns a single feature that is the head of a track of related simple features
 */
@DescribeProcess(
  title = "Track Label Process",
  description = "Returns a single feature appropriate for labelling a track of features"
)
class TrackLabelProcess extends GeoMesaProcess {

  @throws(classOf[ProcessException])
  @DescribeResult(name = "result", description = "Label features")
  def execute(@DescribeParameter(name = "data", description = "Input features")
              featureCollection: SimpleFeatureCollection,
              @DescribeParameter(name = "track", description = "Track attribute to use for grouping features")
              track: String,
              @DescribeParameter(name = "dtg", description = "Date attribute to use for ordering tracks", min = 0)
              dtg: String,
              monitor: ProgressListener): SimpleFeatureCollection = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val sft = featureCollection.getSchema
    lazy val sftString = s"${sft.getTypeName}: ${SimpleFeatureTypes.encodeType(sft)}"

    val trackField = Option(track).map(sft.indexOf).filter(_ != -1).getOrElse {
      throw new IllegalArgumentException(s"Invalid track field $track for schema $sftString")
    }
    val dtgField = Option(dtg).map(sft.indexOf).orElse(sft.getDtgIndex)
    // noinspection ExistsEquals
    if (dtgField.exists(_ == -1)) {
      throw new IllegalArgumentException(s"Invalid track field $track for schema $sftString")
    }

    val results = new ListFeatureCollection(sft)

    val grouped = SelfClosingIterator(featureCollection).toSeq.groupBy(_.getAttribute(trackField))

    dtgField match {
      case None    => grouped.foreach { case (_, features) => results.add(features.head) }
      case Some(d) => grouped.foreach { case (_, features) => results.add(features.maxBy(_.getAttribute(d).asInstanceOf[Date])) }
    }

    results
  }
}
