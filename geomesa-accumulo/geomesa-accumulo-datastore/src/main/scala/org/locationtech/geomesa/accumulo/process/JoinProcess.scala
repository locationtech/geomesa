/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.process

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.geotools.process.vector.VectorProcess
import org.geotools.util.NullProgressListener
import org.locationtech.geomesa.accumulo.process.query.{QueryResult, QueryVisitor}
import org.locationtech.geomesa.accumulo.process.unique.UniqueProcess
import org.locationtech.geomesa.accumulo.util.SelfClosingIterator
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.util.ProgressListener

/**
  * Returns features from a feature type based on a join against a second feature type.
  */
@DescribeProcess(
  title = "Join Process",
  description = "Queries a feature type based on attributes from a second feature type"
)
class JoinProcess extends VectorProcess with LazyLogging {

  // TODO this is currently aimed at a 1.2.2 backport - instead of the UniqueProcess we could run a stats job
  /**
    * @param unique result of running UniqueProcess
    * @param join joined data set from which return features will be taken
    * @param attribute attribute to join on
    * @param bins flag to return results in BIN format
    * @param binDtg date field for bin records - will use default date if not specified
    * @param binTrackId track ID field for bin records - will not include trackId if not specified
    * @param binLabel label field for bin record (optional)
    * @param monitor listener to monitor progress
    * @throws org.geotools.process.ProcessException if something goes wrong
    * @return
    */
  @throws(classOf[ProcessException])
  @DescribeResult(name = "result", description = "Output features")
  def execute(@DescribeParameter(name = "unique", description = "Result of unique process for joined attribute")
              unique: SimpleFeatureCollection,
              @DescribeParameter(name = "join", description = "Joined features")
              join: SimpleFeatureCollection,
              @DescribeParameter(name = "attribute", description = "Attribute field to join on", min = 1)
              attribute: String,
              @DescribeParameter(name = "bins", description = "Return BIN records or regular records", min = 0)
              bins: java.lang.Boolean,
              @DescribeParameter(name = "binDtg", description = "Date field to use for BIN records", min = 0)
              binDtg: String,
              @DescribeParameter(name = "binTrackId", description = "Track field to use for BIN records", min = 0)
              binTrackId: String,
              @DescribeParameter(name = "binLabel", description = "Label field to use for BIN records", min = 0)
              binLabel: String,
              monitor: ProgressListener): SimpleFeatureCollection = {

    import org.locationtech.geomesa.filter.ff

    import scala.collection.JavaConversions._

    logger.trace(s"Attempting join query on ${join.getClass.getName}")

    if (join.isInstanceOf[ReTypingFeatureCollection]) {
      logger.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }

    val descriptor = join.getSchema.getDescriptor(attribute)
    require(descriptor != null, s"Attribute $attribute does not exist in the joined feature collection")

    require(unique.getSchema.getTypeName == UniqueProcess.SftName &&
        unique.getSchema.getDescriptor(0).getLocalName == UniqueProcess.AttributeValue,
      s"Input feature collection is not the result of a unique process. Expected " +
          s"${SimpleFeatureTypes.encodeType(UniqueProcess.createUniqueSft(descriptor.getType.getBinding, histogram = false))} " +
          s"but got ${SimpleFeatureTypes.encodeType(unique.getSchema)}"
    )

    val attributeProperty = ff.property(attribute)
    val joinFilters = SelfClosingIterator(unique).map { sf =>
      ff.equals(attributeProperty, ff.literal(sf.getAttribute(0)))
    }

    val binOptions = if (bins != null && bins) {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
      val sft = join.getSchema
      val dtg = Option(binDtg).orElse(sft.getDtgField).getOrElse {
        throw new IllegalArgumentException("Please specify binDtg for BIN output format")
      }
      val track = Option(binTrackId).orElse(sft.getBinTrackId)
      Some(EncodingOptions(dtg, track, Option(binLabel)))
    } else {
      None
    }

    val result = if (joinFilters.isEmpty) {
      new DefaultFeatureCollection(null, join.getSchema)
    } else {
      val filter = ff.or(joinFilters.toList)
      val visitor = new QueryVisitor(join, filter)
      join.accepts(visitor, new NullProgressListener)
      visitor.getResult.asInstanceOf[QueryResult].results
    }

    // pass bin parameters off to the output format
    binOptions match {
      case None    => BinaryOutputEncoder.CollectionEncodingOptions.remove(result.getID)
      case Some(o) => BinaryOutputEncoder.CollectionEncodingOptions.put(result.getID, o)
    }

    result
  }
}
