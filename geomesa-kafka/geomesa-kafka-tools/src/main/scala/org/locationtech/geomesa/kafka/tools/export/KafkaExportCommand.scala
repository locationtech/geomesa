/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.export

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.beust.jcommander.{ParameterException, Parameters}
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator}
import org.geotools.data.{FeatureEvent, FeatureListener, Query}
import org.geotools.feature.collection.BaseSimpleFeatureCollection
import org.locationtech.geomesa.features.TransformSimpleFeature
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.tools.export.KafkaExportCommand._
import org.locationtech.geomesa.kafka.tools.{ConsumerDataStoreParams, KafkaDataStoreCommand}
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent.KafkaFeatureChanged
import org.locationtech.geomesa.tools.export.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.tools.export.formats.FeatureExporter
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

class KafkaExportCommand extends ExportCommand[KafkaDataStore] with KafkaDataStoreCommand {

  override val params = new KafkaExportParameters()

  private var max: Option[Int] = None
  private val queue: BlockingQueue[SimpleFeature] = new LinkedBlockingQueue[SimpleFeature]

  override protected def export(exporter: FeatureExporter, collection: SimpleFeatureCollection): Option[Long] = {
    val features = CloseableIterator(collection.features())
    val count = try {
      max match {
        case None => exportContinuously(exporter, features)
        case Some(m) => exportWithMax(exporter, features, m)
      }
    } finally {
      features.close()
    }
    Some(count)
  }

  private def exportContinuously(exporter: FeatureExporter, features: Iterator[SimpleFeature]): Long = {
    // try to close the exporter when user cancels to finish off whatever the export was
    sys.addShutdownHook(exporter.close())
    var count = 0L
    while (true) {
      // hasNext may return false one time, and then true the next if more data is read from kafka
      if (features.hasNext) {
        exporter.export(features).foreach(count += _)
      } else {
        Thread.sleep(1000)
      }
    }
    count
  }

  private def exportWithMax(exporter: FeatureExporter, features: Iterator[SimpleFeature], max: Int): Long = {
    // noinspection LoopVariableNotUpdated
    var count = 0L
    while (count < max) {
      // hasNext may return false one time, and then true the next if more data is read from kafka
      if (features.hasNext) {
        // note: side effect in map - do count here in case exporter doesn't report counts
        val batch = features.take(max - count.toInt).map { f => count += 1; f }
        exporter.export(batch)
      } else {
        Thread.sleep(1000)
      }
    }
    count
  }

  override protected def getFeatures(ds: KafkaDataStore, query: Query): SimpleFeatureCollection = {
    val sft = ds.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Type ${params.featureName} does not exist at path ${params.zkPath}")
    }

    max = Option(query.getMaxFeatures).filter(_ != Int.MaxValue)

    val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE)
    val transform = Option(query.getPropertyNames).map(QueryPlanner.buildTransformSFT(sft, _))

    val listener = new ExportFeatureListener(sft, filter, transform, queue)

    val fs = ds.getFeatureSource(query.getTypeName)
    fs.addFeatureListener(listener)

    Command.user.info(s"Exporting from kafka topic '${sft.getUserData.get(KafkaDataStore.TopicKey)}' " +
        "- use `ctrl-c` to stop")

    new BaseSimpleFeatureCollection(transform.map(_._2).getOrElse(sft)) {
      override def features(): SimpleFeatureIterator = new SimpleFeatureIterator {
        private var current: SimpleFeature = _

        // may return false one time, and then true the next if more data is read from kafka
        override def hasNext: Boolean = {
          if (current == null) {
            current = queue.poll(100, TimeUnit.MILLISECONDS)
          }
          current != null
        }
        override def next(): SimpleFeature = {
          val res = current
          current = null
          res
        }
        override def close(): Unit = fs.removeFeatureListener(listener)
      }
    }
  }
}

object KafkaExportCommand {

  @Parameters(commandDescription = "Export features from a GeoMesa Kafka topic")
  class KafkaExportParameters extends ConsumerDataStoreParams with RequiredTypeNameParam with ExportParams

  class ExportFeatureListener(sft: SimpleFeatureType,
                              filter: Option[Filter],
                              transform: Option[(String, SimpleFeatureType)],
                              queue: BlockingQueue[SimpleFeature]) extends FeatureListener {

    private val attributes = transform.map { case (tdefs, tsft) =>
      (tsft, TransformSimpleFeature.attributes(sft, tsft, tdefs))
    }

    override def changed(event: FeatureEvent): Unit = {
      event match {
        case e: KafkaFeatureChanged => added(e.feature)
        case _ => // no-op
      }
    }

    def added(sf: SimpleFeature): Unit = {
      if (filter.forall(_.evaluate(sf))) {
        queue.put(attributes.map { case (tsft, a) => new TransformSimpleFeature(tsft, a, sf) }.getOrElse(sf))
      }
    }
  }
}

