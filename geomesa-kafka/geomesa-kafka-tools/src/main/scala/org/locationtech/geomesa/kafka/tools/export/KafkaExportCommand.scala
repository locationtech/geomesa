/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.tools.export

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.beust.jcommander.{ParameterException, Parameters}
import org.geotools.data.{FeatureEvent, FeatureListener, Query}
import org.locationtech.geomesa.features.TransformSimpleFeature
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.tools.ConsumerDataStoreParams
import org.locationtech.geomesa.kafka.tools.KafkaDataStoreCommand.KafkaDistributedCommand
import org.locationtech.geomesa.kafka.tools.export.KafkaExportCommand._
import org.locationtech.geomesa.kafka.utils.KafkaFeatureEvent.KafkaFeatureChanged
import org.locationtech.geomesa.tools.export.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.tools.export.formats.FeatureExporter
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.geotools.Transform.Transforms
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

class KafkaExportCommand extends ExportCommand[KafkaDataStore] with KafkaDistributedCommand {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  override val params = new KafkaExportParameters()

  private val queue: BlockingQueue[SimpleFeature] = new LinkedBlockingQueue[SimpleFeature]

  override protected def export(ds: KafkaDataStore, query: Query, exporter: FeatureExporter): Option[Long] = {
    val sft = ds.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Type ${params.featureName} does not exist at path ${params.zkPath}")
    }

    val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE)
    val transform = query.getHints.getTransform

    val listener = new ExportFeatureListener(sft, filter, transform, queue)

    Command.user.info(s"Exporting from kafka topic '${sft.getUserData.get(KafkaDataStore.TopicKey)}' " +
        "- use `ctrl-c` to stop")

    val features: Iterator[SimpleFeature] = new Iterator[SimpleFeature] {

      private var current: SimpleFeature = _

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
    }

    val fs = ds.getFeatureSource(query.getTypeName)
    fs.addFeatureListener(listener)

    try {
      exporter.start(query.getHints.getReturnSft)
      query.getHints.getMaxFeatures match {
        case None    => exportContinuously(exporter, features)
        case Some(m) => exportWithMax(exporter, features, m)
      }
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException("Could not execute export query. Please ensure that all arguments are correct", e)
    } finally {
      fs.removeFeatureListener(listener)
    }
  }

  private def exportContinuously(exporter: FeatureExporter, features: Iterator[SimpleFeature]): Option[Long] = {
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
    Some(count)
  }

  private def exportWithMax(exporter: FeatureExporter, features: Iterator[SimpleFeature], max: Int): Option[Long] = {
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
    Some(count)
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
      (tsft, Transforms(sft, tdefs).toArray)
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

