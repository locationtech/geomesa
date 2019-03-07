/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io._
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Locale}

import org.geotools.data.memory.MemoryDataStore
import org.geotools.data.{DataStore, DataUtilities, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.tools.DataStoreRegistration
import org.locationtech.geomesa.tools.export.formats.ExportFormats.ExportFormat
import org.locationtech.geomesa.tools.export.formats.{DelimitedExporter, ExportFormats}
import org.locationtech.geomesa.tools.ingest.IngestCommand
import org.locationtech.geomesa.tools.ingest.IngestCommand.IngestParams
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DelimitedExportImportTest extends Specification {

  private val counter = new AtomicInteger(0)

  def withCommand[T](ds: DataStore)(op: IngestCommand[DataStore] => T): T = {
    val key = s"${getClass.getName}:${counter.getAndIncrement()}"
    DataStoreRegistration.register(key, ds)

    val command: IngestCommand[DataStore] = new IngestCommand[DataStore]() {
      override val params: IngestParams = new IngestParams(){}
      override def libjarsFile: String = ""
      override def libjarsPaths: Iterator[() => Seq[File]] = Iterator.empty
      override def connection: Map[String, String] = Map(DataStoreRegistration.param.key -> key)
    }
    command.params.force = true

    try {
      op(command)
    } finally {
      DataStoreRegistration.unregister(key, ds)
    }
  }

  def export(sft: SimpleFeatureType, features: Iterator[SimpleFeature], format: ExportFormat): String = {
    val writer = new StringWriter()
    // exclude feature ID since the inferred ingestion just treats it as another column
    val export = format match {
      case ExportFormats.Csv => DelimitedExporter.csv(writer, withHeader = true, includeIds = false)
      case ExportFormats.Tsv => DelimitedExporter.tsv(writer, withHeader = true, includeIds = false)
    }
    export.start(sft)
    export.export(features)
    export.close()
    writer.toString
  }

  "Delimited export import" should {

    "export and import simple schemas" >> {

      val sft = SimpleFeatureTypes.createType("tools", "name:String,dtg:Date,*geom:Point:srid=4326")
      val features = List(
        ScalaSimpleFeature.create(sft, "id1", "name1", "2016-01-01T00:00:00.000Z", "POINT(1 0)"),
        ScalaSimpleFeature.create(sft, "id2", "name2", "2016-01-02T00:00:00.000Z", "POINT(0 2)")
      )

      foreach(Seq(ExportFormats.Tsv, ExportFormats.Csv)) { format =>
        val path = Files.createTempFile(getClass.getSimpleName, "." + format.toString.toLowerCase(Locale.US))
        val file = new File(path.toAbsolutePath.toString)
        try {
          WithClose(new PrintWriter(file)) { writer =>
            writer.println(export(sft, features.iterator, format))
          }

          val ds = new MemoryDataStore() {
            override def dispose(): Unit = {} // prevent dispose from deleting our data
          }

          withCommand(ds) { command =>
            command.params.featureName = "tools"
            command.params.files = Collections.singletonList(file.getAbsolutePath)
            command.execute()
          }

          ds.getTypeNames mustEqual Array("tools")
          val ingested = SelfClosingIterator(ds.getFeatureReader(new Query("tools"), Transaction.AUTO_COMMIT)).toList
          ingested.map(DataUtilities.encodeFeature(_, false)) mustEqual features.map(DataUtilities.encodeFeature(_, false))
        } finally {
          if (!file.delete()) {
            file.deleteOnExit()
          }
        }
      }
    }
  }
}
