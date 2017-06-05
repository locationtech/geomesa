/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.net.URL

import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.utils.geotools.GeneralShapefileIngest
import org.locationtech.geomesa.utils.text.TextTools
import org.locationtech.geomesa.utils.text.TextTools.getPlural

import scala.collection.parallel.ForkJoinTaskSupport

class ShapefileIngest(connection: java.util.Map[String, String],
                      typeName: Option[String],
                      files: Seq[String],
                      threads: Int) extends Runnable {

  override def run(): Unit = {
    Command.user.info(s"Ingesting ${getPlural(files.length, "file")} with ${getPlural(threads.toLong, "thread")}")

    val start = System.currentTimeMillis()

    // If someone is ingesting file from hdfs, S3, or wasb we add the Hadoop URL Factories to the JVM.
    if (files.exists(IngestCommand.isDistributedUrl)) {
      import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
      val factory = new FsUrlStreamHandlerFactory
      URL.setURLStreamHandlerFactory(factory)
    }

    val ds = DataStoreFinder.getDataStore(connection)

    val (ingested, failed) = try {
      val seq = if (threads > 1) {
        val parfiles = files.par
        parfiles.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(threads))
        parfiles
      } else {
        files
      }
      seq.map(GeneralShapefileIngest.ingestToDataStore(_, ds, typeName)).reduce(sum)
    } finally {
      ds.dispose()
    }

    Command.user.info(s"Shapefile ingestion complete in ${TextTools.getTime(start)}")
    Command.user.info(AbstractIngest.getStatInfo(ingested, failed))
  }

  private def sum(left: (Long, Long), right: (Long, Long)): (Long, Long) = (left._1 + right._1, left._2 + right._2)
}
