/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.hbase.tools

import java.io.File

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.locationtech.geomesa.fs.FileSystemDataStore
import org.locationtech.geomesa.fs.hbase.tools.BulkCopyCommand.BulkCopyParams
import org.locationtech.geomesa.fs.hbase.tools.FsHBaseDataStoreCommand.FsHBaseParams
import org.locationtech.geomesa.fs.tools.ingest.TempDirParam
import org.locationtech.geomesa.hbase.data.HBaseDataStore
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex
import org.locationtech.geomesa.tools.ingest.AbstractIngest.PrintProgress
import org.locationtech.geomesa.tools.{Command, OptionalCqlFilterParam, RequiredIndexParam, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.text.TextTools

class BulkCopyCommand extends FsHBaseDataStoreCommand {

  override val name: String = "bulk-copy"
  override val params = new BulkCopyParams

  val libjarsFile: String = "org/locationtech/geomesa/fs/hbase/tools/ingest-libjars.list"

  def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_FS_HBASE_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[HBaseDataStore]),
    () => ClassPathUtils.getJarsFromClasspath(classOf[FileSystemDataStore])
  )

  override def execute(): Unit = withDataStore(run)

  def run(hbaseDs: HBaseDataStore, fsDs: FileSystemDataStore): Unit = {
    val sft = fsDs.getSchema(params.featureName)
    if (sft == null) {
      throw new ParameterException(s"Schema '${params.featureName}' does not exist")
    }
    if (hbaseDs.getSchema(params.featureName) == null) {
      Command.user.info(s"Creating HBase schema ${params.featureName}...")
      hbaseDs.createSchema(sft)
      Command.user.info(s"Done")
    }

    val index = params.loadRequiredIndex(hbaseDs, IndexMode.Write).asInstanceOf[HBaseFeatureIndex]
    val tempDir = Option(params.tempDir).map(t => new Path(t))
    val output = new Path(params.output)
    val filter = Option(params.cqlFilter)
    val statusCallback = new PrintProgress(System.err, TextTools.buildString(' ', 60), '\u003d', '\u003e', '\u003e')

    val start = System.currentTimeMillis()
    val job = new BulkCopyIndexJob()
    val (ok, success, failed) = job.run(fsConnection, hbaseConnection, params.featureName, filter, index.identifier,
      output, tempDir, libjarsFile, libjarsPaths, statusCallback)
    Command.user.info(s"Bulk copy ${if (ok) "complete" else "failed" } in ${TextTools.getTime(start)} with $success " +
        s"features copied and $failed features failed")

    if (ok && params.load) {
      Command.user.info(s"Running HBase incremental load...")
      val start = System.currentTimeMillis()
      val tableName = TableName.valueOf(index.getTableName(params.featureName, hbaseDs))
      val table = hbaseDs.connection.getTable(tableName)
      val locator = hbaseDs.connection.getRegionLocator(tableName)
      // TODO option for "hbase.loadincremental.validate.hfile"
      new LoadIncrementalHFiles(new Configuration).doBulkLoad(output, hbaseDs.connection.getAdmin, table, locator)
      Command.user.info(s"HBase incremental load complete in ${TextTools.getTime(start)}")
    }
  }
}

object BulkCopyCommand {

  @Parameters(commandDescription = "Bulk copy data into HFiles")
  class BulkCopyParams extends FsHBaseParams with RequiredTypeNameParam with TempDirParam
      with OptionalCqlFilterParam with RequiredIndexParam {
    @Parameter(names = Array("--output"), description = "Path to write output HFiles", required = true)
    var output: String = _
    @Parameter(names = Array("--load"), description = "Run HBase incremental load on HFiles")
    var load: Boolean = false
  }
}
