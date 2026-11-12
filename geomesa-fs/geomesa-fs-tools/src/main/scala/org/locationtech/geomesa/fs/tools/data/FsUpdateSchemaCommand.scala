/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.data

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.FsParams
import org.locationtech.geomesa.fs.tools.data.FsUpdateSchemaCommand.FsUpdateSchemaParams
import org.locationtech.geomesa.tools.data.UpdateSchemaCommand
import org.locationtech.geomesa.tools.data.UpdateSchemaCommand.UpdateSchemaParams
import org.locationtech.geomesa.tools.utils.ParameterConverters.BytesValidator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpecParser
import org.locationtech.geomesa.utils.text.Suffixes.Memory

import scala.util.control.NonFatal

class FsUpdateSchemaCommand extends UpdateSchemaCommand[FileSystemDataStore] with FsDataStoreCommand {

  import org.locationtech.geomesa.fs.storage.core.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  override val params: FsUpdateSchemaParams = new FsUpdateSchemaParams()

  override protected def doUpdate(ds: FileSystemDataStore, typeName: String, updated: SimpleFeatureType): Unit = {
    params.renameAttributes.asScala.grouped(2).foreach { case Seq(from, to) =>
      ds.update(typeName).columns().rename(from, to)
    }
    params.attributes.asScala.foreach { attribute =>
      val descriptor = try { SimpleFeatureSpecParser.parseAttribute(attribute).toDescriptor } catch {
        case NonFatal(e) => throw new ParameterException(s"Invalid attribute spec: $attribute", e)
      }
      ds.update(typeName).columns().add(descriptor)
    }
    if (params.scheme != null) {
      ds.update(typeName).table().setPartitions(params.scheme.split(",").toSeq)
    }
    if (params.targetFileSize != null) {
      // note: Memory.bytes parsing was validated earlier
      ds.update(typeName).metadata().setTargetFileSize(Some(Memory.bytes(params.targetFileSize).get))
    }
    if (!params.plusKeywords.isEmpty || !params.minusKeywords.isEmpty) {
      val keywords = Option(updated.getUserData.get(Configs.Keywords)).map(_.toString).filterNot(_.isBlank)
      ds.update(typeName).metadata().setUserData(Configs.Keywords, keywords)
    }
    params.userData.asScala.foreach { case (k, v) =>
      ds.update(typeName).metadata().setUserData(k, Option(v))
    }
    // do this last so type name change doesn't affect the other ops
    Option(params.rename).foreach { rename =>
      ds.update(typeName).table().rename(rename)
    }
  }

  override protected def storeSpecificUpdates(sft: SimpleFeatureType): Seq[String] = {
    val scheme = Option(params.scheme).map { s =>
      sft.setScheme(s)
      s"Changing partition scheme to '$s'"
    }
    val fileSize = Option(params.targetFileSize).map { s =>
      sft.setTargetFileSize(s)
      s"Setting target file size to '$s'"
    }
    scheme.toSeq ++ fileSize
  }
}

object FsUpdateSchemaCommand {

  @Parameters(commandDescription = "Update a GeoMesa feature type")
  class FsUpdateSchemaParams extends UpdateSchemaParams with FsParams {
    @Parameter(names = Array("--set-partition-scheme"), description = "Update the partition scheme")
    var scheme: String = _

    @Parameter(
      names = Array("--set-target-file-size"),
      description = "Update the target size for data files",
      validateValueWith = Array(classOf[BytesValidator]))
    var targetFileSize: String = _

    override def validate(): Option[ParameterException] = super.validate().filter(_ => scheme == null && targetFileSize == null)
  }
}
