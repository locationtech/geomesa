/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils.FileType
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, PartitionParam}
import org.locationtech.geomesa.fs.tools.ingest.FsGeneratePartitionFiltersCommand.FsGeneratePartitionFiltersParams
import org.locationtech.geomesa.tools.{Command, OptionalCqlFilterParam, RequiredTypeNameParam}

class FsGeneratePartitionFiltersCommand extends FsDataStoreCommand {

  import scala.collection.JavaConverters._

  override val params = new FsGeneratePartitionFiltersParams()

  override val name: String = "generate-partition-filters"

  override def execute(): Unit = withDataStore { ds =>
    val storage = ds.storage(params.featureName)
    val root = storage.context.root
    val metadata = storage.metadata

    lazy val fromFilter = metadata.scheme.getIntersectingPartitions(params.cqlFilter).getOrElse {
      throw new ParameterException("Filter does not correspond to partition scheme - no matching partitions found")
    }
    val partitions = (params.cqlFilter, params.partitions.asScala) match {
      case (null, Seq()) => throw new ParameterException("At least one of --partitions or --cql must be provided")
      case (null, names) => names
      case (_, Seq())    => fromFilter
      case (_, names)    => fromFilter.intersect(names)
    }

    Command.user.info(s"Generating filters for ${partitions.length} partitions")
    if (!params.noHeader) {
      Command.output.info("Partition\tPath\tFilter")
    }

    partitions.sorted.foreach { partition =>
      val path = StorageUtils.nextFile(root, partition, metadata.leafStorage, "", FileType.Imported, "")
      val prefix = path.toString.dropRight(1) // drop '.'
      val filter = ECQL.toCQL(metadata.scheme.getCoveringFilter(partition))
      Command.output.info(s"$partition\t$prefix\t$filter")
    }
  }
}

object FsGeneratePartitionFiltersCommand {
  @Parameters(commandDescription = "Generate filters corresponding to partitions")
  class FsGeneratePartitionFiltersParams extends FsParams
      with RequiredTypeNameParam with PartitionParam with OptionalCqlFilterParam {
    @Parameter(names = Array("--no-header"), description = "Suppress output header")
    var noHeader: Boolean = false
  }
}
