/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import com.beust.jcommander.{Parameter, ParameterException, Parameters}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, PartitionParam}
import org.locationtech.geomesa.fs.tools.ingest.FsGeneratePartitionFiltersCommand.FsGeneratePartitionFiltersParams
import org.locationtech.geomesa.tools.{Command, OptionalCqlFilterParam, RequiredTypeNameParam}

class FsGeneratePartitionFiltersCommand extends FsDataStoreCommand {

  import org.locationtech.geomesa.filter.andFilters

  import scala.collection.JavaConverters._

  override val params = new FsGeneratePartitionFiltersParams()

  override val name: String = "generate-partition-filters"

  override def execute(): Unit = withDataStore { ds =>
    if (params.cqlFilter == null && params.partitions.isEmpty) {
      throw new ParameterException("At least one of --partitions or --cql must be specified")
    }

    val metadata = ds.storage(params.featureName).metadata

    val partitions =
      (params.partitions.asScala ++ Option(params.cqlFilter).toSeq.flatMap(f => metadata.getFiles(f).map(_.file.partition)))
        .distinct

    Command.user.info(s"Generating filters for ${partitions.size} partitions")
    if (!params.noHeader) {
      Command.output.info("Partition\tFilter")
    }

    partitions.toSeq.sortBy(_.encoded).foreach { partition =>
      val filters = partition.values.flatMap(v => metadata.schemes.find(_.name == v.name).map(_.getCoveringFilter(v.value)))
      val filter = ECQL.toCQL(andFilters(filters.toSeq))
      Command.output.info(s"$partition\t$filter")
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
