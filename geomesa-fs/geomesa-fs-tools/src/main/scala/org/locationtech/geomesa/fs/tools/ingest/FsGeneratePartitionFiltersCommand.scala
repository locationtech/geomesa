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
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionScheme
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand
import org.locationtech.geomesa.fs.tools.FsDataStoreCommand.{FsParams, PartitionParam}
import org.locationtech.geomesa.fs.tools.ingest.FsGeneratePartitionFiltersCommand.FsGeneratePartitionFiltersParams
import org.locationtech.geomesa.tools.{Command, RequiredTypeNameParam}

import java.time.ZonedDateTime

class FsGeneratePartitionFiltersCommand extends FsDataStoreCommand {

  import org.locationtech.geomesa.filter.andFilters

  override val params = new FsGeneratePartitionFiltersParams()

  override val name: String = "generate-partition-filters"

  override def execute(): Unit = withDataStore { ds =>
    if (params.date == null && params.loadedPartitions.isEmpty) {
      throw new ParameterException("At least one of --partition, --partition-file, or --date must be specified")
    }

    val storage = ds.storage(params.featureName)

    val fromDate = Option(params.date).toSeq.flatMap(PartitionScheme.enumerate(storage.schemes, _))

    val partitions = if (params.loadedPartitions.isEmpty) { fromDate } else { (params.loadedPartitions ++ fromDate).distinct }

    Command.user.info(s"Generating filters for ${partitions.size} partitions")
    if (!params.noHeader) {
      Command.output.info("Partition\tFilter")
    }

    partitions.sortBy(_.toString).foreach { partition =>
      val filters = partition.values.flatMap(v => storage.schemes.find(_.name == v.name).map(_.getCoveringFilter(v)))
      val filter = ECQL.toCQL(andFilters(filters))
      Command.output.info(s"$partition\t$filter")
    }
  }
}

object FsGeneratePartitionFiltersCommand {
  @Parameters(commandDescription = "Generate filters corresponding to partitions")
  class FsGeneratePartitionFiltersParams extends FsParams with RequiredTypeNameParam with PartitionParam {

    @Parameter(names = Array("--date"), description = "Date to get partitions for")
    var date: ZonedDateTime = _

    @Parameter(names = Array("--no-header"), description = "Suppress output header")
    var noHeader: Boolean = false
  }
}
