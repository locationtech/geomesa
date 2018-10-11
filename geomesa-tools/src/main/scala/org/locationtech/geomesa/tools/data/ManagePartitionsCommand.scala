/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.data

import java.time.{ZoneOffset, ZonedDateTime}

import com.beust.jcommander.{JCommander, Parameter, ParameterException, Parameters}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.index.conf.partition.{TablePartition, TimePartition}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.data.ManagePartitionsCommand._
import org.locationtech.geomesa.tools.utils.ParameterConverters.IntervalConverter
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.SimpleFeatureType

/**
  * List, add, delete partitioned tables
  *
  * @param runner runner
  * @param jc jcommander instance
  */
abstract class ManagePartitionsCommand(val runner: Runner, val jc: JCommander) extends CommandWithSubCommands {

  override val name = "manage-partitions"
  override val params = new ManagePartitionsParams()

  override val subCommands: Seq[Command] = Seq(list, add, adopt, delete, generate)

  protected def list: ListPartitionsCommand[_, _, _]
  protected def add: AddPartitionsCommand[_, _, _]
  protected def adopt: AdoptPartitionCommand[_, _, _]
  protected def delete: DeletePartitionsCommand[_, _, _]
  protected def generate: NamePartitionsCommand[_, _, _]
}

object ManagePartitionsCommand {

  import scala.collection.JavaConverters._

  @Parameters(commandDescription = "Manage partitioned schemas")
  class ManagePartitionsParams {}

  /**
    * List existing partitions
    */
  trait ListPartitionsCommand[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
      extends PartitionsCommand[DS, F, W] {

    override val name = "list"

    override protected def execute(ds: DS, sft: SimpleFeatureType, partition: TablePartition): Unit = {
      Command.user.info(s"Partitions for schema ${params.featureName}:")
      val partitions = ds.manager.indices(sft).par.flatMap(_.getPartitions(sft, ds))
      partitions.seq.distinct.sorted.foreach(p => Command.output.info(p))
    }
  }

  /**
    * Add new partitions
    */
  trait AddPartitionsCommand[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
      extends ModifyPartitionsCommand[DS, F, W] {

    override val name = "add"

    override protected def modify(ds: DS, sft: SimpleFeatureType, partition: TablePartition, p: String): Unit = {
      Command.user.info(s"Adding partition '$p'")
      ds.manager.indices(sft, mode = IndexMode.Write).par.foreach(_.configure(sft, ds, Some(p)))
    }
  }

  /**
    * Adopt an existing set of index tables as a new partitions
    */
  trait AdoptPartitionCommand[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
      extends PartitionsCommand[DS, F, W] {

    override val name = "adopt"
    override def params: AdoptPartitionParam

    override protected def execute(ds: DS, sft: SimpleFeatureType, partition: TablePartition): Unit = {
      val time = Option(partition).collect { case p: TimePartition => p }.getOrElse {
        throw new NotImplementedError(s"Unsupported partition implementation: ${partition.getClass.getName}")
      }
      val (start, end) = {
        val (s, e) = new IntervalConverter("value").convert(params.value)
        (ZonedDateTime.ofInstant(s.toInstant, ZoneOffset.UTC), ZonedDateTime.ofInstant(e.toInstant, ZoneOffset.UTC))
      }
      val tables = params.tables.asScala

      Command.user.info(s"Adopting tables ${tables.mkString(", ")} as partition ${params.partition}")

      val indices = ds.manager.indices(sft)
      if (indices.lengthCompare(tables.length) != 0) {
        throw new IllegalArgumentException(s"Expected an index table for each index: ${indices.map(_.name).mkString(", ")}")
      }
      // match tables first, to fail fast if there is no match
      val indicesAndTables = indices.map { index =>
        val table = tables.find(_.contains(GeoMesaFeatureIndex.tableSuffix(index, None))).getOrElse {
          throw new IllegalArgumentException(s"None of the index tables correspond to ${index.identifier}")
        }
        (index, table)
      }
      indicesAndTables.foreach { case (index, table) =>
        ds.metadata.insert(sft.getTypeName, index.tableNameKey(Some(params.partition)), table)
      }
      time.register(params.partition, start, end)

      Command.user.info("Done")
    }
  }

  /**
    * Delete existing partitions
    */
  trait DeletePartitionsCommand[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
      extends ModifyPartitionsCommand[DS, F, W] {

    override val name = "delete"
    override def params: PartitionsParam with OptionalForceParam

    override protected def execute(ds: DS, sft: SimpleFeatureType, partition: TablePartition): Unit = {
      lazy val prompt = s"Deleting partition(s) '${params.partitions.asScala.mkString("', '")}'. Continue (y/n)? "
      if (params.force || Prompt.confirm(prompt)) {
        super.execute(ds, sft, partition)
      } else {
        Command.user.info("Delete cancelled")
      }
    }

    override protected def modify(ds: DS, sft: SimpleFeatureType, partition: TablePartition, p: String): Unit = {
      Command.user.info(s"Deleting partition '$p'")
      ds.manager.indices(sft).par.foreach(_.delete(sft, ds, Some(p)))
    }
  }

  /**
    * Map from values (e.g. dates) to partition names
    */
  trait NamePartitionsCommand[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
      extends PartitionsCommand[DS, F, W] {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    override val name = "name"
    override def params: NamePartitionsParam

    override protected def execute(ds: DS, sft: SimpleFeatureType, partition: TablePartition): Unit = {
      partition match {
        case p: TimePartition =>
          val sf = new ScalaSimpleFeature(sft, "")
          params.values.asScala.foreach { value =>
            sf.setAttribute(sft.getDtgIndex.get, value)
            Command.output.info(s"$value -> ${partition.partition(sf)}")
          }

        case _ => throw new NotImplementedError(s"Unsupported partition implementation: ${partition.getClass.getName}")
      }
    }
  }

  /**
    * Base trait to facilitate acting on each partition
    */
  trait ModifyPartitionsCommand[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
      extends PartitionsCommand[DS, F, W] {

    override def params: PartitionsParam

    override protected def execute(ds: DS, sft: SimpleFeatureType, partition: TablePartition): Unit = {
      params.partitions.asScala.foreach(modify(ds, sft, partition, _))
      Command.user.info("Done")
    }

    protected def modify(ds: DS, sft: SimpleFeatureType, partition: TablePartition, p: String): Unit
  }

  /**
    * Base trait for dealing with partitions
    */
  trait PartitionsCommand[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
      extends DataStoreCommand[DS] {

    override def params: RequiredTypeNameParam

    override def execute(): Unit = {
      withDataStore { ds =>
        val sft = ds.getSchema(params.featureName)
        if (sft == null) {
          throw new ParameterException(s"Schema '${params.featureName}' does not exist in the data store. " +
              s"Available types: ${ds.getTypeNames.mkString(", ")}")
        }
        TablePartition(ds, sft) match {
          case None => Command.user.error(s"Schema is not partitioned")
          case Some(p) => execute(ds, sft, p)
        }
      }
    }

    protected def execute(ds: DS, sft: SimpleFeatureType, partition: TablePartition): Unit
  }

  trait PartitionsParam extends RequiredTypeNameParam {
    @Parameter(names = Array("--partition"), description = "Partition(s) to operate on", required = true)
    var partitions: java.util.List[String] = _
  }

  trait AdoptPartitionParam extends RequiredTypeNameParam {
    @Parameter(names = Array("--partition"), description = "Partition name to adopt", required = true)
    var partition: String = _

    @Parameter(names = Array("--table"), description = "Name(s) of the index table(s) to adopt", required = true)
    var tables: java.util.List[String] = _

    @Parameter(names = Array("--value"), description = "Value used to bound the partition", required = true)
    var value: String = _
  }

  trait NamePartitionsParam extends RequiredTypeNameParam {
    @Parameter(names = Array("--value"), description = "Value(s) used to generate partitions", required = true)
    var values: java.util.List[String] = _
  }
}
