/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.nio.charset.StandardCharsets
import java.util.regex.Pattern
import java.util.{Collections, Locale, UUID}

import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{FilterList, KeyOnlyFilter, MultiRowRangeFilter, Filter => HFilter}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.security.visibility.CellVisibility
import org.apache.hadoop.hbase.{Coprocessor, HColumnDescriptor, HTableDescriptor, TableName}
import org.locationtech.geomesa.hbase.HBaseSystemProperties
import org.locationtech.geomesa.hbase.HBaseSystemProperties.{CoprocessorPath, TableAvailabilityTimeout}
import org.locationtech.geomesa.hbase.coprocessor.aggregators.HBaseArrowAggregator.HBaseArrowResultsToFeatures
import org.locationtech.geomesa.hbase.coprocessor.aggregators.HBaseBinAggregator.HBaseBinResultsToFeatures
import org.locationtech.geomesa.hbase.coprocessor.aggregators.HBaseDensityAggregator.HBaseDensityResultsToFeatures
import org.locationtech.geomesa.hbase.coprocessor.aggregators.HBaseStatsAggregator.HBaseStatsResultsToFeatures
import org.locationtech.geomesa.hbase.coprocessor.aggregators.{HBaseArrowAggregator, HBaseBinAggregator, HBaseDensityAggregator, HBaseStatsAggregator}
import org.locationtech.geomesa.hbase.coprocessor.{AllCoprocessors, GeoMesaCoprocessor}
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan.{CoprocessorPlan, EmptyPlan, ScanPlan}
import org.locationtech.geomesa.hbase.filters._
import org.locationtech.geomesa.hbase.utils.HBaseVersions
import org.locationtech.geomesa.index.api.IndexAdapter.BaseIndexWriter
import org.locationtech.geomesa.index.api.QueryPlan.IndexResultsToFeatures
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api.{WritableFeature, _}
import org.locationtech.geomesa.index.filters.{S2Filter, S3Filter, Z2Filter, Z3Filter}
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.s2.{S2Index, S2IndexValues}
import org.locationtech.geomesa.index.index.s3.{S3Index, S3IndexValues}
import org.locationtech.geomesa.index.index.z2.{Z2Index, Z2IndexValues}
import org.locationtech.geomesa.index.index.z3.{Z3Index, Z3IndexValues}
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.planning.LocalQueryRunner.{ArrowDictionaryHook, LocalTransformReducer}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.{CloseWithLogging, FlushWithLogging, WithClose}
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.control.NonFatal

class HBaseIndexAdapter(ds: HBaseDataStore) extends IndexAdapter[HBaseDataStore] with StrictLogging {

  import HBaseIndexAdapter._

  import scala.collection.JavaConverters._

  override def createTable(
      index: GeoMesaFeatureIndex[_, _],
      partition: Option[String],
      splits: => Seq[Array[Byte]]): Unit = {
    // write table name to metadata
    val name = TableName.valueOf(index.configureTableName(partition, tableNameLimit))

    WithClose(ds.connection.getAdmin) { admin =>
      if (!admin.tableExists(name)) {
        logger.debug(s"Creating table $name")

        val conf = admin.getConfiguration
        val compression = Option(index.sft.getUserData.get(Configs.TableCompression)).collect {
          case e: String if e.toBoolean =>
            // note: all compression types in HBase are case-sensitive and lower-cased
            val compressionType = index.sft.getUserData.get(Configs.TableCompressionType) match {
              case null => "gz"
              case t: String => t.toLowerCase(Locale.US)
            }
            logger.debug(s"Setting compression '$compressionType' on table $name for feature ${index.sft.getTypeName}")
            Compression.getCompressionAlgorithmByName(compressionType)
        }

        val descriptor = new HTableDescriptor(name)

        groups.apply(index.sft).foreach { case (k, _) =>
          val column = new HColumnDescriptor(k)
          column.setBloomFilterType(BloomType.NONE)
          compression.foreach(column.setCompressionType)
          if (index.name != IdIndex.name) {
            column.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
          }
          HBaseVersions.addFamily(descriptor, column)
        }

        if (ds.config.remoteFilter) {
          lazy val coprocessorUrl = ds.config.coprocessorUrl.orElse(CoprocessorPath.option.map(new Path(_))).orElse {
            try {
              // the jar should be under hbase.dynamic.jars.dir to enable filters, so look there
              val dir = new Path(conf.get("hbase.dynamic.jars.dir"))
              WithClose(dir.getFileSystem(conf)) { fs =>
                if (!fs.isDirectory(dir)) { None } else {
                  fs.listStatus(dir).collectFirst {
                    case s if distributedJarNamePattern.matcher(s.getPath.getName).matches() => s.getPath
                  }
                }
              }
            } catch {
              case NonFatal(e) => logger.warn("Error checking dynamic jar path:", e); None
            }
          }

          def addCoprocessor(clazz: Class[_ <: Coprocessor], desc: HTableDescriptor): Unit = {
            val name = clazz.getCanonicalName
            if (!desc.getCoprocessors.contains(name)) {
              logger.debug(s"Using coprocessor path ${coprocessorUrl.orNull}")
              // TODO: Warn if the path given is different from paths registered in other coprocessors
              // if so, other tables would need updating
              HBaseVersions.addCoprocessor(desc, name, coprocessorUrl)
            }
          }

          // if the coprocessors are installed site-wide don't register them in the table descriptor
          val installed = Option(conf.get(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY))
          val names = installed.map(_.split(":").toSet).getOrElse(Set.empty[String])
          AllCoprocessors.foreach(c => if (!names.contains(c.getCanonicalName)) { addCoprocessor(c, descriptor) })
        }

        try { admin.createTableAsync(descriptor, splits.toArray) } catch {
          case _: org.apache.hadoop.hbase.TableExistsException => // ignore, another thread created it for us
        }
      }

      waitForTable(admin, name)
    }
  }

  override def renameTable(from: String, to: String): Unit = {
    WithClose(ds.connection.getAdmin) { admin =>
      val existing = TableName.valueOf(from)
      val renamed = TableName.valueOf(to)
      if (admin.tableExists(existing)) {
        // renaming in hbase requires creating a snapshot and using that to create the new table
        val snapshot = StringSerialization.alphaNumericSafeString(UUID.randomUUID().toString)
        admin.disableTable(existing)
        admin.snapshot(snapshot, existing)
        admin.cloneSnapshot(snapshot, renamed)
        admin.deleteSnapshot(snapshot)
        admin.deleteTable(existing)
        waitForTable(admin, renamed)
      }
    }
  }

  override def deleteTables(tables: Seq[String]): Unit = {
    WithClose(ds.connection.getAdmin) { admin =>
      tables.par.foreach { name =>
        val table = TableName.valueOf(name)
        if (admin.tableExists(table)) {
          admin.disableTableAsync(table)
          val timeout = TableAvailabilityTimeout.toDuration.filter(_.isFinite())
          logger.debug(s"Waiting for table '$table' to be disabled with " +
              s"${timeout.map(t => s"a timeout of $t").getOrElse("no timeout")}")
          val stop = timeout.map(t => System.currentTimeMillis() + t.toMillis)
          while (!admin.isTableDisabled(table) && stop.forall(_ > System.currentTimeMillis())) {
            Thread.sleep(1000)
          }
          // no async operation, but timeout can be controlled through hbase-site.xml "hbase.client.sync.wait.timeout.msec"
          admin.deleteTable(table)
        }
      }
    }
  }

  override def clearTables(tables: Seq[String], prefix: Option[Array[Byte]]): Unit = {
    tables.par.foreach { name =>
      val tableName = TableName.valueOf(name)
      WithClose(ds.connection.getTable(tableName)) { table =>
        val scan = new Scan().setFilter(new KeyOnlyFilter)
        prefix.foreach(scan.setRowPrefixFilter)
        ds.applySecurity(scan)
        val mutateParams = new BufferedMutatorParams(tableName)
        WithClose(table.getScanner(scan), ds.connection.getBufferedMutator(mutateParams)) { case (scanner, mutator) =>
          scanner.iterator.asScala.grouped(10000).foreach { result =>
            // TODO GEOMESA-2546 set delete visibilities
            val deletes = result.map(r => new Delete(r.getRow))
            mutator.mutate(deletes.asJava)
          }
        }
      }
    }
  }

  override def createQueryPlan(strategy: QueryStrategy): HBaseQueryPlan = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val QueryStrategy(filter, byteRanges, _, _, ecql, hints, _) = strategy
    val index = filter.index

    // index api defines empty start/end for open-ended range
    // index api defines start row inclusive, end row exclusive
    // both these conventions match the conventions for hbase scan objects
    val ranges = byteRanges.map {
      case BoundedByteRange(start, end) => new Scan(start, end)
      case SingleRowByteRange(row)      => new Scan(row, ByteArrays.rowFollowingRow(row)).setSmall(true)
    }

    val tables = index.getTablesForQuery(filter.filter).map(TableName.valueOf)
    val (colFamily, schema) = groups.group(index.sft, hints.getTransformDefinition, ecql)

    val transform: Option[(String, SimpleFeatureType)] = hints.getTransform

    if (!ds.config.remoteFilter) {
      // everything is done client side
      val arrowHook = Some(ArrowDictionaryHook(ds.stats, filter.filter))
      // note: we assume visibility filtering is still done server-side as it's part of core hbase
      // note: we use the full filter here, since we can't use the z3 server-side filter
      // for some attribute queries we wouldn't need the full filter...
      val reducer = Some(new LocalTransformReducer(schema, filter.filter, None, transform, hints, arrowHook))

      if (ranges.isEmpty) { EmptyPlan(filter, reducer) } else {
        val scans = configureScans(ranges, colFamily, Seq.empty, coprocessor = false)
        val resultsToFeatures = new HBaseResultsToFeatures(index, schema)
        val sort = hints.getSortFields
        val max = hints.getMaxFeatures
        val project = hints.getProjection
        ScanPlan(filter, tables, ranges, scans, resultsToFeatures, reducer, sort, max, project)
      }
    } else {
      lazy val returnSchema = transform.map(_._2).getOrElse(schema)

      val notSampling = hints.getSampling.isEmpty

      lazy val cqlFilter = if (ecql.isEmpty && transform.isEmpty && notSampling) { Seq.empty } else {
        Seq((CqlTransformFilter.Priority, CqlTransformFilter(schema, strategy.index, ecql, transform, hints)))
      }

      // TODO pull this out to be SPI loaded so that new indices can be added seamlessly
      val indexFilter = strategy.index match {
        case _: Z3Index =>
          strategy.values.map { case v: Z3IndexValues =>
            (Z3HBaseFilter.Priority, Z3HBaseFilter(Z3Filter(v), index.keySpace.sharding.length))
          }

        case _: Z2Index =>
          strategy.values.map { case v: Z2IndexValues =>
            (Z2HBaseFilter.Priority, Z2HBaseFilter(Z2Filter(v), index.keySpace.sharding.length))
          }

        case _: S2Index =>
          strategy.values.map { case v: S2IndexValues =>
            (S2HBaseFilter.Priority, S2HBaseFilter(S2Filter(v), index.keySpace.sharding.length))
          }

        case _: S3Index =>
          strategy.values.map { case v: S3IndexValues =>
            (S3HBaseFilter.Priority, S3HBaseFilter(S3Filter(v), index.keySpace.sharding.length))
          }
        // TODO GEOMESA-1807 deal with non-points in a pushdown XZ filter

        case _ => None
      }

      // if there is a coprocessorConfig it handles filter/transform
      lazy val Seq(cScan) = configureScans(ranges, colFamily, indexFilter.toSeq.map(_._2), coprocessor = true)

      val max = hints.getMaxFeatures
      val projection = hints.getProjection
      lazy val timeout = strategy.index.ds.config.queryTimeout.map(GeoMesaCoprocessor.timeout)

      if (hints.isDensityQuery) {
        if (ranges.isEmpty) { EmptyPlan(filter, None) } else {
          val options = HBaseDensityAggregator.configure(schema, index, ecql, hints)
          val results = new HBaseDensityResultsToFeatures()
          CoprocessorPlan(filter, tables, ranges, cScan, options ++ timeout, results, None, max, projection)
        }
      } else if (hints.isArrowQuery) {
        val (options, reducer) = HBaseArrowAggregator.configure(schema, index, ds.stats, filter.filter, ecql, hints)
        if (ranges.isEmpty) { EmptyPlan(filter, Some(reducer)) } else {
          val results = new HBaseArrowResultsToFeatures()
          CoprocessorPlan(filter, tables, ranges, cScan, options ++ timeout, results, Some(reducer), max, projection)
        }
      } else if (hints.isStatsQuery) {
        val reducer = Some(StatsScan.StatsReducer(returnSchema, hints))
        if (ranges.isEmpty) { EmptyPlan(filter, reducer) } else {
          val options = HBaseStatsAggregator.configure(schema, index, ecql, hints)
          val results = new HBaseStatsResultsToFeatures()
          CoprocessorPlan(filter, tables, ranges, cScan, options ++ timeout, results, reducer, max, projection)
        }
      } else if (hints.isBinQuery) {
        if (ranges.isEmpty) { EmptyPlan(filter,  None) } else {
          val options = HBaseBinAggregator.configure(schema, index, ecql, hints)
          val results = new HBaseBinResultsToFeatures()
          CoprocessorPlan(filter, tables, ranges, cScan, options ++ timeout, results, None, max, projection)
        }
      } else {
        if (ranges.isEmpty) { EmptyPlan(filter, None) } else {
          val filters = (cqlFilter ++ indexFilter).sortBy(_._1).map(_._2)
          val scans = configureScans(ranges, colFamily, filters, coprocessor = false)
          val results = new HBaseResultsToFeatures(index, returnSchema)
          ScanPlan(filter, tables, ranges, scans, results, None, hints.getSortFields, max, projection)
        }
      }
    }
  }

  override def createWriter(sft: SimpleFeatureType,
                            indices: Seq[GeoMesaFeatureIndex[_, _]],
                            partition: Option[String]): HBaseIndexWriter =
    new HBaseIndexWriter(ds, indices, WritableFeature.wrapper(sft, groups), partition)

  /**
    * Configure the hbase scan
    *
    * @param originalRanges ranges to scan
    * @param colFamily col family to scan
    * @param filters scan filters
    * @param coprocessor coprocessor scan or not
    * @return
    */
  protected def configureScans(originalRanges: Seq[Scan],
                               colFamily: Array[Byte],
                               filters: Seq[HFilter],
                               coprocessor: Boolean): Seq[Scan] = {
    import scala.collection.JavaConverters._

    val cacheBlocks = HBaseSystemProperties.ScannerBlockCaching.toBoolean.get // has a default value so .get is safe
    val cacheSize = HBaseSystemProperties.ScannerCaching.toInt

    logger.debug(s"HBase client scanner: block caching: $cacheBlocks, caching: $cacheSize")

    if (coprocessor) {
      val scan = new Scan()
      scan.addFamily(colFamily)
      // note: mrrf first priority
      val mrrf = new MultiRowRangeFilter(sortAndMerge(originalRanges))
      // note: our coprocessors always expect a filter list
      scan.setFilter(new FilterList(filters.+:(mrrf): _*))
      scan.setCacheBlocks(cacheBlocks)
      cacheSize.foreach(scan.setCaching)
      ds.applySecurity(scan)
      Seq(scan)
    } else if (originalRanges.headOption.exists(_.isSmall)) {
      val filter = filters match {
        case Nil    => None
        case Seq(f) => Some(f)
        case f      => Some(new FilterList(f: _*))
      }
      originalRanges.map { s =>
        val scan = new Scan(s).addFamily(colFamily)
        filter.foreach(scan.setFilter)
        scan.setCacheBlocks(cacheBlocks)
        cacheSize.foreach(scan.setCaching)
        ds.applySecurity(scan)
        scan
      }
    } else {
      val rowRanges = sortAndMerge(originalRanges)

      // TODO GEOMESA-1806 parameterize this?
      val rangesPerThread = math.min(ds.config.maxRangesPerExtendedScan,
        math.max(1, math.ceil(rowRanges.size() / ds.config.queryThreads * 2).toInt))

      // group scans into batches to achieve some client side parallelism
      // we double the initial size to account for extra groupings based on the shard byte
      val groupedScans = new java.util.ArrayList[Scan]((rowRanges.size() / rangesPerThread + 1) * 2)

      def addGroup(group: java.util.List[RowRange]): Unit = {
        val s = new Scan(group.get(0).getStartRow, group.get(group.size() - 1).getStopRow).addFamily(colFamily)
        if (group.size() > 1) {
          // TODO GEOMESA-1806
          // currently, the MultiRowRangeFilter constructor will call sortAndMerge a second time
          // this is unnecessary as we have already sorted and merged
          val mrrf = new MultiRowRangeFilter(group)
          // note: mrrf first priority
          s.setFilter(if (filters.isEmpty) { mrrf } else { new FilterList(filters.+:(mrrf): _*) })
        } else {
          filters match {
            case Nil    => // no-op
            case Seq(f) => s.setFilter(f)
            case f      => s.setFilter(new FilterList(f: _*))
          }
        }

        s.setCacheBlocks(cacheBlocks)
        cacheSize.foreach(s.setCaching)

        // apply visibilities
        ds.applySecurity(s)

        groupedScans.add(s)
      }

      // TODO GEOMESA-1806 align partitions with region boundaries

      if (!rowRanges.isEmpty) {
        var i = 1
        var groupStart = 0
        var groupCount = 1
        var groupFirstByte: Byte =
          if (rowRanges.get(0).getStartRow.isEmpty) { 0 } else { rowRanges.get(0).getStartRow()(0) }

        while (i < rowRanges.size()) {
          val nextRange = rowRanges.get(i)
          // add the group if we hit our group size or if we transition the first byte (i.e. our shard byte)
          if (groupCount == rangesPerThread ||
              (nextRange.getStartRow.length > 0 && groupFirstByte != nextRange.getStartRow()(0))) {
            // note: excludes current range we're checking
            addGroup(rowRanges.subList(groupStart, i))
            groupFirstByte = if (nextRange.getStopRow.isEmpty) { Byte.MaxValue } else { nextRange.getStopRow()(0) }
            groupStart = i
            groupCount = 1
          } else {
            groupCount += 1
          }
          i += 1
        }

        // add the final group - there will always be at least one remaining range
        addGroup(rowRanges.subList(groupStart, i))
      }

      // shuffle the ranges, otherwise our threads will tend to all hit the same region server at once
      Collections.shuffle(groupedScans)

      groupedScans.asScala
    }
  }

  /**
    * Waits for a table to come online after being created
    *
    * @param admin hbase admin
    * @param table table name
    */
  private def waitForTable(admin: Admin, table: TableName): Unit = {
    if (!admin.isTableAvailable(table)) {
      val timeout = TableAvailabilityTimeout.toDuration.filter(_.isFinite())
      logger.debug(s"Waiting for table '$table' to become available with " +
          s"${timeout.map(t => s"a timeout of $t").getOrElse("no timeout")}")
      val stop = timeout.map(t => System.currentTimeMillis() + t.toMillis)
      while (!admin.isTableAvailable(table) && stop.forall(_ > System.currentTimeMillis())) {
        Thread.sleep(1000)
      }
    }
  }
}

object HBaseIndexAdapter extends LazyLogging {

  private val distributedJarNamePattern = Pattern.compile("^geomesa-hbase-distributed-runtime.*\\.jar$")

  val durability: Durability = HBaseSystemProperties.WalDurability.option match {
    case Some(value) =>
      Durability.values.find(_.toString.equalsIgnoreCase(value)).getOrElse {
        logger.error(s"Invalid HBase WAL durability setting: $value. Falling back to default durability")
        Durability.USE_DEFAULT
      }
    case None => Durability.USE_DEFAULT
  }

  /**
    * Scala convenience method for org.apache.hadoop.hbase.filter.MultiRowRangeFilter#sortAndMerge(java.util.List)
    *
    * @param ranges scan ranges
    * @return
    */
  def sortAndMerge(ranges: Seq[Scan]): java.util.List[RowRange] = {
    val rowRanges = new java.util.ArrayList[RowRange](ranges.length)
    ranges.foreach(r => rowRanges.add(new RowRange(r.getStartRow, true, r.getStopRow, false)))
    MultiRowRangeFilter.sortAndMerge(rowRanges)
  }

  /**
    * Deserializes row bytes into simple features
    *
    * @param _index index
    * @param _sft sft
    */
  class HBaseResultsToFeatures(_index: GeoMesaFeatureIndex[_, _], _sft: SimpleFeatureType) extends
    IndexResultsToFeatures[Result](_index, _sft) {

    def this() = this(null, null) // no-arg constructor required for serialization

    override def apply(result: Result): SimpleFeature = {
      val cell = result.rawCells()(0)
      val id = index.getIdFromRow(cell.getRowArray, cell.getRowOffset, cell.getRowLength, null)
      serializer.deserialize(id, cell.getValueArray, cell.getValueOffset, cell.getValueLength)
    }
  }

  /**
    * Writer for hbase
    *
    * @param ds datastore
    * @param indices indices to write to
    * @param partition partition to write to
    */
  class HBaseIndexWriter(
      ds: HBaseDataStore,
      indices: Seq[GeoMesaFeatureIndex[_, _]],
      wrapper: FeatureWrapper[WritableFeature],
      partition: Option[String]
    ) extends BaseIndexWriter(indices, wrapper) {

    private val batchSize = HBaseSystemProperties.WriteBatchSize.toLong

    private val mutators = indices.toArray.map { index =>
      val table = index.getTableNames(partition) match {
        case Seq(t) => t // should always be writing to a single table here
        case tables => throw new IllegalStateException(s"Expected a single table but got: ${tables.mkString(", ")}")
      }
      val params = new BufferedMutatorParams(TableName.valueOf(table))
      batchSize.foreach(params.writeBufferSize)
      ds.connection.getBufferedMutator(params)
    }

    private var i = 0

    override protected def write(feature: WritableFeature, values: Array[RowKeyValue[_]], update: Boolean): Unit = {
      if (update) {
        // for updates, ensure that our timestamps don't clobber each other
        flush()
        Thread.sleep(1)
      }
      i = 0
      while (i < values.length) {
        val mutator = mutators(i)
        values(i) match {
          case kv: SingleRowKeyValue[_] =>
            kv.values.foreach { value =>
              val put = new Put(kv.row)
              put.addImmutable(value.cf, value.cq, value.value)
              if (!value.vis.isEmpty) {
                put.setCellVisibility(new CellVisibility(new String(value.vis, StandardCharsets.UTF_8)))
              }
              put.setDurability(durability)
              mutator.mutate(put)
            }

          case mkv: MultiRowKeyValue[_] =>
            mkv.rows.foreach { row =>
              mkv.values.foreach { value =>
                val put = new Put(row)
                put.addImmutable(value.cf, value.cq, value.value)
                if (!value.vis.isEmpty) {
                  put.setCellVisibility(new CellVisibility(new String(value.vis, StandardCharsets.UTF_8)))
                }
                put.setDurability(durability)
                mutator.mutate(put)
              }
            }
        }
        i += 1
      }
    }

    override protected def delete(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
      i = 0
      while (i < values.length) {
        val mutator = mutators(i)
        values(i) match {
          case kv: SingleRowKeyValue[_] =>
            kv.values.foreach { value =>
              val del = new Delete(kv.row)
              del.addFamily(value.cf) // note: passing in the column qualifier seems to keep deletes from working
              if (!value.vis.isEmpty) {
                del.setCellVisibility(new CellVisibility(new String(value.vis, StandardCharsets.UTF_8)))
              }
              mutator.mutate(del)
            }

          case mkv: MultiRowKeyValue[_] =>
            mkv.rows.foreach { row =>
              mkv.values.foreach { value =>
                val del = new Delete(row)
                del.addFamily(value.cf) // note: passing in the column qualifier seems to keep deletes from working
                if (!value.vis.isEmpty) {
                  del.setCellVisibility(new CellVisibility(new String(value.vis, StandardCharsets.UTF_8)))
                }
                mutator.mutate(del)
              }
            }
        }
        i += 1
      }
    }

    override def flush(): Unit = {
      val exceptions = mutators.flatMap(FlushWithLogging.apply)
      if (exceptions.nonEmpty) {
        val head = exceptions.head
        exceptions.tail.foreach(head.addSuppressed)
        throw head
      }
    }

    override def close(): Unit = {
      val exceptions = mutators.flatMap(CloseWithLogging.apply)
      if (exceptions.nonEmpty) {
        val head = exceptions.head
        exceptions.tail.foreach(head.addSuppressed)
        throw head
      }
    }
  }
}
