/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{FilterList, KeyOnlyFilter, MultiRowRangeFilter, Filter => HFilter}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.security.visibility.CellVisibility
import org.apache.hadoop.hbase.{Coprocessor, NamespaceDescriptor, TableName}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.hbase.HBaseSystemProperties
import org.locationtech.geomesa.hbase.HBaseSystemProperties.{CoprocessorPath, CoprocessorUrl, TableAvailabilityTimeout}
import org.locationtech.geomesa.hbase.aggregators.HBaseArrowAggregator.HBaseArrowResultsToFeatures
import org.locationtech.geomesa.hbase.aggregators.HBaseBinAggregator.HBaseBinResultsToFeatures
import org.locationtech.geomesa.hbase.aggregators.HBaseDensityAggregator.HBaseDensityResultsToFeatures
import org.locationtech.geomesa.hbase.aggregators.HBaseStatsAggregator.HBaseStatsResultsToFeatures
import org.locationtech.geomesa.hbase.aggregators.{HBaseArrowAggregator, HBaseBinAggregator, HBaseDensityAggregator, HBaseStatsAggregator}
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan._
import org.locationtech.geomesa.hbase.rpc.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.hbase.rpc.filter._
import org.locationtech.geomesa.index.api.IndexAdapter.{BaseIndexWriter, RequiredVisibilityWriter}
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, IndexResultsToFeatures, ResultsToFeatures}
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.{ColumnGroups, QueryHints}
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.planning.LocalQueryRunner.LocalProcessor
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.{CloseWithLogging, FlushWithLogging, IsFlushableImplicits, WithClose}
import org.locationtech.geomesa.utils.text.StringSerialization

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.util.{Collections, Locale, UUID}
import scala.util.control.NonFatal
import scala.util.{Random, Try}

class HBaseIndexAdapter(ds: HBaseDataStore) extends IndexAdapter[HBaseDataStore] with StrictLogging {

  import HBaseIndexAdapter._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  lazy private val dynamicJarPath: Option[Path] = try {
    val conf = ds.connection.getConfiguration
    // the jar should be under hbase.dynamic.jars.dir to enable filters, so look there
    val dir = new Path(conf.get("hbase.dynamic.jars.dir"))
    WithClose(dir.getFileSystem(conf)) { fs =>
      if (!fs.getFileStatus(dir).isDirectory) { None } else {
        fs.listStatus(dir).collectFirst {
          case s if distributedJarNamePattern.matcher(s.getPath.getName).matches() => s.getPath
        }
      }
    }
  } catch {
    case NonFatal(e) => logger.warn("Error checking dynamic jar path:", e); None
  }

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

        val compression = index.sft.getCompression.map { alg =>
          logger.debug(s"Setting compression '$alg' on table $name for feature ${index.sft.getTypeName}")
          // note: all compression types in HBase are case-sensitive and lower-cased
          Compression.getCompressionAlgorithmByName(alg.toLowerCase(Locale.US))
        }

        val cols = groups.apply(index.sft).map(_._1)
        val bloom = Some(BloomType.NONE)
        val encoding = if (index.name == IdIndex.name) { None } else { Some(DataBlockEncoding.FAST_DIFF) }

        val coprocessor = if (!ds.config.remoteFilter) { None } else {
          // if the coprocessors are installed site-wide don't register them in the table descriptor.
          // this key is CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY - but don't want to pull in
          // a dependency on hbase-server just for this constant
          val installed = Option(conf.get("hbase.coprocessor.user.region.classes"))
          val names = installed.map(_.split(":").toSet).getOrElse(Set.empty[String])
          if (names.contains(CoprocessorClass)) { None } else {
            // noinspection ScalaDeprecation
            def urlFromSysProp: Option[Path] = CoprocessorUrl.option.orElse(CoprocessorPath.option).map(new Path(_))
            val coprocessorUrl = ds.config.coprocessors.url.orElse(urlFromSysProp).orElse(dynamicJarPath)
            logger.debug(s"Using coprocessor path ${coprocessorUrl.orNull}")
            // TODO: Warn if the path given is different from paths registered in other coprocessors
            // if so, other tables would need updating
            Some(CoprocessorClass -> coprocessorUrl)
          }
        }
        val metadata = index.sft.getTableProps

        try {
          createTableAsync(admin, name, cols, bloom, compression, encoding, None, coprocessor, splits, metadata)
        } catch {
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
      def deleteOne(name: String): Unit = {
        val table = TableName.valueOf(name)
        if (admin.tableExists(table)) {
          admin.disableTableAsync(table)
          val timeout = TableAvailabilityTimeout.toUnboundedDuration.filter(_.isFinite)
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
      tables.toList.map(t => CachedThreadPool.submit(() => deleteOne(t))).foreach(_.get)
    }
  }

  override def clearTables(tables: Seq[String], prefix: Option[Array[Byte]]): Unit = {
    def clearOne(name: String): Unit = {
      val tableName = TableName.valueOf(name)
      WithClose(ds.connection.getTable(tableName)) { table =>
        val scan = new Scan().setFilter(new KeyOnlyFilter)
        prefix.foreach(scan.setStartStopRowForPrefixScan)
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
    tables.toList.map(t => CachedThreadPool.submit(() => clearOne(t))).foreach(_.get)
  }

  override def createQueryPlan(strategy: QueryStrategy): HBaseQueryPlan = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val byteRanges = strategy.ranges
    val hints = strategy.hints
    val index = strategy.index

    // index api defines empty start/end for open-ended range
    // index api defines start row inclusive, end row exclusive
    // both these conventions match the conventions for hbase scan objects
    val ranges = byteRanges.map {
      case BoundedByteRange(start, stop) => new RowRange(start, true, stop, false)
      case SingleRowByteRange(row)       => new RowRange(row, true, ByteArrays.rowFollowingRow(row), false)
    }
    val small = byteRanges.headOption.exists(_.isInstanceOf[SingleRowByteRange])

    val tables = index.getTablesForQuery(strategy.filter.filter).map(TableName.valueOf)

    // check for an empty query plan, if there are no tables or ranges to scan
    def empty(reducer: Option[FeatureReducer]): Option[HBaseQueryPlan] =
      if (tables.isEmpty || ranges.isEmpty) { Some(EmptyPlan(ds, strategy, reducer)) } else { None }

    if (!ds.config.remoteFilter) {
      // everything is done client side
      // note: we use the full filter here, since we can't use the z3 server-side filter
      // for some attribute queries we wouldn't need the full filter...
      val (colFamily, schema) = groups.group(index.sft, hints.getTransformDefinition, strategy.filter.filter)
      // re-optimize the filter to account for the colFamily/schema being used
      val ecql = {
        val f = strategy.filter.filter
        if (colFamily.eq(ColumnGroups.Default)) { f } else { f.map(FastFilterFactory.optimize(schema, _)) }
      }
      // note: we assume visibility filtering is still done server-side as it's part of core hbase
      val processor = LocalProcessor(schema, hints, None)
      empty(processor.reducer).getOrElse {
        val scans = configureScans(tables, ranges, small, colFamily, Seq.empty, coprocessor = false)
        LocalProcessorScanPlan(ds, strategy, ranges, scans, ecql, processor, hints.getProjection)
      }
    } else {
      val ecql = strategy.ecql
      val (colFamily, schema) = groups.group(index.sft, hints.getTransformDefinition, ecql)
      val projection = hints.getProjection
      val indexFilter = strategy.values.flatMap(IndexFilters(index, _))
      lazy val returnSchema = hints.getTransformSchema.getOrElse(schema)
      lazy val scans = {
        val transform = hints.getTransform
        val cqlFilter = if (ecql.isEmpty && transform.isEmpty && hints.getSampling.isEmpty) { Seq.empty } else {
          Seq((CqlTransformFilter.Priority, CqlTransformFilter(schema, index, ecql, transform, hints)))
        }
        val filters = (cqlFilter ++ indexFilter).sortBy(_._1).map(_._2)
        configureScans(tables, ranges, small, colFamily, filters, coprocessor = false)
      }

      def coprocessorPlan(options: Map[String, String], toFeatures: ResultsToFeatures[Array[Byte]], reducer: Option[FeatureReducer]): CoprocessorPlan = {
        val coprocessorOptions = Map(GeoMesaCoprocessor.YieldOpt -> String.valueOf(ds.config.coprocessors.yieldPartialResults))
        val scans = configureScans(tables, ranges, small, colFamily, indexFilter.toSeq.map(_._2), coprocessor = true)
        CoprocessorPlan(ds, strategy, ranges, scans, options ++ coprocessorOptions, toFeatures, reducer, hints.getMaxFeatures, projection)
      }

      def semiLocalPlan(): LocalProcessorScanPlan = {
        // note: transforms are handled in the cqlTransformFilter
        val processor = LocalProcessor(returnSchema, QueryHints.Internal.clearTransforms(hints), None)
        LocalProcessorScanPlan(ds, strategy, ranges, scans, None, processor, projection)
      }

      if (hints.isDensityQuery) {
        empty(None).getOrElse {
          if (ds.config.coprocessors.enabled.density) {
            val options = HBaseDensityAggregator.configure(schema, index, ecql, hints)
            val results = new HBaseDensityResultsToFeatures()
            coprocessorPlan(options, results, None)
          } else {
            semiLocalPlan()
          }
        }
      } else if (hints.isArrowQuery) {
        val config = HBaseArrowAggregator.configure(schema, index, ds.stats, strategy.filter.filter, ecql, hints)
        val reducer = Some(config.reduce)
        empty(reducer).getOrElse {
          if (ds.config.coprocessors.enabled.arrow) {
            val options = config.config
            val results = new HBaseArrowResultsToFeatures()
            coprocessorPlan(options, results, reducer)
          } else {
            semiLocalPlan()
          }
        }
      } else if (hints.isStatsQuery) {
        val reducer = Some(StatsScan.StatsReducer(returnSchema, hints))
        empty(reducer).getOrElse {
          if (ds.config.coprocessors.enabled.stats) {
            val options = HBaseStatsAggregator.configure(schema, index, ecql, hints)
            val results = new HBaseStatsResultsToFeatures()
            coprocessorPlan(options, results, reducer)
          } else {
            semiLocalPlan()
          }
        }
      } else if (hints.isBinQuery) {
        empty(None).getOrElse {
          if (ds.config.coprocessors.enabled.bin) {
            val options = HBaseBinAggregator.configure(schema, index, ecql, hints)
            val results = new HBaseBinResultsToFeatures()
            coprocessorPlan(options, results, None)
          } else {
            semiLocalPlan()
          }
        }
      } else {
        empty(None).getOrElse {
          val resultsToFeatures = new HBaseResultsToFeatures(index, returnSchema)
          ScanPlan(ds, strategy, ranges, scans, resultsToFeatures, hints.getSortFields, hints.getMaxFeatures, projection)
        }
      }
    }
  }

  override def createWriter(
      sft: SimpleFeatureType,
      indices: Seq[GeoMesaFeatureIndex[_, _]],
      partition: Option[String],
      atomic: Boolean): HBaseIndexWriter = {
    require(!atomic, "HBase data store does not currently support atomic writes")
    val wrapper = WritableFeature.wrapper(sft, groups)
    if (sft.isVisibilityRequired) {
      new HBaseIndexWriter(ds, indices, wrapper, partition) with RequiredVisibilityWriter
    } else {
      new HBaseIndexWriter(ds, indices, wrapper, partition)
    }
  }

  override def getStrategyCost(strategy: FilterStrategy, explain: Explainer): Option[Long] =
    ds.stats.getCount(strategy.index.sft, strategy.primary.getOrElse(Filter.INCLUDE))

  /**
   * Configure the hbase scan
   *
   * @param tables tables being scanned, used for region location information
   * @param ranges ranges to scan, non-empty. needs to be mutable as we will sort it in place
   * @param small whether 'small' ranges (i.e. gets)
   * @param colFamily col family to scan
   * @param filters scan filters
   * @param coprocessor is this a coprocessor scan or not
   * @return
   */
  private def configureScans(
      tables: Seq[TableName],
      ranges: Seq[RowRange],
      small: Boolean,
      colFamily: Array[Byte],
      filters: Seq[HFilter],
      coprocessor: Boolean): Seq[TableScan] = {
    val cacheBlocks = HBaseSystemProperties.ScannerBlockCaching.toBoolean.get // has a default value so .get is safe
    val cacheSize = HBaseSystemProperties.ScannerCaching.toInt

    logger.debug(s"HBase client scanner: block caching: $cacheBlocks, caching: $cacheSize")

    if (small && !coprocessor) {
      val filter = filters match {
        case Nil    => None
        case Seq(f) => Some(f)
        case f      => Some(new FilterList(f: _*))
      }
      // note: we have to copy the ranges for each table scan
      tables.map { table =>
        val scans = ranges.map { r =>
          val scan = new Scan().withStartRow(r.getStartRow).withStopRow(r.getStopRow)
          scan.addFamily(colFamily).setCacheBlocks(cacheBlocks).setSmall(true)
          filter.foreach(scan.setFilter)
          cacheSize.foreach(scan.setCaching)
          ds.applySecurity(scan)
          scan
        }
        TableScan(table, scans)
      }
    } else {
      // split and group ranges by region server
      // note: we have to copy the ranges for each table scan anyway
      val rangesPerTable: Seq[(TableName, collection.Map[String, java.util.List[RowRange]])] =
        tables.map(t => t -> groupRangesByRegion(t, ranges))

      def createGroup(group: java.util.List[RowRange]): Scan = {
        val scan = new Scan().withStartRow(group.get(0).getStartRow).withStopRow(group.get(group.size() - 1).getStopRow)
        val mrrf = if (group.size() < 2) { filters } else {
          // TODO GEOMESA-1806
          // currently, the MultiRowRangeFilter constructor will call sortAndMerge a second time
          // this is unnecessary as we have already sorted and merged
          // note: mrrf first priority
          filters.+:(new MultiRowRangeFilter(group))
        }
        scan.setFilter(if (mrrf.lengthCompare(1) > 0) { new FilterList(mrrf: _*) } else { mrrf.headOption.orNull })
        scan.addFamily(colFamily).setCacheBlocks(cacheBlocks)
        cacheSize.foreach(scan.setCaching)

        // apply visibilities
        ds.applySecurity(scan)

        scan
      }

      rangesPerTable.map { case (table, rangesPerRegion) =>
        val maxRangesPerGroup = {
          def calcMax(maxPerGroup: Int, threads: Int): Int = {
            val totalRanges = rangesPerRegion.values.map(_.size).sum
            math.min(maxPerGroup, math.max(1, math.ceil(totalRanges.toDouble / threads).toInt))
          }
          if (coprocessor) {
            calcMax(ds.config.coprocessors.maxRangesPerExtendedScan, ds.config.coprocessors.threads)
          } else {
            calcMax(ds.config.queries.maxRangesPerExtendedScan, ds.config.queries.threads)
          }
        }

        val groupedScans = Seq.newBuilder[Scan]

        rangesPerRegion.foreach { case (_, list) =>
          // our ranges are non-overlapping, so just sort them but don't bother merging them
          Collections.sort(list)

          var i = 0
          while (i < list.size()) {
            val groupSize = math.min(maxRangesPerGroup, list.size() - i)
            groupedScans += createGroup(list.subList(i, i + groupSize))
            i += groupSize
          }
        }

        // shuffle the ranges, otherwise our threads will tend to all hit the same region server at once
        TableScan(table, Random.shuffle(groupedScans.result))
      }
    }
  }

  /**
   * Split and group ranges by region server
   *
   * @param table table being scanned
   * @param ranges ranges to group
   * @return
   */
  private def groupRangesByRegion(
      table: TableName,
      ranges: Seq[RowRange]): scala.collection.Map[String, java.util.List[RowRange]] = {
    val rangesPerRegion = scala.collection.mutable.Map.empty[String, java.util.List[RowRange]]
    WithClose(ds.connection.getRegionLocator(table)) { locator =>
      ranges.foreach(groupRange(locator, _, rangesPerRegion))
    }
    rangesPerRegion
  }

  /**
   * Group the range based on the region server hosting it. Splits ranges as needed if they span
   * more than one region
   *
   * @param locator region locator
   * @param range range to group
   * @param result collected results
   */
  @scala.annotation.tailrec
  private def groupRange(
      locator: RegionLocator,
      range: RowRange,
      result: scala.collection.mutable.Map[String, java.util.List[RowRange]]): Unit = {
    var encodedName: String = null
    var split: Array[Byte] = null
    try {
      val regionInfo = locator.getRegionLocation(range.getStartRow).getRegionInfo
      encodedName = regionInfo.getEncodedName
      val regionEndKey = regionInfo.getEndKey // note: this is exclusive
      if (regionEndKey.nonEmpty &&
          (range.getStopRow.isEmpty || ByteArrays.ByteOrdering.compare(regionEndKey, range.getStopRow) <= 0)) {
        if (ByteArrays.ByteOrdering.compare(range.getStartRow, regionEndKey) < 0) {
          split = regionEndKey
        } else {
          logger.warn(s"HBase region location does not correspond to requested range:\n" +
              s"  requested row: ${ByteArrays.toHex(range.getStartRow)}\n" +
              s"  region: $encodedName ${ByteArrays.toHex(regionInfo.getStartKey)} :: ${ByteArrays.toHex(regionEndKey)}")
        }
      }
    } catch {
      case NonFatal(e) => logger.warn(s"Error checking range location for '$range''", e)
    }
    val buffer = result.getOrElseUpdate(encodedName, new java.util.ArrayList())
    if (split == null) {
      buffer.add(range)
    } else {
      // split the range based on the current region
      buffer.add(new RowRange(range.getStartRow, true, split, false))
      groupRange(locator, new RowRange(split, true, range.getStopRow, false), result)
    }
  }
}

object HBaseIndexAdapter extends LazyLogging {

  import scala.collection.JavaConverters._

  private val distributedJarNamePattern = Pattern.compile("^geomesa-hbase-distributed-runtime.*\\.jar$")

  // these are in the geomesa-hbase-server module, so not accessible directly
  val CoprocessorClass = "org.locationtech.geomesa.hbase.server.coprocessor.GeoMesaCoprocessor"
  val AggregatorPackage = "org.locationtech.geomesa.hbase.server.common"

  val durability: Durability = HBaseSystemProperties.WalDurability.option match {
    case Some(value) =>
      Durability.values.find(_.toString.equalsIgnoreCase(value)).getOrElse {
        logger.error(s"Invalid HBase WAL durability setting: $value. Falling back to default durability")
        Durability.USE_DEFAULT
      }
    case None => Durability.USE_DEFAULT
  }

  /**
   * Create a new table
   *
   * @param admin admin connection to hbase
   * @param name table name
   * @param colFamilies column families
   * @param bloom bloom filter
   * @param compression compression
   * @param encoding data block encoding
   * @param coprocessor coprocessor class and optional jar path
   * @param splits initial table splits (empty for no splits)
   */
  def createTableAsync(
      admin: Admin,
      name: TableName,
      colFamilies: Seq[Array[Byte]],
      bloom: Option[BloomType],
      compression: Option[Algorithm],
      encoding: Option[DataBlockEncoding],
      inMemory: Option[Boolean],
      coprocessor: Option[(String, Option[Path])],
      splits: Seq[Array[Byte]],
      metadata: Map[String, String],
    ): Unit = {

    val namespace = name.getNamespaceAsString
    if (namespace != null &&
        namespace != NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR &&
        namespace != NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR &&
        Try(Option(admin.getNamespaceDescriptor(namespace))).getOrElse(None).isEmpty) {
      admin.createNamespace(NamespaceDescriptor.create(namespace).build())
    }

    val builder = new TableDescriptorBuilder()
    val columnFamilyDescriptors = colFamilies.map { colFamily =>
      val builder = ColumnFamilyDescriptorBuilder.newBuilder(colFamily)
      bloom.foreach(builder.setBloomFilterType)
      compression.foreach(builder.setCompressionType)
      encoding.foreach(builder.setDataBlockEncoding)
      inMemory.foreach(builder.setInMemory)
      builder.build()
    }
    builder.setColumnFamilies(columnFamilyDescriptors.asJava)
    coprocessor.foreach { case (clas, path) =>
      val descriptor =
        CoprocessorDescriptorBuilder.newBuilder(clas)
          .setPriority(Coprocessor.PRIORITY_USER)
          .setJarPath(path.fold[String](null)(_.toString))
          .build()
      builder.setCoprocessor(descriptor)
    }
    metadata.foreach { case (k, v) => builder.setValue(k, v) }
    admin.createTableAsync(builder.build(), splits.toArray)
  }

  /**
   * Waits for a table to come online after being created
   *
   * @param admin hbase admin
   * @param table table name
   */
  def waitForTable(admin: Admin, table: TableName): Unit = {
    if (!admin.isTableAvailable(table)) {
      val timeout = TableAvailabilityTimeout.toUnboundedDuration.filter(_.isFinite)
      logger.debug(s"Waiting for table '$table' to become available with " +
          s"${timeout.map(t => s"a timeout of $t").getOrElse("no timeout")}")
      val stop = timeout.map(t => System.currentTimeMillis() + t.toMillis)
      while (!admin.isTableAvailable(table) && stop.forall(_ > System.currentTimeMillis())) {
        Thread.sleep(1000)
      }
    }
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

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    private val batchSize = HBaseSystemProperties.WriteBatchSize.toLong
    private val flushTimeout = HBaseSystemProperties.WriteFlushTimeout.toLong
    private val deleteVis = HBaseSystemProperties.DeleteVis.option.map(new CellVisibility(_))

    private val pools = {
      // mimic config from default hbase connection
      val maxThreads = math.max(1, ds.connection.getConfiguration.getInt("hbase.htable.threads.max", Int.MaxValue))
      Array.fill(indices.length)(new CachedThreadPool(maxThreads))
    }

    private val mutators = indices.toArray.map { index =>
      // should always be writing to a single table here
      val table = index.getTableName(partition)
      val params = new BufferedMutatorParams(TableName.valueOf(table))
      batchSize.foreach(params.writeBufferSize)
      flushTimeout.foreach(params.setWriteBufferPeriodicFlushTimeoutMs)

      // We have to pass a pool explicitly and close it after manually,
      // cause of HBase issue where pools got leaked and never closed
      // (in case of long running Spark jobs 24+ hours the workers go out of memory without custom pool)
      params.pool(pools(indices.indexOf(index)))
      ds.connection.getBufferedMutator(params)
    }

    private val expiration = indices.headOption.flatMap(_.sft.getFeatureExpiration).orNull

    private var i = 0

    override protected def append(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
      val ttl = if (expiration != null) {
        val t = expiration.expires(feature.feature) - System.currentTimeMillis
        if (t > 0) {
          t
        }
        else {
          logger.warn("Feature is already past its TTL; not added to database")
          return
        }
      } else {
        0L
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
              if (ttl > 0) put.setTTL(ttl)
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
                if (ttl > 0) put.setTTL(ttl)
                mutator.mutate(put)
              }
            }
        }
        i += 1
      }
    }

    override protected def update(
        feature: WritableFeature,
        values: Array[RowKeyValue[_]],
        previous: WritableFeature,
        previousValues: Array[RowKeyValue[_]]): Unit = {
      delete(previous, previousValues)
      // for updates, ensure that our timestamps don't clobber each other
      flush()
      Thread.sleep(1)
      append(feature, values)
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
              } else {
                deleteVis.foreach(del.setCellVisibility)
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
                } else {
                  deleteVis.foreach(del.setCellVisibility)
                }
                mutator.mutate(del)
              }
            }
        }
        i += 1
      }
    }

    override def flush(): Unit = FlushWithLogging.raise(mutators)(BufferedMutatorIsFlushable.arrayIsFlushable)

    override def close(): Unit = {
      try { CloseWithLogging.raise(mutators) } finally {
        CloseWithLogging(pools)
      }
      if (!pools.foldLeft(true) { case (terminated, pool) => terminated && pool.awaitTermination(60, TimeUnit.SECONDS) }) {
        logger.warn("Failed to terminate thread pool after 60 seconds")
      }
    }
  }

  object BufferedMutatorIsFlushable extends IsFlushableImplicits[BufferedMutator] {
    override protected def flush(f: BufferedMutator): Try[Unit] = Try(f.flush())
  }
}
