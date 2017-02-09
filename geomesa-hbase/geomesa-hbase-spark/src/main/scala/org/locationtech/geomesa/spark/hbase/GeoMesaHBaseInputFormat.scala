package org.locationtech.geomesa.spark.hbase

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.collections.map.CaseInsensitiveMap
import org.apache.commons.lang3.ArrayUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Mutation, Scan}
import org.apache.hadoop.hbase.mapreduce.{MultiTableInputFormat, TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.{CellScanner, HBaseConfiguration}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.{KryoFeatureSerializer, ProjectingKryoFeatureDeserializer}
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseFeature}
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.mapreduce.GeoMesaRecordReader
import org.locationtech.geomesa.utils.index.IndexMode
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
  * Input format that allows processing of simple features from GeoMesa based on a CQL query
  */
class GeoMesaHBaseInputFormat extends InputFormat[Text, SimpleFeature] with LazyLogging {

  val delegate = new MultiTableInputFormat

  var sft: SimpleFeatureType = _
  var table: GeoMesaFeatureIndex[HBaseDataStore, HBaseFeature, Mutation] = _

  private def init(conf: Configuration) = if (sft == null) {
    import scala.collection.JavaConversions._

    val params = GeoMesaConfigurator.getDataStoreInParams(conf)
    val ds = DataStoreFinder.getDataStore(new CaseInsensitiveMap(params).asInstanceOf[java.util.Map[_, _]]).asInstanceOf[HBaseDataStore]
    sft = ds.getSchema(GeoMesaConfigurator.getFeatureType(conf))
    val tableName = GeoMesaConfigurator.getTable(conf)
    table = HBaseFeatureIndex.indices(sft, IndexMode.Read)
      .find(t => t.getTableName(sft.getTypeName, ds) == tableName)
      .getOrElse(throw new RuntimeException(s"Couldn't find input table $tableName"))
    ds.dispose()

    delegate.setConf(conf)
    // see TableMapReduceUtil.java
    HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf))
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
  }


  /**
    * Gets splits for a job.
    *
    * Our delegated AccumuloInputFormat creates a split for each range - because we set a lot of ranges in
    * geomesa, that creates too many mappers. Instead, we try to group the ranges by tservers. We use the
    * number of shards in the schema as a proxy for number of tservers.
    */
  override def getSplits(context: JobContext): java.util.List[InputSplit] = {
    init(context.getConfiguration)
    val splits = delegate.getSplits(context)
    logger.debug(s"Got ${splits.size()} splits")
    splits
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, SimpleFeature] = {
    init(context.getConfiguration)
    val reader = new RecordReader[Array[Byte], Array[Byte]] {
      private val rr = delegate.createRecordReader(split, context)
      private var cs: CellScanner = _

      override def getProgress: Float = rr.getProgress

      private def initCS(): Boolean = {
        if(rr.nextKeyValue()) {
          cs = rr.getCurrentValue.cellScanner()
          cs.advance()
        } else {
          false
        }
      }

      override def nextKeyValue(): Boolean = {
        if(cs == null) {
          initCS()
        } else {
          if (!cs.advance()) {
            initCS()
          } else {
            false
          }
        }
      }

      override def getCurrentValue: Array[Byte] = {
        val cell = cs.current()
        val voffset = cell.getValueOffset
        val vlength = cell.getValueLength
        ArrayUtils.subarray(cell.getValueArray, voffset, voffset + vlength)
      }

      override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Unit = {
        rr.initialize(inputSplit, taskAttemptContext)
        if(cs == null) {
          initCS()
        }
      }

      override def getCurrentKey: Array[Byte] = rr.getCurrentKey.get()

      override def close(): Unit = rr.close()
    }


    val serializationOptions = SerializationOptions.withoutId
    val decoder =
      GeoMesaConfigurator.getTransformSchema(context.getConfiguration) match {
        case None         => new KryoFeatureSerializer(sft, serializationOptions)
        case Some(schema) => new ProjectingKryoFeatureDeserializer(sft, schema, serializationOptions)
      }
    new GeoMesaRecordReader(sft, table, reader, true, decoder)
  }
}
