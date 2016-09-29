package org.locationtech.geomesa.spark

import java.util.Properties
import java.{lang => jl}

import com.vividsolutions.jts.geom.Point
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

class GeoMesaRelationProvider extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = ???

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = ???

  override def shortName(): String = ???
}

case class GeoMesaRelation(url: String,
                           table: String,
                           parts: Array[Partition],
                           properties: Properties = new Properties())
                          (@transient val sparkSession: SparkSession)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation {
  override def sqlContext: SQLContext = ???

  override def schema: StructType = ???

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = ???

  override def insert(data: DataFrame, overwrite: Boolean): Unit = ???
}

class GeoMesaSparkRDD(sc: SparkContext,
                      schema: StructType,
                      fqTable: String,
                      columns: Array[String],
                      filters: Array[Filter],
                      partitions: Array[Partition],
                      url: String,
                      properties: Properties)
  extends RDD[InternalRow](sc, Nil) {
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = ???

  override protected def getPartitions: Array[Partition] = ???
}

/**
  * Created by afox on 9/25/16.
  */
object GeoMesaSparkSQL {

  val PointType = StructType(Array(StructField(name = "g", dataType = DataTypes.BinaryType)))

  val typeMap: Map[Class[_], DataType] =
    Map(
        classOf[String]      -> StringType
      , classOf[jl.Integer]  -> IntegerType
      , classOf[jl.Long]     -> LongType
      , classOf[jl.Double]   -> DoubleType
      , classOf[jl.Float]    -> FloatType
      , classOf[Point]       -> BinaryType
    )


  implicit class SimpleFeatureSQLContext(val sql: SQLContext) extends AnyVal {
    def simpleFeatureRelation(sft: SimpleFeatureType) = new SimpleFeatureRelation(sft)(sql)
  }
}

class GeoMesaSparkSQLDataSource
  extends RelationProvider
    with SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    createRelation(sqlContext, parameters, null)

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    new SimpleFeatureRelation(SimpleFeatureTypes.createType(parameters("name"), parameters("spec")))(sqlContext)
  }
}

class SimpleFeatureRelation(sft: SimpleFeatureType)(@transient sql: SQLContext) extends BaseRelation with Serializable {
  import GeoMesaSparkSQL._

  import scala.collection.JavaConversions._

  override def sqlContext: SQLContext = sql

  override def schema: StructType = {
    val fields = sft.getAttributeDescriptors.map { ad =>
      val fieldType = typeMap(ad.getType.getBinding)
      StructField(name = ad.getLocalName, fieldType, nullable = true)
    }
    StructType(fields)
  }
}

