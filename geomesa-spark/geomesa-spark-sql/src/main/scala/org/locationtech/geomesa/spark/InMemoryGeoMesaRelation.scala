package org.locationtech.geomesa.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.locationtech.geomesa.spark.SparkUtils._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

case class InMemoryGeoMesaRelation(sqlContext: SQLContext,
                                   sft: SimpleFeatureType,
                                   schema: StructType,
                                   params: Map[String, String],
                                   filt: org.opengis.filter.Filter = org.opengis.filter.Filter.INCLUDE,
                                   props: Option[Seq[String]] = None) extends BaseRelation with PrunedFilteredScan with LazyLogging {


  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    logger.debug(s"""Building scan, filt = $filt, filters = ${filters.mkString(",")}, requiredColumns = ${requiredColumns.mkString(",")}""")
    val compiledCQL = filters.flatMap(sparkFilterToCQLFilter).foldLeft[org.opengis.filter.Filter](filt) { (l, r) => ff.and(l,r) }
    logger.debug(s"compiledCQL = $compiledCQL")

    val requiredAttributes = requiredColumns.filterNot(_ == "__fid__")

    // TODO: Extract as buildExtractors()
    type EXTRACTOR = SimpleFeature => AnyRef
    val IdExtractor: SimpleFeature => AnyRef = sf => sf.getID

    // the SFT attributes do not have the __fid__ so we have to translate accordingly
    val extractors: Array[EXTRACTOR] = requiredColumns.map {
      case "__fid__" => IdExtractor
      case col       =>
        val index = requiredAttributes.indexOf(col)
        sf: SimpleFeature => toSparkType(sf.getAttribute(index))
    }


    val rdd: RDD[SimpleFeature] = ???

    val result = rdd.map(SparkUtils.sf2row(schema, _, extractors))
    result.asInstanceOf[RDD[Row]]
  }

  //override def sqlContext: SQLContext = ???

  //override def schema: StructType = ???
}
