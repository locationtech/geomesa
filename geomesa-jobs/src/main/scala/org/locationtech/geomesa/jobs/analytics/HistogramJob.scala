/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.analytics

import com.twitter.algebird.Aggregator
import com.twitter.scalding._
import com.twitter.scalding.typed.UnsortedGrouped
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.jobs.GeoMesaBaseJob
import org.locationtech.geomesa.jobs.GeoMesaBaseJob.RichArgs
import org.locationtech.geomesa.jobs.analytics.HistogramJob._
import org.locationtech.geomesa.jobs.scalding.ConnectionParams._
import org.locationtech.geomesa.jobs.scalding._
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._
import scala.math.Ordering

/**
 * Job that will histogram an attribute. Values can be grouped by up to 8 other attributes. Values can also
 * be filtered based on a regex.
 */
class HistogramJob(args: Args) extends GeoMesaBaseJob(args) {

  val feature  = args(FEATURE_IN)
  val dsParams = toDataStoreInParams(args)
  val filter   = args.optional(CQL_IN)

  val attribute  = args(ATTRIBUTE)
  val groupBy    = args.nonStrictList(GROUP_BY)
  val uniqueBy   = args.nonStrictList(UNIQUE_BY)
  val transforms = Option(args.list(TRANSFORM_IN).toArray).filter(_.nonEmpty)
  val writeToAccumulo  = args.boolean(WRITE_TO_ACCUMULO)

  val input = GeoMesaInputOptions(dsParams, feature, filter, transforms)
  lazy val dsParamsAccOutput = dsParams.updated("tableName", s"${dsParams.get("tableName").get}_stats")
  lazy val accOutput = AccumuloOutputOptions(dsParamsAccOutput)

  val output = args(FILE_OUT)

  // TODO we could look up attribute values by index for performance gain
  // verify input params - inside a block so they don't get serialized
  {
    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]
    val sft = ds.getSchema(feature)
    assert(sft != null, s"The feature '$feature' does not exist in the input data store")
    val descriptors = sft.getAttributeDescriptors.map(_.getLocalName)
    // consider attributes available to group on - include derived attributes from transforms if any
    val available: Seq[String] = transforms match {
      case Some(transform) => descriptors ++ transform.map(_.split("=")(0).trim)
      case None            => descriptors
    }
    (List(attribute) ++ groupBy ++ uniqueBy).foreach { a =>
      assert(available.contains(a),  s"Attribute '$a' does not exist in feature $feature")
    }
    assert(groupBy.length + uniqueBy.length < 9, "Can't group by + unique by more than 8 attributes")
  }

  // groups by attributes - we convert the list of attributes to a tuple to get implicit grouping
  // the return type is Product, since that's the only common subclass of TupleN
  val groupByOrdering = getOrdering(groupBy.length + 1)
  def groupByAttributes(sf: SimpleFeature): Product =
    (groupBy.map(sf.safeString) ++ List(sf.safeString(attribute))).toTuple

  // unique values per group
  val uniqueByOrdering = getOrdering(groupBy.length + uniqueBy.length + 1)
  def uniqueByAttributes(sf: SimpleFeature): Product =
    (groupBy.map(sf.safeString) ++ uniqueBy.map(sf.safeString) ++ List(sf.safeString(attribute))).toTuple

  // raw pipe of our input features
  val features: TypedPipe[SimpleFeature] = TypedPipe.from(GeoMesaSource(input)).values

  // unique by if we have one - unique is calculated on each grouping/attribute
  val unique: TypedPipe[SimpleFeature] = if (uniqueBy.isEmpty) { features } else {
    // use groupby and take the head of each group - distinctBy doesn't seem to work...
    features.groupBy(uniqueByAttributes)(uniqueByOrdering).mapValueStream(i => Seq(i.next()).iterator).values
  }

  // pipe grouped by groups and attribute
  val groups: Grouped[Product, SimpleFeature] = unique.groupBy(groupByAttributes)(groupByOrdering)

  // aggregate to get the histogram count
  val aggregates: UnsortedGrouped[Product, Long] = groups.aggregate(Aggregator.size)

  // write out the histogram to a tsv file - we create the tabs ourselves since length isn't known
  if (writeToAccumulo) {
    aggregates.toTypedPipe
      .map{ case (group, count) =>
      val mut = new Mutation(new Text(s"$feature~$attribute~${group.productElement(group.productArity - 1).toString}"))
      mut.put(new Text(s"${group.productIterator.mkString("\t")}"), new Text(count.toString.getBytes), EMPTY_VALUE)
      (null: Text, mut)
    }.write(AccumuloSource(accOutput))
  } else {
    aggregates.toTypedPipe.map { case (group, count) => s"${group.productIterator.mkString("\t")}\t$count" }
      .write(TypedTsv[String](output))
  }
}

object HistogramJob {

  val ATTRIBUTE         = "geomesa.hist.attribute"
  val GROUP_BY          = "geomesa.hist.group.attributes"
  val UNIQUE_BY         = "geomesa.hist.unique.attributes"
  val FILE_OUT          = "geomesa.hist.file.out"
  val WRITE_TO_ACCUMULO = "geomesa.hist.write.to.accumulo"

  implicit class RichList[A <: Object](val l: List[A]) extends AnyVal {

    /**
     * Converts a list to a TupleN based on the length of the list. Won't work for lists greater than max
     * tuple size (currently 22).
     */
    def toTuple: Product =
      Class.forName("scala.Tuple" + l.size).getConstructors.apply(0).newInstance(l: _*).asInstanceOf[Product]
  }

  implicit class RichAttribute(val sf: SimpleFeature) extends AnyVal {

    /**
     * Gets a valid string for an attribute - 'null' as a string if the attribute is null
     */
    def safeString(attribute: String): String =
      Option(sf.getAttribute(attribute)).map(_.toString).filter(!_.isEmpty).getOrElse("null")
  }

  // since we grouped into a Product, we have to define an ordering for it - we know that under the covers
  // the product is a tuple, so use the implicit orderings already defined
  // we are ordering on groups + attribute - we should never have more than 9 due to check above
  def getOrdering(length: Int): Ordering[Product] = {
    val ordering = length match {
      case 1 => new Ordering[Tuple1[String]] {
        def compare(x: Tuple1[String], y: Tuple1[String]): Int = Ordering.String.compare(x._1, y._1)
      }
      case 2 => Ordering.Tuple2[String, String]
      case 3 => Ordering.Tuple3[String, String, String]
      case 4 => Ordering.Tuple4[String, String, String, String]
      case 5 => Ordering.Tuple5[String, String, String, String, String]
      case 6 => Ordering.Tuple6[String, String, String, String, String, String]
      case 7 => Ordering.Tuple7[String, String, String, String, String, String, String]
      case 8 => Ordering.Tuple8[String, String, String, String, String, String, String, String]
      case 9 => Ordering.Tuple9[String, String, String, String, String, String, String, String, String]
    }
    ordering.asInstanceOf[Ordering[Product]]
  }

  def runJob(conf: Configuration,
             dsParams: Map[String, String],
             feature: String,
             attribute: String,
             fileOut: String,
             groupBy: List[String]    = List.empty,
             uniqueBy: List[String]   = List.empty,
             writeToAccumulo: Boolean = false) = {
    val args = Seq(FEATURE_IN        -> List(feature),
                   ATTRIBUTE         -> List(attribute),
                   GROUP_BY          -> groupBy,
                   UNIQUE_BY         -> uniqueBy,
                   FILE_OUT          -> List(fileOut),
                   WRITE_TO_ACCUMULO -> List(writeToAccumulo.toString)).toMap ++ toInArgs(dsParams)
    val instantiateJob = (args: Args) => new HistogramJob(args)
    GeoMesaBaseJob.runJob(conf, args, instantiateJob)
  }
}
