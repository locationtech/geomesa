/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.jobs.analytics

import java.util.regex.Pattern

import com.twitter.algebird.Aggregator
import com.twitter.scalding._
import com.twitter.scalding.typed.UnsortedGrouped
import org.apache.accumulo.core.data.{Range => AcRange}
import org.apache.hadoop.conf.Configuration
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.jobs.GeoMesaBaseJob
import org.locationtech.geomesa.jobs.GeoMesaBaseJob.RichArgs
import org.locationtech.geomesa.jobs.analytics.HistogramJob._
import org.locationtech.geomesa.jobs.scalding.ConnectionParams._
import org.locationtech.geomesa.jobs.scalding._
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.ListSet
import scala.io.Source
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
  val transforms = Option(args.list(TRANSFORM_IN).toArray).filter(!_.isEmpty)
  val valueRegex = try { args.optional(VALUE_REGEX).map(_.r.pattern) } catch {
    case e: Exception => throw new IllegalArgumentException(s"Invalid regex ${args(VALUE_REGEX)}", e)
  }

  val input = GeoMesaInputOptions(dsParams, feature, filter, transforms)
  val output = args(FILE_OUT)

  // verify input params - inside a block so they don't get serialized
  {
    val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]
    val sft = ds.getSchema(feature)
    assert(sft != null, s"The feature '$feature' does not exist in the input data store")
    val descriptors = sft.getAttributeDescriptors.map(_.getLocalName)
    // consider attributes available to group on - include derived attributes from transforms if any
    val available: Seq[String] = transforms match {
      case Some(transform) => descriptors ++ transform.map(_.split("=")(0).trim)
      case None            => descriptors
    }
    assert(available.contains(attribute),  s"Attribute '$attribute' does not exist in feature $feature")
    groupBy.foreach { a =>
      assert(available.contains(a),  s"Attribute '$a' does not exist in feature $feature")
    }
    assert(groupBy.length < 9, "Can't group by more than 8 attributes")
  }

  // filters based on the regex arg
  def regexFilter(sf: SimpleFeature, p: Pattern): Boolean =
    Option(sf.getAttribute(attribute)).exists(v => p.matcher(v.toString).matches())

  // groups by attributes - we convert the list of attributes to a tuple to get implicit grouping
  // the return type is Product, since that's the only common subclass of TupleN
  def groupByAttributes(sf: SimpleFeature): Product = {
    val group = groupBy.map(a => Option(sf.getAttribute(a)).map(_.toString).filter(!_.isEmpty).getOrElse("null"))
    val attr = Option(sf.getAttribute(attribute)).map(_.toString).filter(!_.isEmpty).getOrElse("null")
    toTuple(group ++ List(attr))
  }

  // since we grouped into a Product, we have to define an ordering for it - we know that under the covers
  // the product is a tuple, so use the implicit orderings already defined
  // we are ordering on groups + attribute - we should never have more than 9 due to check above
  implicit val ordering: Ordering[Product] = (groupBy.length match {
    case 0 => new Ordering[Tuple1[String]] {
      def compare(x: Tuple1[String], y: Tuple1[String]): Int = Ordering.String.compare(x._1, y._1)
    }
    case 1 => Ordering.Tuple2[String, String]
    case 2 => Ordering.Tuple3[String, String, String]
    case 3 => Ordering.Tuple4[String, String, String, String]
    case 4 => Ordering.Tuple5[String, String, String, String, String]
    case 5 => Ordering.Tuple6[String, String, String, String, String, String]
    case 6 => Ordering.Tuple7[String, String, String, String, String, String, String]
    case 7 => Ordering.Tuple8[String, String, String, String, String, String, String, String]
    case 8 => Ordering.Tuple9[String, String, String, String, String, String, String, String, String]
  }).asInstanceOf[Ordering[Product]]

  // raw pipe of our input features
  val features: TypedPipe[SimpleFeature] = TypedPipe.from(GeoMesaSource(input)).values
  // pipe filtered by our regex, if we have one
  val filtered: TypedPipe[SimpleFeature] = valueRegex match {
    case Some(p) => features.filter(sf => regexFilter(sf, p))
    case None    => features
  }
  // pipe grouped by groups and attribute
  val groups: Grouped[Product, SimpleFeature] = filtered.groupBy(groupByAttributes)

  // aggregate to get the histogram count
  val aggregates: UnsortedGrouped[Product, Long] = groups.aggregate(Aggregator.size)

  // write out the histogram to a tsv file - we create the tabs ourselves since length isn't known
  aggregates.toTypedPipe.map { case (group, count) => s"${group.productIterator.mkString("\t")}\t$count" }
      .write(TypedTsv[String](output))
}

object HistogramJob {

  val ATTRIBUTE   = "geomesa.hist.attribute"
  val GROUP_BY    = "geomesa.hist.group.attributes"
  val VALUE_REGEX = "geomesa.hist.value.regex"
  val FILE_OUT    = "geomesa.hist.file.out"

  /**
   * Converts a list to a TupleN based on the length of the list. Won't work for lists greater than max
   * tuple size (currently 22).
   */
  def toTuple[A <: Object](as: List[A]): Product =
    Class.forName("scala.Tuple" + as.size).getConstructors.apply(0).newInstance(as: _*).asInstanceOf[Product]

  def runJob(conf: Configuration,
             dsParams: Map[String, String],
             feature: String,
             attribute: String,
             groupBy: List[String],
             valueRegex: Option[String]) = {
    val args = Seq(FEATURE_IN  -> List(feature),
                   ATTRIBUTE   -> List(attribute),
                   GROUP_BY    -> groupBy,
                   VALUE_REGEX -> valueRegex.toList).toMap ++ toInArgs(dsParams)
    val instantiateJob = (args: Args) => new HistogramJob(args)
    GeoMesaBaseJob.runJob(conf, args, instantiateJob)
  }
}
