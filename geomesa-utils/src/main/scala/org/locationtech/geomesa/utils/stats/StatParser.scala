/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import org.geotools.data.DataUtilities
import org.locationtech.geomesa.curve.TimePeriod
import org.locationtech.geomesa.curve.TimePeriod._
import org.locationtech.geomesa.utils.stats.MinMax.MinMaxDefaults
import org.locationtech.geomesa.utils.text.BasicParser
import org.opengis.feature.simple.SimpleFeatureType
import org.parboiled.errors.{ErrorUtils, ParsingException}
import org.parboiled.scala._
import org.parboiled.scala.rules.Rule1

import scala.reflect.ClassTag

object StatParser {

  private val Parser = new StatParser()

  private val sfts = new ThreadLocal[SimpleFeatureType]

  @throws(classOf[ParsingException])
  def parse(sft: SimpleFeatureType, stat: String, report: Boolean = true): Stat = {
    if (stat == null) {
      throw new IllegalArgumentException("Stat must not be null")
    }
    val runner = if (report) { ReportingParseRunner(Parser.stat) } else { BasicParseRunner(Parser.stat) }
    sfts.set(sft)
    try {
      val parsing = runner.run(stat)
      parsing.result.getOrElse {
        throw new ParsingException(s"Invalid stat string: ${ErrorUtils.printParseErrors(parsing)}")
      }
    } finally {
      sfts.remove()
    }
  }

  @throws(classOf[ParsingException])
  def propertyNames(stat: String, report: Boolean = true): Seq[String] = {
    if (stat == null) {
      throw new IllegalArgumentException("Stat must not be null")
    }
    val runner = if (report) { ReportingParseRunner(Parser.attributes) } else { BasicParseRunner(Parser.attributes) }
    val parsing = runner.run(stat)
    parsing.result.getOrElse {
      throw new ParsingException(s"Invalid stat string: ${ErrorUtils.printParseErrors(parsing)}")
    }
  }

  /**
    * Obtains the index of the attribute within the SFT
    *
    * @param attribute attribute name as a string
    * @return attribute index
    */
  private def getIndex(attribute: String): Int = {
    val i = sft.indexOf(attribute)
    require(i != -1, s"Attribute '$attribute' does not exist in sft ${DataUtilities.encodeType(sft)}")
    i
  }

  private def sft: SimpleFeatureType = sfts.get
}

private class StatParser extends BasicParser {

  import StatParser.{getIndex, sft}

  // main parsing rule
  def stat: Rule1[Stat] = rule { stats ~ EOI }

  def attributes: Rule1[Seq[String]] = rule { properties ~ EOI  ~~> { p => p.distinct} }

  private def stats: Rule1[Stat] = rule {
    oneOrMore(singleStat, ";") ~~> { s => if (s.length == 1) s.head else new SeqStat(s) }
  }

  private def properties: Rule1[Seq[String]] = rule {
    oneOrMore(names, ";") ~~> { s => s.flatten }
  }

  private def singleStat: Rule1[Stat] = rule {
    count | minMax | groupBy | descriptiveStats | enumeration | topK | histogram |
        frequency | z3Histogram | z3Frequency | iteratorStack
  }

  private def names: Rule1[Seq[String]] = rule {
    countNames | minMaxNames | groupByNames | descriptiveStatsNames | enumerationNames |
        topKNames | histogramNames | frequencyNames | z3HistogramNames | z3FrequencyNames | iteratorStackNames
  }

  private def groupBy: Rule1[Stat] = rule {
    "GroupBy(" ~ string ~ "," ~ (stats ~> { s => s }) ~ ")" ~~> { (attribute, _, groupedStats) =>
      new GroupBy(getIndex(attribute), groupedStats, sft)
    }
  }

  private def groupByNames: Rule1[Seq[String]] = rule {
    "GroupBy(" ~ string ~ "," ~ properties ~ ")" ~~> { (attribute, groupedStats) =>
      groupedStats.+:(attribute)
    }
  }

  private def count: Rule1[Stat] = rule {
    "Count()" ~> { _ => new CountStat() }
  }

  private def countNames: Rule1[Seq[String]] = rule {
    "Count()" ~> { _ => Seq.empty }
  }

  private def minMax: Rule1[Stat] = rule {
    "MinMax(" ~ string ~ ")" ~~> { attribute =>
      val index = getIndex(attribute)
      val binding = sft.getDescriptor(attribute).getType.getBinding
      new MinMax[Any](index)(MinMaxDefaults(binding))
    }
  }

  private def minMaxNames: Rule1[Seq[String]] = rule {
    "MinMax(" ~ string ~ ")" ~~> { attribute => Seq(attribute) }
  }

  private def iteratorStack: Rule1[Stat] = rule {
    "IteratorStackCount()" ~> { _ => new IteratorStackCount() }
  }

  private def iteratorStackNames: Rule1[Seq[String]] = rule {
    "IteratorStackCount()" ~> { _ => Seq.empty }
  }

  private def enumeration: Rule1[Stat] = rule {
    "Enumeration(" ~ string ~ ")" ~~> { attribute =>
      val index = getIndex(attribute)
      val binding = sft.getDescriptor(attribute).getType.getBinding
      new EnumerationStat[Any](index)(ClassTag(binding))
    }
  }

  private def enumerationNames: Rule1[Seq[String]] = rule {
    "Enumeration(" ~ string ~ ")" ~~> { attribute => Seq(attribute) }
  }

  private def topK: Rule1[Stat] = rule {
    "TopK(" ~ string ~ ")" ~~> { attribute =>
      val index = getIndex(attribute)
      val binding = sft.getDescriptor(attribute).getType.getBinding
      new TopK[Any](index)
    }
  }

  private def topKNames: Rule1[Seq[String]] = rule {
    "TopK(" ~ string ~ ")" ~~> { attribute => Seq(attribute) }
  }

  private def descriptiveStats: Rule1[Stat] = rule {
    "DescriptiveStats(" ~ string ~ ")" ~~> { attributes =>
      val indices = attributes.split(",").map(getIndex)
      new DescriptiveStats(indices)
    }
  }

  private def descriptiveStatsNames: Rule1[Seq[String]] = rule {
    "DescriptiveStats(" ~ string ~ ")" ~~> { attributes => attributes.split(",").toSeq }
  }

  private def histogram: Rule1[Stat] = rule {
    "Histogram(" ~ string ~ "," ~ int ~ "," ~ string ~ "," ~ string ~ ")" ~~> {
      (attribute, numBins, lower, upper) => {
        val index = getIndex(attribute)
        val binding = sft.getDescriptor(attribute).getType.getBinding
        val destringify = Stat.destringifier(binding)
        val tLower = destringify(lower)
        val tUpper = destringify(upper)
        new Histogram[Any](index, numBins, (tLower, tUpper))(MinMaxDefaults(binding), ClassTag(binding))
      }
    }
  }

  private def histogramNames: Rule1[Seq[String]] = rule {
    "Histogram(" ~ string ~ "," ~ int ~ "," ~ string ~ "," ~ string ~ ")" ~~> {
      (attribute, _, _, _) => Seq(attribute)
    }
  }

  private def frequency: Rule1[Stat] = rule {
    "Frequency(" ~ string ~ "," ~ optional(string ~ "," ~ timePeriod ~ ",") ~ int ~ ")" ~~> {
      (attribute, dtgAndPeriod, precision) => {
        val index = getIndex(attribute)
        val dtgIndex = dtgAndPeriod.map(dap => getIndex(dap._1)).getOrElse(-1)
        val period = dtgAndPeriod.map(_._2).getOrElse(TimePeriod.Week)
        val binding = sft.getDescriptor(attribute).getType.getBinding
        new Frequency[Any](index, dtgIndex, period, precision)(ClassTag(binding))
      }
    }
  }

  private def frequencyNames: Rule1[Seq[String]] = rule {
    "Frequency(" ~ string ~ "," ~ optional(string ~ "," ~ timePeriod ~ ",") ~ int ~ ")" ~~> {
      (attribute, dtgAndPeriod, _) => Seq(attribute) ++ dtgAndPeriod.map(_._1).toSeq
    }
  }

  private def z3Histogram: Rule1[Stat] = rule {
    "Z3Histogram(" ~ string ~ "," ~ string ~ "," ~ timePeriod ~ "," ~ int ~ ")" ~~> {
      (geom, dtg, period, length) => new Z3Histogram(getIndex(geom), getIndex(dtg), period, length)
    }
  }

  private def z3HistogramNames: Rule1[Seq[String]] = rule {
    "Z3Histogram(" ~ string ~ "," ~ string ~ "," ~ timePeriod ~ "," ~ int ~ ")" ~~> {
      (geom, dtg, _, _) => Seq(geom, dtg)
    }
  }

  private def z3Frequency: Rule1[Stat] = rule {
    "Z3Frequency(" ~ string ~ "," ~ string ~ "," ~ timePeriod ~ "," ~ int ~ ")" ~~> {
      (geom, dtg, period, precision) => new Z3Frequency(getIndex(geom), getIndex(dtg), period, precision)
    }
  }

  private def z3FrequencyNames: Rule1[Seq[String]] = rule {
    "Z3Frequency(" ~ string ~ "," ~ string ~ "," ~ timePeriod ~ "," ~ int ~ ")" ~~> {
      (geom, dtg, _, _) => Seq(geom, dtg)
    }
  }

  private def timePeriod: Rule1[TimePeriod] = rule {
    string ~~> { period => TimePeriod.withName(period.toLowerCase) }
  }
}
