/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

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
  def propertyNames(sft: SimpleFeatureType, stat: String, report: Boolean = true): Seq[String] = {
    if (stat == null) {
      throw new IllegalArgumentException("Stat must not be null")
    }
    val runner = if (report) { ReportingParseRunner(Parser.attributes) } else { BasicParseRunner(Parser.attributes) }
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

  private def sft: SimpleFeatureType = sfts.get
}

private class StatParser extends BasicParser {

  import StatParser.sft

  // main parsing rule
  def stat: Rule1[Stat] = rule { stats ~ EOI }

  def attributes: Rule1[Seq[String]] = rule { properties ~ EOI  ~~> { p => p.distinct} }

  private def stats: Rule1[Stat] = rule {
    oneOrMore(singleStat, ";") ~~> { s => if (s.lengthCompare(1) == 0) { s.head } else { new SeqStat(sft, s) }}
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
    "GroupBy(" ~ attribute ~ "," ~ (stats ~> { s => s }) ~ ")" ~~> { (attribute, _, groupedStats) =>
      new GroupBy(sft, attribute, groupedStats)
    }
  }

  private def groupByNames: Rule1[Seq[String]] = rule {
    "GroupBy(" ~ attribute ~ "," ~ properties ~ ")" ~~> { (attribute, groupedStats) =>
      groupedStats.+:(attribute)
    }
  }

  private def count: Rule1[Stat] = rule {
    "Count()" ~> { _ => new CountStat(sft) }
  }

  private def countNames: Rule1[Seq[String]] = rule {
    "Count()" ~> { _ => Seq.empty }
  }

  private def minMax: Rule1[Stat] = rule {
    "MinMax(" ~ attribute ~ ")" ~~> { attribute =>
      val binding = sft.getDescriptor(attribute).getType.getBinding
      new MinMax[Any](sft, attribute)(MinMaxDefaults(binding))
    }
  }

  private def minMaxNames: Rule1[Seq[String]] = rule {
    "MinMax(" ~ attribute ~ ")" ~~> { attribute => Seq(attribute) }
  }

  private def iteratorStack: Rule1[Stat] = rule {
    "IteratorStackCount()" ~> { _ => new IteratorStackCount(sft) }
  }

  private def iteratorStackNames: Rule1[Seq[String]] = rule {
    "IteratorStackCount()" ~> { _ => Seq.empty }
  }

  private def enumeration: Rule1[Stat] = rule {
    "Enumeration(" ~ attribute ~ ")" ~~> { attribute =>
      val binding = sft.getDescriptor(attribute).getType.getBinding
      new EnumerationStat[Any](sft, attribute)(ClassTag(binding))
    }
  }

  private def enumerationNames: Rule1[Seq[String]] = rule {
    "Enumeration(" ~ attribute ~ ")" ~~> { attribute => Seq(attribute) }
  }

  private def topK: Rule1[Stat] = rule {
    "TopK(" ~ attribute ~ ")" ~~> { attribute =>
      new TopK[Any](sft, attribute)
    }
  }

  private def topKNames: Rule1[Seq[String]] = rule {
    "TopK(" ~ attribute ~ ")" ~~> { attribute => Seq(attribute) }
  }

  private def descriptiveStats: Rule1[Stat] = rule {
    "DescriptiveStats(" ~ oneOrMore(attribute, ",") ~ ")" ~~> { attributes =>
      new DescriptiveStats(sft, attributes)
    }
  }

  private def descriptiveStatsNames: Rule1[Seq[String]] = rule {
    "DescriptiveStats(" ~ oneOrMore(attribute, ",") ~ ")" ~~> { attributes => attributes }
  }

  private def histogram: Rule1[Stat] = rule {
    "Histogram(" ~ attribute ~ "," ~ int ~ "," ~ string ~ "," ~ string ~ ")" ~~> {
      (attribute, numBins, lower, upper) => {
        val binding = sft.getDescriptor(attribute).getType.getBinding
        val destringify = Stat.destringifier(binding)
        val tLower = destringify(lower)
        val tUpper = destringify(upper)
        new Histogram[Any](sft, attribute, numBins, (tLower, tUpper))(MinMaxDefaults(binding), ClassTag(binding))
      }
    }
  }

  private def histogramNames: Rule1[Seq[String]] = rule {
    "Histogram(" ~ attribute ~ "," ~ int ~ "," ~ string ~ "," ~ string ~ ")" ~~> {
      (attribute, _, _, _) => Seq(attribute)
    }
  }

  private def frequency: Rule1[Stat] = rule {
    "Frequency(" ~ attribute ~ "," ~ optional(attribute ~ "," ~ timePeriod ~ ",") ~ int ~ ")" ~~> {
      (attribute, dtgAndPeriod, precision) => {
        val dtg = dtgAndPeriod.map(_._1)
        val period = dtgAndPeriod.map(_._2).getOrElse(TimePeriod.Week)
        val binding = sft.getDescriptor(attribute).getType.getBinding
        new Frequency[Any](sft, attribute, dtg, period, precision)(ClassTag(binding))
      }
    }
  }

  private def frequencyNames: Rule1[Seq[String]] = rule {
    "Frequency(" ~ attribute ~ "," ~ optional(attribute ~ "," ~ timePeriod ~ ",") ~ int ~ ")" ~~> {
      (attribute, dtgAndPeriod, _) => Seq(attribute) ++ dtgAndPeriod.map(_._1).toSeq
    }
  }

  private def z3Histogram: Rule1[Stat] = rule {
    "Z3Histogram(" ~ attribute ~ "," ~ attribute ~ "," ~ timePeriod ~ "," ~ int ~ ")" ~~> {
      (geom, dtg, period, length) => new Z3Histogram(sft, geom, dtg, period, length)
    }
  }

  private def z3HistogramNames: Rule1[Seq[String]] = rule {
    "Z3Histogram(" ~ attribute ~ "," ~ attribute ~ "," ~ timePeriod ~ "," ~ int ~ ")" ~~> {
      (geom, dtg, _, _) => Seq(geom, dtg)
    }
  }

  private def z3Frequency: Rule1[Stat] = rule {
    "Z3Frequency(" ~ attribute ~ "," ~ attribute ~ "," ~ timePeriod ~ "," ~ int ~ ")" ~~> {
      (geom, dtg, period, precision) => new Z3Frequency(sft, geom, dtg, period, precision)
    }
  }

  private def z3FrequencyNames: Rule1[Seq[String]] = rule {
    "Z3Frequency(" ~ attribute ~ "," ~ attribute ~ "," ~ timePeriod ~ "," ~ int ~ ")" ~~> {
      (geom, dtg, _, _) => Seq(geom, dtg)
    }
  }

  private def timePeriod: Rule1[TimePeriod] = rule {
    string ~~> { period => TimePeriod.withName(period.toLowerCase) }
  }

  private def attribute: Rule1[String] = rule {
    string ~~~? { s => sft.indexOf(s) != -1 }
  }
}
