/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.Date

import com.esotericsoftware.kryo.io.{Input, Output}
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.utils.cache.SoftThreadLocal
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.mutable.ArrayBuffer

/**
  * Serialize and deserialize stats
  */
trait StatSerializer {
  def serialize(stat: Stat): Array[Byte]
  def deserialize(bytes: Array[Byte]): Stat = deserialize(bytes, 0, bytes.length)
  def deserialize(bytes: Array[Byte], offset: Int, length: Int): Stat
}

object StatSerializer {
  def apply(sft: SimpleFeatureType): StatSerializer = new KryoStatSerializer(sft)
}

/**
  * Kryo implementation of stat serializer. Thread-safe.
  *
  * @param sft simple feature type
  */
class KryoStatSerializer(sft: SimpleFeatureType) extends StatSerializer {

  override def serialize(stat: Stat): Array[Byte] = {
    val output = KryoStatSerializer.outputs.getOrElseUpdate(new Output(1024, -1))
    output.clear()
    KryoStatSerializer.write(output, sft, stat)
    output.toBytes
  }

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): Stat = {
    val input = KryoStatSerializer.inputs.getOrElseUpdate(new Input)
    input.setBuffer(bytes, offset, length)
    KryoStatSerializer.read(input, sft)
  }
}

object KryoStatSerializer {

  private [stats] val inputs  = new SoftThreadLocal[Input]()
  private [stats] val outputs = new SoftThreadLocal[Output]()

  // bytes indicating the type of stat
  private [stats] val SeqStatByte: Byte        = '0'
  private [stats] val CountByte: Byte          = '1'
  private [stats] val MinMaxByte: Byte         = '2'
  private [stats] val IteratorStackByte: Byte  = '3'
  private [stats] val HistogramByte: Byte      = '4'
  private [stats] val RangeHistogramByte: Byte = '5'

  private [stats] def write(output: Output, sft: SimpleFeatureType, stat: Stat): Unit = {
    stat match {
      case s: CountStat          => output.writeByte(CountByte);          writeCount(output, s)
      case s: MinMax[_]          => output.writeByte(MinMaxByte);         writeMinMax(output, sft, s)
      case s: Histogram[_]       => output.writeByte(HistogramByte);      writeHistogram(output, sft, s)
      case s: RangeHistogram[_]  => output.writeByte(RangeHistogramByte); writeRangeHistogram(output, sft, s)
      case s: IteratorStackCount => output.writeByte(IteratorStackByte);  writeIteratorStackCount(output, s)
      case s: SeqStat            => output.writeByte(SeqStatByte);        writeSeqStat(output, sft, s)
    }
  }

  private [stats] def read(input: Input, sft: SimpleFeatureType): Stat = {
    input.readByte() match {
      case CountByte          => readCount(input)
      case MinMaxByte         => readMinMax(input, sft)
      case HistogramByte      => readHistogram(input, sft)
      case RangeHistogramByte => readRangeHistogram(input, sft)
      case IteratorStackByte  => readIteratorStackCount(input)
      case SeqStatByte        => readSeqStat(input, sft)
    }
  }

  private [stats] def writeSeqStat(output: Output, sft: SimpleFeatureType, stat: SeqStat): Unit =
    stat.stats.foreach(write(output, sft, _))

  private [stats] def readSeqStat(input: Input, sft: SimpleFeatureType): SeqStat = {
    val stats = ArrayBuffer.empty[Stat]
    while (input.available() > 0) {
      stats.append(read(input, sft))
    }
    new SeqStat(stats)
  }

  private [stats] def writeCount(output: Output, stat: CountStat): Unit = output.writeLong(stat.counter, true)

  private [stats] def readCount(input: Input): CountStat = {
    val stat = new CountStat()
    stat.counter = input.readLong(true)
    stat
  }

  private [stats] def writeMinMax(output: Output, sft: SimpleFeatureType, stat: MinMax[_]): Unit = {
    val stringify = Stat.stringifier(sft.getDescriptor(stat.attribute).getType.getBinding)
    output.writeInt(stat.attribute, true)
    val (min, max) = stat.bounds.getOrElse((null, null))
    output.writeString(stringify(min))
    output.writeString(stringify(max))
  }

  private [stats] def readMinMax(input: Input, sft: SimpleFeatureType): MinMax[_] = {
    val attribute = input.readInt(true)
    val minString = input.readString
    val maxString = input.readString

    val attributeType = sft.getDescriptor(attribute).getType.getBinding
    val destringify = Stat.destringifier(attributeType)
    val min = destringify(minString)
    val max = destringify(maxString)

    val stat = if (attributeType == classOf[String]) {
      new MinMax[String](attribute)
    } else if (attributeType == classOf[Integer]) {
      new MinMax[Integer](attribute)
    } else if (attributeType == classOf[jLong]) {
      new MinMax[jLong](attribute)
    } else if (attributeType == classOf[jFloat]) {
      new MinMax[jFloat](attribute)
    } else if (attributeType == classOf[jDouble]) {
      new MinMax[jDouble](attribute)
    } else if (attributeType == classOf[Date]) {
      new MinMax[Date](attribute)
    } else if (classOf[Geometry].isAssignableFrom(attributeType)) {
      new MinMax[Geometry](attribute)
    } else {
      throw new Exception(s"Cannot deserialize MinMax due to invalid type: $attributeType")
    }

    if (min != null) {
      stat.asInstanceOf[MinMax[Any]].min = min
      stat.asInstanceOf[MinMax[Any]].max = max
    }

    stat
  }

  private [stats] def writeIteratorStackCount(output: Output, stat: IteratorStackCount): Unit =
    output.writeLong(stat.counter, true)

  private [stats] def readIteratorStackCount(input: Input): IteratorStackCount = {
    val stat = new IteratorStackCount()
    stat.counter = input.readLong(true)
    stat
  }

  private [stats] def writeHistogram(output: Output, sft: SimpleFeatureType, stat: Histogram[_]): Unit = {
    val stringify = Stat.stringifier(sft.getDescriptor(stat.attribute).getType.getBinding)
    output.writeInt(stat.attribute, true)
    output.writeInt(stat.histogram.size, true)
    stat.histogram.foreach { case (key, count) => output.writeString(stringify(key)); output.writeLong(count, true) }
  }

  private [stats] def readHistogram(input: Input, sft: SimpleFeatureType): Histogram[_] = {
    val attribute = input.readInt(true)
    val attributeType = sft.getDescriptor(attribute).getType.getBinding

    val stat: Histogram[Any] = if (attributeType == classOf[String]) {
      new Histogram[String](attribute).asInstanceOf[Histogram[Any]]
    } else if (attributeType == classOf[Integer]) {
      new Histogram[Integer](attribute).asInstanceOf[Histogram[Any]]
    } else if (attributeType == classOf[jLong]) {
      new Histogram[jLong](attribute).asInstanceOf[Histogram[Any]]
    } else if (attributeType == classOf[jFloat]) {
      new Histogram[jFloat](attribute).asInstanceOf[Histogram[Any]]
    } else if (attributeType == classOf[jDouble]) {
      new Histogram[jDouble](attribute).asInstanceOf[Histogram[Any]]
    } else if (attributeType == classOf[Date]) {
      new Histogram[Date](attribute).asInstanceOf[Histogram[Any]]
    } else if (classOf[Geometry].isAssignableFrom(attributeType)) {
      new Histogram[Geometry](attribute).asInstanceOf[Histogram[Any]]
    } else {
      throw new Exception(s"Cannot unpack EnumeratedHistogram due to invalid type: $attributeType")
    }

    val size = input.readInt(true)
    if (size > 0) {
      val destringify = Stat.destringifier(attributeType)
      var i = 0
      while (i < size) {
        stat.histogram(destringify(input.readString)) = input.readLong(true)
        i += 1
      }
    }

    stat
  }

  private [stats] def writeRangeHistogram(output: Output, sft: SimpleFeatureType, stat: RangeHistogram[_]): Unit = {
    val stringify = Stat.stringifier(sft.getDescriptor(stat.attribute).getType.getBinding)
    output.writeInt(stat.attribute, true)
    output.writeInt(stat.length, true)
    output.writeString(stringify(stat.endpoints._1))
    output.writeString(stringify(stat.endpoints._2))
    if (stat.isEmpty) {
      output.writeBoolean(false)
    } else {
      output.writeBoolean(true)
      var i = 0
      while (i < stat.length) {
        output.writeLong(stat.count(i), true)
        i += 1
      }
    }
  }

  private [stats] def readRangeHistogram(input: Input, sft: SimpleFeatureType): RangeHistogram[_] = {
    val attribute = input.readInt(true)
    val numBins = input.readInt(true)
    val lowerEndpoint = input.readString
    val upperEndpoint = input.readString

    val attributeType = sft.getDescriptor(attribute).getType.getBinding
    val destringify = Stat.destringifier(attributeType)
    val bounds = (destringify(lowerEndpoint), destringify(upperEndpoint))

    val stat = if (attributeType == classOf[String]) {
      new RangeHistogram[String](attribute, numBins, bounds.asInstanceOf[(String, String)])
    } else if (attributeType == classOf[Integer]) {
      new RangeHistogram[Integer](attribute, numBins, bounds.asInstanceOf[(Integer, Integer)])
    } else if (attributeType == classOf[jLong]) {
      new RangeHistogram[jLong](attribute, numBins, bounds.asInstanceOf[(jLong, jLong)])
    } else if (attributeType == classOf[jFloat]) {
      new RangeHistogram[jFloat](attribute, numBins, bounds.asInstanceOf[(jFloat, jFloat)])
    } else if (attributeType == classOf[jDouble]) {
      new RangeHistogram[jDouble](attribute, numBins, bounds.asInstanceOf[(jDouble, jDouble)])
    } else if (attributeType == classOf[Date]) {
      new RangeHistogram[Date](attribute, numBins, bounds.asInstanceOf[(Date, Date)])
    } else if (classOf[Geometry].isAssignableFrom(attributeType)) {
      new RangeHistogram[Geometry](attribute, numBins, bounds.asInstanceOf[(Geometry, Geometry)])
    } else {
      throw new Exception(s"Cannot unpack RangeHistogram due to invalid type: $attributeType")
    }

    if (input.readBoolean()) {
      var i = 0
      while (i < numBins) {
        stat.bins.counts(i) = input.readLong(true)
        i += 1
      }
    }

    stat
  }
}
