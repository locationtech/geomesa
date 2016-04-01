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
import org.locationtech.geomesa.utils.text.WKBUtils
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
    output.writeInt(stat.attribute, true)
    val binding = sft.getDescriptor(stat.attribute).getType.getBinding
    if (binding == classOf[String]) {
      output.writeString(stat.minValue.asInstanceOf[String])
      output.writeString(stat.maxValue.asInstanceOf[String])
    } else if (binding == classOf[Integer]) {
      output.writeInt(stat.minValue.asInstanceOf[Integer], true)
      output.writeInt(stat.maxValue.asInstanceOf[Integer], true)
    } else if (binding == classOf[jLong]) {
      output.writeLong(stat.minValue.asInstanceOf[jLong], true)
      output.writeLong(stat.maxValue.asInstanceOf[jLong], true)
    } else if (binding == classOf[jFloat]) {
      output.writeFloat(stat.minValue.asInstanceOf[jFloat])
      output.writeFloat(stat.maxValue.asInstanceOf[jFloat])
    } else if (binding == classOf[jDouble]) {
      output.writeDouble(stat.minValue.asInstanceOf[jDouble])
      output.writeDouble(stat.maxValue.asInstanceOf[jDouble])
    } else if (binding == classOf[Date]) {
      output.writeLong(stat.minValue.asInstanceOf[Date].getTime, true)
      output.writeLong(stat.maxValue.asInstanceOf[Date].getTime, true)
    } else if (classOf[Geometry].isAssignableFrom(binding)) {
      val b1 = WKBUtils.write(stat.minValue.asInstanceOf[Geometry])
      val b2 = WKBUtils.write(stat.maxValue.asInstanceOf[Geometry])
      output.writeInt(b1.length, true)
      output.write(b1)
      output.writeInt(b2.length, true)
      output.write(b2)
    } else {
      throw new Exception(s"Cannot serialize MinMax due to invalid type: $binding")
    }
  }

  private [stats] def readMinMax(input: Input, sft: SimpleFeatureType): MinMax[_] = {
    val attribute = input.readInt(true)

    val binding = sft.getDescriptor(attribute).getType.getBinding
    if (binding == classOf[String]) {
      val stat = new MinMax[String](attribute)
      stat.minValue = input.readString()
      stat.maxValue = input.readString()
      stat
    } else if (binding == classOf[Integer]) {
      val stat = new MinMax[Integer](attribute)
      stat.minValue = input.readInt(true)
      stat.maxValue = input.readInt(true)
      stat
    } else if (binding == classOf[jLong]) {
      val stat = new MinMax[jLong](attribute)
      stat.minValue = input.readLong(true)
      stat.maxValue = input.readLong(true)
      stat
    } else if (binding == classOf[jFloat]) {
      val stat = new MinMax[jFloat](attribute)
      stat.minValue = input.readFloat()
      stat.maxValue = input.readFloat()
      stat
    } else if (binding == classOf[jDouble]) {
      val stat = new MinMax[jDouble](attribute)
      stat.minValue = input.readDouble()
      stat.maxValue = input.readDouble()
      stat
    } else if (binding == classOf[Date]) {
      val stat = new MinMax[Date](attribute)
      stat.minValue = new Date(input.readLong(true))
      stat.maxValue = new Date(input.readLong(true))
      stat
    } else if (classOf[Geometry].isAssignableFrom(binding)) {
      val stat = new MinMax[Geometry](attribute)
      val b1 = Array.ofDim[Byte](input.readInt(true))
      input.read(b1)
      val b2 = Array.ofDim[Byte](input.readInt(true))
      input.read(b2)
      stat.minValue = WKBUtils.read(b1)
      stat.maxValue = WKBUtils.read(b2)
      stat
    } else {
      throw new Exception(s"Cannot deserialize MinMax due to invalid type: $binding")
    }
  }

  private [stats] def writeHistogram(output: Output, sft: SimpleFeatureType, stat: Histogram[_]): Unit = {
    output.writeInt(stat.attribute, true)
    output.writeInt(stat.histogram.size, true)

    val binding = sft.getDescriptor(stat.attribute).getType.getBinding
    val write: (Any) => Unit = if (binding == classOf[String]) {
      (key) => output.writeString(key.asInstanceOf[String])
    } else if (binding == classOf[Integer]) {
      (key) => output.writeInt(key.asInstanceOf[Integer], true)
    } else if (binding == classOf[jLong]) {
      (key) => output.writeLong(key.asInstanceOf[jLong], true)
    } else if (binding == classOf[jFloat]) {
      (key) => output.writeFloat(key.asInstanceOf[jFloat])
    } else if (binding == classOf[jDouble]) {
      (key) => output.writeDouble(key.asInstanceOf[jDouble])
    } else if (binding == classOf[Date]) {
      (key) => output.writeLong(key.asInstanceOf[Date].getTime, true)
    } else if (classOf[Geometry].isAssignableFrom(binding)) {
      (key) => {
        val b = WKBUtils.write(key.asInstanceOf[Geometry])
        output.writeInt(b.length, true)
        output.write(b)
      }
    } else {
      throw new Exception(s"Cannot serialize Histogram due to invalid type: $binding")
    }

    stat.histogram.foreach { case (key, count) => write(key); output.writeLong(count, true) }
  }

  private [stats] def readHistogram(input: Input, sft: SimpleFeatureType): Histogram[_] = {
    val attribute = input.readInt(true)

    val binding = sft.getDescriptor(attribute).getType.getBinding

    var stat: Histogram[Any] = null
    var read: () => Any = null
    if (binding == classOf[String]) {
      stat = new Histogram[String](attribute).asInstanceOf[Histogram[Any]]
      read = () => input.readString
    } else if (binding == classOf[Integer]) {
      stat = new Histogram[Integer](attribute).asInstanceOf[Histogram[Any]]
      read = () => input.readInt(true)
    } else if (binding == classOf[jLong]) {
      stat = new Histogram[jLong](attribute).asInstanceOf[Histogram[Any]]
      read = () => input.readLong(true)
    } else if (binding == classOf[jFloat]) {
      stat = new Histogram[jFloat](attribute).asInstanceOf[Histogram[Any]]
      read = () => input.readFloat
    } else if (binding == classOf[jDouble]) {
      stat = new Histogram[jDouble](attribute).asInstanceOf[Histogram[Any]]
      read = () => input.readDouble
    } else if (binding == classOf[Date]) {
      stat = new Histogram[Date](attribute).asInstanceOf[Histogram[Any]]
      read = () => new Date(input.readLong(true))
    } else if (classOf[Geometry].isAssignableFrom(binding)) {
      stat = new Histogram[Geometry](attribute).asInstanceOf[Histogram[Any]]
      read = () => {
        val b = Array.ofDim[Byte](input.readInt(true))
        input.read(b)
        WKBUtils.read(b)
      }
    } else {
      throw new Exception(s"Cannot deserialize Histogram due to invalid type: $binding")
    }

    val size = input.readInt(true)

    var i = 0
    while (i < size) {
      stat.histogram(read()) = input.readLong(true)
      i += 1
    }

    stat
  }

  private [stats] def writeRangeHistogram(output: Output, sft: SimpleFeatureType, stat: RangeHistogram[_]): Unit = {
    output.writeInt(stat.attribute, true)
    output.writeInt(stat.length, true)

    val binding = sft.getDescriptor(stat.attribute).getType.getBinding
    if (binding == classOf[String]) {
      output.writeString(stat.bounds._1.asInstanceOf[String])
      output.writeString(stat.bounds._2.asInstanceOf[String])
    } else if (binding == classOf[Integer]) {
      output.writeInt(stat.bounds._1.asInstanceOf[Integer], true)
      output.writeInt(stat.bounds._2.asInstanceOf[Integer], true)
    } else if (binding == classOf[jLong]) {
      output.writeLong(stat.bounds._1.asInstanceOf[jLong], true)
      output.writeLong(stat.bounds._2.asInstanceOf[jLong], true)
    } else if (binding == classOf[jFloat]) {
      output.writeFloat(stat.bounds._1.asInstanceOf[jFloat])
      output.writeFloat(stat.bounds._2.asInstanceOf[jFloat])
    } else if (binding == classOf[jDouble]) {
      output.writeDouble(stat.bounds._1.asInstanceOf[jDouble])
      output.writeDouble(stat.bounds._2.asInstanceOf[jDouble])
    } else if (binding == classOf[Date]) {
      output.writeLong(stat.bounds._1.asInstanceOf[Date].getTime, true)
      output.writeLong(stat.bounds._2.asInstanceOf[Date].getTime, true)
    } else if (classOf[Geometry].isAssignableFrom(binding)) {
      val b1 = WKBUtils.write(stat.bounds._1.asInstanceOf[Geometry])
      val b2 = WKBUtils.write(stat.bounds._2.asInstanceOf[Geometry])
      output.writeInt(b1.length, true)
      output.write(b1)
      output.writeInt(b2.length, true)
      output.write(b2)
    } else {
      throw new Exception(s"Cannot serialize RangeHistogram due to invalid type: $binding")
    }
    var i = 0
    while (i < stat.length) {
      output.writeLong(stat.count(i), true)
      i += 1
    }
  }

  private [stats] def readRangeHistogram(input: Input, sft: SimpleFeatureType): RangeHistogram[_] = {
    val attribute = input.readInt(true)
    val length = input.readInt(true)

    val binding = sft.getDescriptor(attribute).getType.getBinding
    val stat = if (binding == classOf[String]) {
      val s = input.readString()
      val e = input.readString()
      new RangeHistogram[String](attribute, length, (s, e))
    } else if (binding == classOf[Integer]) {
      val s = input.readInt(true)
      val e = input.readInt(true)
      new RangeHistogram[Integer](attribute, length, (s, e))
    } else if (binding == classOf[jLong]) {
      val s = input.readLong(true)
      val e = input.readLong(true)
      new RangeHistogram[jLong](attribute, length, (s, e))
    } else if (binding == classOf[jFloat]) {
      val s = input.readFloat()
      val e = input.readFloat()
      new RangeHistogram[jFloat](attribute, length, (s, e))
    } else if (binding == classOf[jDouble]) {
      val s = input.readDouble()
      val e = input.readDouble()
      new RangeHistogram[jDouble](attribute, length, (s, e))
    } else if (binding == classOf[Date]) {
      val s = new Date(input.readLong(true))
      val e = new Date(input.readLong(true))
      new RangeHistogram[Date](attribute, length, (s, e))
    } else if (classOf[Geometry].isAssignableFrom(binding)) {
      val b1 = Array.ofDim[Byte](input.readInt(true))
      input.read(b1)
      val b2 = Array.ofDim[Byte](input.readInt(true))
      input.read(b2)
      val s = WKBUtils.read(b1)
      val e = WKBUtils.read(b2)
      new RangeHistogram[Geometry](attribute, length, (s, e))
    } else {
      throw new Exception(s"Cannot deserialize RangeHistogram due to invalid type: $binding")
    }

    var i = 0
    while (i < length) {
      stat.bins.counts(i) = input.readLong(true)
      i += 1
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
}
