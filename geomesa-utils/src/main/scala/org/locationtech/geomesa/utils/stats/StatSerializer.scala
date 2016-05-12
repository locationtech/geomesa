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

import com.clearspring.analytics.stream.cardinality.HyperLogLog
import com.clearspring.analytics.stream.frequency.RichCountMinSketch
import com.esotericsoftware.kryo.io.{Input, Output}
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.utils.cache.SoftThreadLocal
import org.locationtech.geomesa.utils.stats.MinMax.MinMaxDefaults
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Serialize and deserialize stats
  */
trait StatSerializer {
  def serialize(stat: Stat): Array[Byte]
  def deserialize(bytes: Array[Byte], immutable: Boolean = false): Stat =
    deserialize(bytes, 0, bytes.length, immutable)
  def deserialize(bytes: Array[Byte], offset: Int, length: Int, immutable: Boolean): Stat
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

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int, immutable: Boolean): Stat = {
    val input = KryoStatSerializer.inputs.getOrElseUpdate(new Input)
    input.setBuffer(bytes, offset, length)
    KryoStatSerializer.read(input, sft, immutable)
  }
}

object KryoStatSerializer {

  private [stats] val inputs  = new SoftThreadLocal[Input]()
  private [stats] val outputs = new SoftThreadLocal[Output]()

  // bytes indicating the type of stat
  private [stats] val SeqStatByte: Byte          = 0
  private [stats] val CountByte: Byte            = 1
  private [stats] val MinMaxByte: Byte           = 2
  private [stats] val IteratorStackByte: Byte    = 3
  private [stats] val HistogramByte: Byte        = 4
  private [stats] val RangeHistogramByte: Byte   = 5
  private [stats] val FrequencyByte: Byte        = 6
  private [stats] val Z3RangeHistogramByte: Byte = 7
  private [stats] val Z3FrequencyByte: Byte      = 8

  private [stats] def write(output: Output, sft: SimpleFeatureType, stat: Stat): Unit = {
    stat match {
      case s: CountStat          => output.writeByte(CountByte);            writeCount(output, s)
      case s: MinMax[_]          => output.writeByte(MinMaxByte);           writeMinMax(output, sft, s)
      case s: Histogram[_]       => output.writeByte(HistogramByte);        writeHistogram(output, sft, s)
      case s: RangeHistogram[_]  => output.writeByte(RangeHistogramByte);   writeRangeHistogram(output, sft, s)
      case s: Frequency[_]       => output.writeByte(FrequencyByte);        writeFrequency(output, sft, s)
      case s: Z3RangeHistogram   => output.writeByte(Z3RangeHistogramByte); writeZ3RangeHistogram(output, sft, s)
      case s: Z3Frequency        => output.writeByte(Z3FrequencyByte);      writeZ3Frequency(output, sft, s)
      case s: IteratorStackCount => output.writeByte(IteratorStackByte);    writeIteratorStackCount(output, s)
      case s: SeqStat            => output.writeByte(SeqStatByte);          writeSeqStat(output, sft, s)
    }
  }

  private [stats] def read(input: Input, sft: SimpleFeatureType, immutable: Boolean): Stat = {
    input.readByte() match {
      case CountByte            => readCount(input, immutable)
      case MinMaxByte           => readMinMax(input, sft, immutable)
      case HistogramByte        => readHistogram(input, sft, immutable)
      case RangeHistogramByte   => readRangeHistogram(input, sft, immutable)
      case FrequencyByte        => readFrequency(input, sft, immutable)
      case Z3RangeHistogramByte => readZ3RangeHistogram(input, sft, immutable)
      case Z3FrequencyByte      => readZ3Frequency(input, sft, immutable)
      case IteratorStackByte    => readIteratorStackCount(input, immutable)
      case SeqStatByte          => readSeqStat(input, sft, immutable)
    }
  }

  private [stats] def writeSeqStat(output: Output, sft: SimpleFeatureType, stat: SeqStat): Unit =
    stat.stats.foreach(write(output, sft, _))

  private [stats] def readSeqStat(input: Input, sft: SimpleFeatureType, immutable: Boolean): SeqStat = {
    val stats = ArrayBuffer.empty[Stat]
    while (input.available() > 0) {
      stats.append(read(input, sft, immutable))
    }
    if (immutable) {
      new SeqStat(stats) with ImmutableStat
    } else {
      new SeqStat(stats)
    }
  }

  private [stats] def writeCount(output: Output, stat: CountStat): Unit = output.writeLong(stat.counter, true)

  private [stats] def readCount(input: Input, immutable: Boolean): CountStat = {
    val stat = if (immutable) new CountStat() with ImmutableStat else new CountStat
    stat.counter = input.readLong(true)
    stat
  }

  private [stats] def writeMinMax(output: Output, sft: SimpleFeatureType, stat: MinMax[_]): Unit = {
    output.writeInt(stat.attribute, true)
    val hpp = stat.hpp.getBytes
    output.writeInt(hpp.length, true)
    output.write(hpp)

    val write = writer(output, sft.getDescriptor(stat.attribute).getType.getBinding)
    write(stat.minValue)
    write(stat.maxValue)
  }

  private [stats] def readMinMax(input: Input, sft: SimpleFeatureType, immutable: Boolean): MinMax[_] = {
    val attribute = input.readInt(true)
    val hpp = {
      val hppBytes = Array.ofDim[Byte](input.readInt(true))
      input.read(hppBytes)
      HyperLogLog.Builder.build(hppBytes)
    }

    val binding = sft.getDescriptor(attribute).getType.getBinding
    val read = reader(input, binding)
    val min = read()
    val max = read()

    val defaults = MinMaxDefaults[Any](binding)
    val classTag = ClassTag[Any](binding)

    if (immutable) {
      new MinMax[Any](attribute, min, max, hpp)(defaults, classTag) with ImmutableStat
    } else {
      new MinMax[Any](attribute, min, max, hpp)(defaults, classTag)
    }
  }

  private [stats] def writeHistogram(output: Output, sft: SimpleFeatureType, stat: Histogram[_]): Unit = {
    output.writeInt(stat.attribute, true)
    output.writeInt(stat.histogram.size, true)

    val write = writer(output, sft.getDescriptor(stat.attribute).getType.getBinding)
    stat.histogram.foreach { case (key, count) => write(key); output.writeLong(count, true) }
  }

  private [stats] def readHistogram(input: Input, sft: SimpleFeatureType, immutable: Boolean): Histogram[_] = {
    val attribute = input.readInt(true)
    val size = input.readInt(true)

    val binding = sft.getDescriptor(attribute).getType.getBinding
    val read = reader(input, binding)

    val classTag = ClassTag[Any](binding)
    val stat = if (immutable) {
      new Histogram[Any](attribute)(classTag) with ImmutableStat
    } else {
      new Histogram[Any](attribute)(classTag)
    }

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

    val write = writer(output, sft.getDescriptor(stat.attribute).getType.getBinding)
    write(stat.bounds._1)
    write(stat.bounds._2)

    writeCountArray(output, stat.bins.counts)
  }

  private [stats] def readRangeHistogram(input: Input, sft: SimpleFeatureType, immutable: Boolean): RangeHistogram[_] = {
    val attribute = input.readInt(true)
    val length = input.readInt(true)

    val binding = sft.getDescriptor(attribute).getType.getBinding
    val read = reader(input, binding)

    val min = read()
    val max = read()

    val defaults = MinMaxDefaults[Any](binding)
    val classTag = ClassTag[Any](binding)
    val stat = if (immutable) {
      new RangeHistogram[Any](attribute, length, (min, max))(defaults, classTag) with ImmutableStat
    } else {
      new RangeHistogram[Any](attribute, length, (min, max))(defaults, classTag)
    }

    readCountArray(input, stat.bins.counts)

    stat
  }

  private [stats] def writeZ3RangeHistogram(output: Output, sft: SimpleFeatureType, stat: Z3RangeHistogram): Unit = {
    output.writeInt(stat.geomIndex, true)
    output.writeInt(stat.dtgIndex, true)
    output.writeInt(stat.length, true)

    val bins = stat.binMap.filter(_._2.counts.exists(_ != 0L))

    output.writeInt(bins.size, true)

    bins.foreach { case (w, bin) =>
      output.writeShort(w)
      writeCountArray(output, bin.counts)
    }
  }

  private [stats] def readZ3RangeHistogram(input: Input, sft: SimpleFeatureType, immutable: Boolean): Z3RangeHistogram = {
    val geomIndex = input.readInt(true)
    val dtgIndex  = input.readInt(true)
    val length = input.readInt(true)

    val stat = if (immutable) {
      new Z3RangeHistogram(geomIndex, dtgIndex, length) with ImmutableStat
    } else {
      new Z3RangeHistogram(geomIndex, dtgIndex, length)
    }

    val numWeeks = input.readInt(true)
    var week = 0

    while (week < numWeeks) {
      val bins = stat.newBins
      stat.binMap.put(input.readShort, bins)
      readCountArray(input, bins.counts)
      week += 1
    }

    stat
  }

  private def writeCountArray(output: Output, counts: Array[Long]): Unit = {
    var i = 0
    while (i < counts.length) {
      val count = counts(i)
      if (count == 0) {
        var nextNonZero = i + 1
        while (nextNonZero < counts.length && counts(nextNonZero) == 0) {
          nextNonZero += 1
        }
        val numZeros = nextNonZero - i
        if (numZeros > 4) {
          // write a max long as an indicator that we have sparse values, then write the number of zeros
          output.writeLong(Long.MaxValue, true)
          output.writeInt(numZeros, true)
        } else if (numZeros > 0) {
          (0 until numZeros).foreach(_ => output.writeLong(0L, true))
        }
        i = nextNonZero
      } else {
        output.writeLong(count, true)
        i += 1
      }
    }
  }

  private def readCountArray(input: Input, counts: Array[Long]): Unit = {
    var i = 0
    while (i < counts.length) {
      val count = input.readLong(true)
      if (count == Long.MaxValue) {
        i += input.readInt(true) // skip sparsely written values
      } else {
        counts(i) = count
        i += 1
      }
    }
  }

  private [stats] def writeFrequency(output: Output, sft: SimpleFeatureType, stat: Frequency[_]): Unit = {
    output.writeInt(stat.attribute, true)
    output.writeInt(stat.precision, true)
    output.writeDouble(stat.eps)
    output.writeDouble(stat.confidence)

    val table = new RichCountMinSketch(stat.sketch).table
    var i = 0
    while (i < table.length) {
      val row = table(i)
      var j = 0
      while (j < row.length) {
        output.writeLong(row(j), true)
        j += 1
      }
      i += 1
    }

    output.writeLong(stat.sketch.size, true)
  }

  private [stats] def readFrequency(input: Input, sft: SimpleFeatureType, immutable: Boolean): Frequency[_] = {
    val attribute = input.readInt(true)
    val precision = input.readInt(true)
    val eps = input.readDouble()
    val confidence = input.readDouble()

    val binding = sft.getDescriptor(attribute).getType.getBinding
    val stat = if (immutable) {
      new Frequency[Any](attribute, precision, eps, confidence)(ClassTag[Any](binding)) with ImmutableStat
    } else {
      new Frequency[Any](attribute, precision, eps, confidence)(ClassTag[Any](binding))
    }

    val table = new RichCountMinSketch(stat.sketch).table
    var i = 0
    while (i < table.length) {
      val row = table(i)
      var j = 0
      while (j < row.length) {
        row(j) = input.readLong(true)
        j += 1
      }
      i += 1
    }

    new RichCountMinSketch(stat.sketch).setSize(input.readLong(true))

    stat
  }

  private [stats] def writeZ3Frequency(output: Output, sft: SimpleFeatureType, stat: Z3Frequency): Unit = {
    output.writeInt(stat.geomIndex, true)
    output.writeInt(stat.dtgIndex, true)
    output.writeInt(stat.precision, true)
    output.writeDouble(stat.eps)
    output.writeDouble(stat.confidence)

    val sketches = stat.sketches.filter(_._2.size > 0)
    output.writeInt(sketches.size, true)

    sketches.foreach { case (w, sketch) =>
      output.writeShort(w)
      val table = new RichCountMinSketch(sketch).table
      var i = 0
      while (i < table.length) {
        val row = table(i)
        var j = 0
        while (j < row.length) {
          output.writeLong(row(j), true)
          j += 1
        }
        i += 1
      }

      output.writeLong(sketch.size, true)
    }
  }

  private [stats] def readZ3Frequency(input: Input, sft: SimpleFeatureType, immutable: Boolean): Z3Frequency = {
    val geomIndex = input.readInt(true)
    val dtgIndex  = input.readInt(true)
    val precision = input.readInt(true)
    val eps = input.readDouble()
    val confidence = input.readDouble()

    val stat = if (immutable) {
      new Z3Frequency(geomIndex, dtgIndex, precision, eps, confidence) with ImmutableStat
    } else {
      new Z3Frequency(geomIndex, dtgIndex, precision, eps, confidence)
    }

    val numSketches = input.readInt(true)
    var sketchCount = 0

    while (sketchCount < numSketches) {
      val sketch = stat.newSketch
      stat.sketches.put(input.readShort, sketch)
      val table = new RichCountMinSketch(sketch).table
      var i = 0
      while (i < table.length) {
        val row = table(i)
        var j = 0
        while (j < row.length) {
          row(j) = input.readLong(true)
          j += 1
        }
        i += 1
      }

      new RichCountMinSketch(sketch).setSize(input.readLong(true))

      sketchCount += 1
    }

    stat
  }

  private [stats] def writeIteratorStackCount(output: Output, stat: IteratorStackCount): Unit =
    output.writeLong(stat.counter, true)

  private [stats] def readIteratorStackCount(input: Input, immutable: Boolean): IteratorStackCount = {
    val stat = if (immutable) new IteratorStackCount() with ImmutableStat else new IteratorStackCount()
    stat.counter = input.readLong(true)
    stat
  }

  private def writer(output: Output, binding: Class[_]): (Any) => Unit = {
    if (binding == classOf[String]) {
      (value) => output.writeString(value.asInstanceOf[String])
    } else if (binding == classOf[Integer]) {
      (value) => output.writeInt(value.asInstanceOf[Integer], true)
    } else if (binding == classOf[jLong]) {
      (value) => output.writeLong(value.asInstanceOf[jLong], true)
    } else if (binding == classOf[jFloat]) {
      (value) => output.writeFloat(value.asInstanceOf[jFloat])
    } else if (binding == classOf[jDouble]) {
      (value) => output.writeDouble(value.asInstanceOf[jDouble])
    } else if (classOf[Date].isAssignableFrom(binding)) {
      (value) => output.writeLong(value.asInstanceOf[Date].getTime, true)
    } else if (classOf[Geometry].isAssignableFrom(binding)) {
      (value) => {
        val b1 = WKBUtils.write(value.asInstanceOf[Geometry])
        output.writeInt(b1.length, true)
        output.write(b1)
      }
    } else {
      throw new Exception(s"Cannot serialize stat due to invalid type: $binding")
    }
  }

  private def reader(input: Input, binding: Class[_]): () => Any = {
    if (binding == classOf[String]) {
      () => input.readString()
    } else if (binding == classOf[Integer]) {
      () => input.readInt(true)
    } else if (binding == classOf[jLong]) {
      () => input.readLong(true)
    } else if (binding == classOf[jFloat]) {
      () => input.readFloat()
    } else if (binding == classOf[jDouble]) {
      () => input.readDouble()
    } else if (classOf[Date].isAssignableFrom(binding)) {
      () => new Date(input.readLong(true))
    } else if (classOf[Geometry].isAssignableFrom(binding)) {
      () => {
        val b = Array.ofDim[Byte](input.readInt(true))
        input.read(b)
        WKBUtils.read(b)
      }
    } else {
      throw new Exception(s"Cannot deserialize stat due to invalid type: $binding")
    }
  }
}
