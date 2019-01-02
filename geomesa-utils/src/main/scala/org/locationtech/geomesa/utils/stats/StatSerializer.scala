/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.Date

import com.clearspring.analytics.stream.cardinality.RegisterSet
import com.esotericsoftware.kryo.io.{Input, Output}
import org.locationtech.jts.geom.Geometry
import org.locationtech.geomesa.curve.TimePeriod
import org.locationtech.geomesa.utils.cache.{CacheKeyGenerator, SoftThreadLocal}
import org.locationtech.geomesa.utils.clearspring.{HyperLogLog, StreamSummary}
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

  private val serializers = scala.collection.mutable.Map.empty[String, StatSerializer]

  def apply(sft: SimpleFeatureType): StatSerializer = serializers.synchronized {
    serializers.getOrElseUpdate(CacheKeyGenerator.cacheKey(sft), new KryoStatSerializer(sft))
  }
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

  private val inputs  = new SoftThreadLocal[Input]()
  private val outputs = new SoftThreadLocal[Output]()

  // bytes indicating the type of stat - currently using up to 25

  private val SeqStatByte: Byte           = 0
  private val CountByte: Byte             = 1
  private val IteratorStackByte: Byte     = 3

  private val MinMaxByteV1: Byte          = 2
  private val MinMaxByteV2: Byte          = 16
  private val MinMaxByte: Byte            = 25

  private val EnumerationByteV1: Byte     = 4
  private val EnumerationByte: Byte       = 17

  private val HistogramByteV1: Byte       = 5
  private val HistogramByte: Byte         = 18

  private val FrequencyByteV1: Byte       = 6
  private val FrequencyByteV2: Byte       = 10
  private val FrequencyByte: Byte         = 19

  private val Z3HistogramByteV1: Byte     = 7
  private val Z3HistogramByteV2: Byte     = 11
  private val Z3HistogramByte: Byte       = 20

  private val Z3FrequencyByteV1: Byte     = 8
  private val Z3FrequencyByteV2: Byte     = 12
  private val Z3FrequencyByte: Byte       = 21

  private val DescriptiveStatByteV1: Byte = 13
  private val DescriptiveStatByte: Byte   = 22

  private val GroupByByteV1: Byte         = 14
  private val GroupByByte: Byte           = 23

  private val TopKByteV1: Byte            = 9
  private val TopKByteV2: Byte            = 15
  private val TopKByte: Byte              = 24

  private def write(output: Output, sft: SimpleFeatureType, stat: Stat): Unit = {
    stat match {
      case s: CountStat           => output.writeByte(CountByte);           writeCount(output, s)
      case s: MinMax[_]           => output.writeByte(MinMaxByte);          writeMinMax(output, sft, s)
      case s: EnumerationStat[_]  => output.writeByte(EnumerationByte);     writeEnumeration(output, sft, s)
      case s: TopK[_]             => output.writeByte(TopKByte);            writeTopK(output, sft, s)
      case s: Histogram[_]        => output.writeByte(HistogramByte);       writeHistogram(output, sft, s)
      case s: Frequency[_]        => output.writeByte(FrequencyByte);       writeFrequency(output, sft, s)
      case s: Z3Histogram         => output.writeByte(Z3HistogramByte);     writeZ3Histogram(output, sft, s)
      case s: Z3Frequency         => output.writeByte(Z3FrequencyByte);     writeZ3Frequency(output, sft, s)
      case s: IteratorStackCount  => output.writeByte(IteratorStackByte);   writeIteratorStackCount(output, s)
      case s: SeqStat             => output.writeByte(SeqStatByte);         writeSeqStat(output, sft, s)
      case s: DescriptiveStats    => output.writeByte(DescriptiveStatByte); writeDescriptiveStats(output, sft, s)
      case s: GroupBy[_]          => output.writeByte(GroupByByte);         writeGroupBy(output, sft, s)
      case _ => throw new NotImplementedError(s"Unhandled stat $stat")
    }
  }

  private def read(input: Input, sft: SimpleFeatureType, immutable: Boolean): Stat = {
    input.readByte() match {
      case CountByte             => readCount(input, sft, immutable)
      case MinMaxByte            => readMinMax(input, sft, immutable, 3)
      case EnumerationByte       => readEnumeration(input, sft, immutable, 2)
      case TopKByte              => readTopK(input, sft, immutable, 3)
      case HistogramByte         => readHistogram(input, sft, immutable, 2)
      case FrequencyByte         => readFrequency(input, sft, immutable, 3)
      case Z3HistogramByte       => readZ3Histogram(input, sft, immutable, 3)
      case Z3FrequencyByte       => readZ3Frequency(input, sft, immutable, 3)
      case IteratorStackByte     => readIteratorStackCount(input, sft, immutable)
      case SeqStatByte           => readSeqStat(input, sft, immutable)
      case DescriptiveStatByte   => readDescriptiveStat(input, sft, immutable, 2)
      case GroupByByte           => readGroupBy(input, sft, immutable, 2)
      case EnumerationByteV1     => readEnumeration(input, sft, immutable, 1)
      case HistogramByteV1       => readHistogram(input, sft, immutable, 1)
      case FrequencyByteV2       => readFrequency(input, sft, immutable, 2)
      case Z3HistogramByteV2     => readZ3Histogram(input, sft, immutable, 2)
      case Z3FrequencyByteV2     => readZ3Frequency(input, sft, immutable, 2)
      case DescriptiveStatByteV1 => readDescriptiveStat(input, sft, immutable, 1)
      case GroupByByteV1         => readGroupBy(input, sft, immutable, 1)
      case TopKByteV2            => readTopK(input, sft, immutable, 2)
      case MinMaxByteV2          => readMinMax(input, sft, immutable, 2)
      case FrequencyByteV1       => readFrequency(input, sft, immutable, 1)
      case Z3HistogramByteV1     => readZ3Histogram(input, sft, immutable, 1)
      case Z3FrequencyByteV1     => readZ3Frequency(input, sft, immutable, 1)
      case MinMaxByteV1          => readMinMax(input, sft, immutable, 1)
      case TopKByteV1            => readTopK(input, sft, immutable, 1)
      case _ => throw new RuntimeException("Trying to read malformed or invalid serialized stat")
    }
  }

  private def writeGroupBy(output: Output, sft: SimpleFeatureType, stat: GroupBy[_]): Unit = {
    output.writeAscii(stat.property)
    output.writeString(stat.stat)
    output.writeInt(stat.groups.keys.size, true)
    val keyWriter = writer(output, sft.getDescriptor(stat.property).getType.getBinding)
    stat.groups.foreach { case (key, groupedStat) =>
      keyWriter(key)
      write(output, sft, groupedStat)
    }
  }

  private def readGroupBy(input: Input, sft: SimpleFeatureType, immutable: Boolean, version: Int): GroupBy[_] = {
    val attribute = version match {
      case 2 => input.readString
      case 1 => sft.getDescriptor(input.readInt(true)).getLocalName
      case _ => throw new IllegalArgumentException(s"Invalid group by serialization version: $version")
    }
    val exampleStat = input.readString()
    val keyLength   = input.readInt(true)

    val classTag = ClassTag[Any](sft.getDescriptor(attribute).getType.getBinding)
    val stat = if (immutable) {
      new GroupBy(sft, attribute, exampleStat)(classTag) with ImmutableStat
    } else {
      new GroupBy(sft, attribute, exampleStat)(classTag)
    }

    val keyReader = reader(input, sft.getDescriptor(attribute).getType.getBinding)

    var i = 0
    while (i < keyLength) {
      val key = keyReader.apply()
      val groupedStat = read(input, sft, immutable)
      stat.groups.put(key, groupedStat)
      i += 1
    }
    stat
  }

  private def writeDescriptiveStats(output: Output, sft: SimpleFeatureType, stat: DescriptiveStats): Unit = {
    output.writeInt(stat.properties.size, true)
    stat.properties.foreach(output.writeAscii)

    def writeArray(array: Array[Double]): Unit = for(v <- array) { output.writeDouble(v) }

    writeArray(stat._min.getMatrix.data)
    writeArray(stat._max.getMatrix.data)
    writeArray(stat._sum.getMatrix.data)
    writeArray(stat._mean.getMatrix.data)
    writeArray(stat._m2n.getMatrix.data)
    writeArray(stat._m3n.getMatrix.data)
    writeArray(stat._m4n.getMatrix.data)
    writeArray(stat._c2.getMatrix.data)

    output.writeLong(stat._count, true)
  }

  private def readDescriptiveStat(input: Input,
                                  sft: SimpleFeatureType,
                                  immutable: Boolean,
                                  version: Int): DescriptiveStats = {
    val size = input.readInt(true)
    val attributes = version match {
      case 2 => Seq.fill(size)(input.readString)
      case 1 => Seq.fill(size)(sft.getDescriptor(input.readInt(true)).getLocalName)
      case _ => throw new IllegalArgumentException(s"Invalid descriptive stats serialization version: $version")
    }

    val stats = if (immutable) {
      new DescriptiveStats(sft, attributes) with ImmutableStat
    } else {
      new DescriptiveStats(sft, attributes)
    }

    def readArray(array: Array[Double]): Unit = for(i <- array.indices) { array(i) = input.readDouble }

    readArray(stats._min.getMatrix.data)
    readArray(stats._max.getMatrix.data)
    readArray(stats._sum.getMatrix.data)
    readArray(stats._mean.getMatrix.data)
    readArray(stats._m2n.getMatrix.data)
    readArray(stats._m3n.getMatrix.data)
    readArray(stats._m4n.getMatrix.data)
    readArray(stats._c2.getMatrix.data)
    
    stats._count = input.readLong(true)
    
    stats
  }

  private def writeSeqStat(output: Output, sft: SimpleFeatureType, stat: SeqStat): Unit =
    stat.stats.foreach(write(output, sft, _))

  private def readSeqStat(input: Input, sft: SimpleFeatureType, immutable: Boolean): SeqStat = {
    val stats = ArrayBuffer.empty[Stat]
    while (input.available() > 0) {
      stats.append(read(input, sft, immutable))
    }
    if (immutable) {
      new SeqStat(sft, stats) with ImmutableStat
    } else {
      new SeqStat(sft, stats)
    }
  }

  private def writeCount(output: Output, stat: CountStat): Unit = output.writeLong(stat.counter, true)

  private def readCount(input: Input, sft: SimpleFeatureType, immutable: Boolean): CountStat = {
    val stat = if (immutable) {
      new CountStat(sft) with ImmutableStat
    } else {
      new CountStat(sft)
    }
    stat.counter = input.readLong(true)
    stat
  }

  private def writeMinMax(output: Output, sft: SimpleFeatureType, stat: MinMax[_]): Unit = {
    output.writeAscii(stat.property)
    output.writeInt(stat.hpp.log2m, true)
    output.writeInt(stat.hpp.registerSet.size, true)
    stat.hpp.registerSet.rawBits.foreach(output.writeInt)

    val write = writer(output, sft.getDescriptor(stat.property).getType.getBinding)
    write(stat.minValue)
    write(stat.maxValue)
  }

  private def readMinMax(input: Input, sft: SimpleFeatureType, immutable: Boolean, version: Int): MinMax[_] = {
    val attribute = version match {
      case 3     => input.readString()
      case 1 | 2 => sft.getDescriptor(input.readInt(true)).getLocalName
      case _ => throw new IllegalArgumentException(s"Invalid min/max serialization version: $version")
    }
    val hpp = if (version > 1) {
      val log2m = input.readInt(true)
      val size = input.readInt(true)
      val bytes = Array.fill(size)(input.readInt)
      HyperLogLog(log2m, bytes)
    } else {
      val hppBytes = Array.ofDim[Byte](input.readInt(true))
      input.read(hppBytes)
      val clearspring = com.clearspring.analytics.stream.cardinality.HyperLogLog.Builder.build(hppBytes)
      // use reflection to access private variables
      def getField[T](name: String): T = {
        val field = clearspring.getClass.getDeclaredField(name)
        field.setAccessible(true)
        field.get(clearspring).asInstanceOf[T]
      }
      val log2m = getField[Int]("log2m")
      val registerSet = getField[RegisterSet]("registerSet").bits
      HyperLogLog(log2m, registerSet)
    }

    val binding = sft.getDescriptor(attribute).getType.getBinding
    val read = reader(input, binding)
    val min = read()
    val max = read()

    val defaults = MinMaxDefaults[Any](binding)

    if (immutable) {
      new MinMax[Any](sft, attribute, min, max, hpp)(defaults) with ImmutableStat
    } else {
      new MinMax[Any](sft, attribute, min, max, hpp)(defaults)
    }
  }

  private def writeEnumeration(output: Output, sft: SimpleFeatureType, stat: EnumerationStat[_]): Unit = {
    output.writeAscii(stat.property)
    output.writeInt(stat.enumeration.size, true)

    val write = writer(output, sft.getDescriptor(stat.property).getType.getBinding)
    stat.enumeration.foreach { case (key, count) => write(key); output.writeLong(count, true) }
  }

  private def readEnumeration(input: Input,
                              sft: SimpleFeatureType,
                              immutable: Boolean,
                              version: Int): EnumerationStat[_] = {
    val attribute = version match {
      case 2 => input.readString
      case 1 => sft.getDescriptor(input.readInt(true)).getLocalName
      case _ => throw new IllegalArgumentException(s"Invalid group by serialization version: $version")
    }
    val size = input.readInt(true)

    val binding = sft.getDescriptor(attribute).getType.getBinding
    val read = reader(input, binding)

    val classTag = ClassTag[Any](binding)
    val stat = if (immutable) {
      new EnumerationStat[Any](sft, attribute)(classTag) with ImmutableStat
    } else {
      new EnumerationStat[Any](sft, attribute)(classTag)
    }

    var i = 0
    while (i < size) {
      stat.enumeration(read()) = input.readLong(true)
      i += 1
    }

    stat
  }

  private def writeTopK(output: Output, sft: SimpleFeatureType, stat: TopK[_]): Unit = {
    output.writeAscii(stat.property)
    output.writeInt(stat.size, true)

    val write = writer(output, sft.getDescriptor(stat.property).getType.getBinding)

    stat.topK(Int.MaxValue).foreach { case (item, count) => write(item); output.writeLong(count, true) }
  }

  private def readTopK(input: Input, sft: SimpleFeatureType, immutable: Boolean, version: Int): TopK[_] = {
    val attribute = version match {
      case 3     => input.readString()
      case 1 | 2 => sft.getDescriptor(input.readInt(true)).getLocalName
      case _ => throw new IllegalArgumentException(s"Invalid top-k serialization version: $version")
    }
    val binding = sft.getDescriptor(attribute).getType.getBinding
    val read = reader(input, binding)

    val summary = if (version > 1) {
      val size = input.readInt(true)
      val counters = (0 until size).map(_ => (read(), input.readLong(true)))
      StreamSummary[Any](TopK.StreamCapacity, counters)
    } else {
      import scala.collection.JavaConversions._
      val summaryBytes = input.readBytes(input.readInt(true))
      val clearspring = new com.clearspring.analytics.stream.StreamSummary[Any](summaryBytes)
      val geomesa = StreamSummary[Any](TopK.StreamCapacity)
      clearspring.topK(clearspring.size()).foreach(c => geomesa.offer(c.getItem, c.getCount))
      geomesa
    }

    if (immutable) {
      new TopK[Any](sft, attribute, summary) with ImmutableStat
    } else {
      new TopK[Any](sft, attribute, summary)
    }
  }

  private def writeHistogram(output: Output, sft: SimpleFeatureType, stat: Histogram[_]): Unit = {
    output.writeAscii(stat.property)
    output.writeInt(stat.length, true)

    val write = writer(output, sft.getDescriptor(stat.property).getType.getBinding)
    write(stat.bounds._1)
    write(stat.bounds._2)

    writeCountArray(output, stat.bins.counts)
  }

  private def readHistogram(input: Input, sft: SimpleFeatureType, immutable: Boolean, version: Int): Histogram[_] = {
    val attribute = version match {
      case 2 => input.readString
      case 1 => sft.getDescriptor(input.readInt(true)).getLocalName
      case _ => throw new IllegalArgumentException(s"Invalid group by serialization version: $version")
    }
    val length = input.readInt(true)

    val binding = sft.getDescriptor(attribute).getType.getBinding
    val read = reader(input, binding)

    val min = read()
    val max = read()

    val defaults = MinMaxDefaults[Any](binding)
    val classTag = ClassTag[Any](binding)
    val stat = if (immutable) {
      new Histogram[Any](sft, attribute, length, (min, max))(defaults, classTag) with ImmutableStat
    } else {
      new Histogram[Any](sft, attribute, length, (min, max))(defaults, classTag)
    }

    readCountArray(input, stat.bins.counts)

    stat
  }

  private def writeZ3Histogram(output: Output, sft: SimpleFeatureType, stat: Z3Histogram): Unit = {
    output.writeAscii(stat.geom)
    output.writeAscii(stat.dtg)
    output.writeAscii(stat.period.toString)
    output.writeInt(stat.length, true)

    val bins = stat.binMap.filter(_._2.counts.exists(_ != 0L))

    output.writeInt(bins.size, true)

    bins.foreach { case (w, bin) =>
      output.writeShort(w)
      writeCountArray(output, bin.counts)
    }
  }

  private def readZ3Histogram(input: Input, sft: SimpleFeatureType, immutable: Boolean, version: Int): Z3Histogram = {
    val Seq(geom, dtg) = version match {
      case 3     => Seq.fill(2)(input.readString())
      case 1 | 2 => Seq.fill(2)(sft.getDescriptor(input.readInt(true)).getLocalName)
      case _ => throw new IllegalArgumentException(s"Invalid z3 histogram serialization version: $version")
    }
    val period = if (version > 1) { TimePeriod.withName(input.readString()) } else { TimePeriod.Week }
    val length = input.readInt(true)

    val stat = if (immutable) {
      new Z3Histogram(sft, geom, dtg, period, length) with ImmutableStat
    } else {
      new Z3Histogram(sft, geom, dtg, period, length)
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

  private def writeFrequency(output: Output, sft: SimpleFeatureType, stat: Frequency[_]): Unit = {
    output.writeAscii(stat.property)
    output.writeAscii(stat.dtg.orNull)
    output.writeAscii(stat.period.toString)
    output.writeInt(stat.precision, true)
    output.writeDouble(stat.eps)
    output.writeDouble(stat.confidence)

    val sketches = stat.sketchMap.filter(_._2.size > 0)
    output.writeInt(sketches.size, true)

    sketches.foreach { case (w, sketch) =>
      output.writeShort(w)
      var i = 0
      while (i < sketch.table.length) {
        writeCountArray(output, sketch.table(i))
        i += 1
      }
      output.writeLong(sketch.size, true)
    }
  }

  private def readFrequency(input: Input, sft: SimpleFeatureType, immutable: Boolean, version: Int): Frequency[_] = {
    def name(i: Int): String = sft.getDescriptor(i).getLocalName
    val (attribute, dtg) = version match {
      case 3     => (input.readString(), Option(input.readString()))
      case 1 | 2 => (name(input.readInt(true)), Some(input.readInt(true)).filter(_ != -1).map(name))
      case _ => throw new IllegalArgumentException(s"Invalid frequency serialization version: $version")
    }
    val period = if (version > 1) { TimePeriod.withName(input.readString()) } else { TimePeriod.Week }
    val precision = input.readInt(true)
    val eps = input.readDouble()
    val confidence = input.readDouble()

    val binding = sft.getDescriptor(attribute).getType.getBinding
    val stat = if (immutable) {
      new Frequency[Any](sft, attribute, dtg, period, precision, eps, confidence)(ClassTag[Any](binding)) with ImmutableStat
    } else {
      new Frequency[Any](sft, attribute, dtg, period, precision, eps, confidence)(ClassTag[Any](binding))
    }

    val sketchCount = input.readInt(true)
    var c = 0
    while (c < sketchCount) {
      val week = input.readShort
      val sketch = stat.newSketch
      stat.sketchMap.put(week, sketch)
      var i = 0
      while (i < sketch.table.length) {
        readCountArray(input, sketch.table(i))
        i += 1
      }
      sketch._size = input.readLong(true)
      c += 1
    }

    stat
  }

  private def writeZ3Frequency(output: Output, sft: SimpleFeatureType, stat: Z3Frequency): Unit = {
    output.writeAscii(stat.geom)
    output.writeAscii(stat.dtg)
    output.writeAscii(stat.period.toString)
    output.writeInt(stat.precision, true)
    output.writeDouble(stat.eps)
    output.writeDouble(stat.confidence)

    val sketches = stat.sketches.filter(_._2.size > 0)
    output.writeInt(sketches.size, true)

    sketches.foreach { case (w, sketch) =>
      output.writeShort(w)
      var i = 0
      while (i < sketch.table.length) {
        writeCountArray(output, sketch.table(i))
        i += 1
      }

      output.writeLong(sketch.size, true)
    }
  }

  private def readZ3Frequency(input: Input, sft: SimpleFeatureType, immutable: Boolean, version: Int): Z3Frequency = {
    val Seq(geom, dtg) = version match {
      case 3     => Seq.fill(2)(input.readString())
      case 1 | 2 => Seq.fill(2)(sft.getDescriptor(input.readInt(true)).getLocalName)
      case _ => throw new IllegalArgumentException(s"Invalid frequency serialization version: $version")
    }
    val period = if (version > 1) { TimePeriod.withName(input.readString()) } else { TimePeriod.Week }
    val precision = input.readInt(true)
    val eps = input.readDouble()
    val confidence = input.readDouble()

    val stat = if (immutable) {
      new Z3Frequency(sft, geom, dtg, period, precision, eps, confidence) with ImmutableStat
    } else {
      new Z3Frequency(sft, geom, dtg, period, precision, eps, confidence)
    }

    val numSketches = input.readInt(true)
    var sketchCount = 0

    while (sketchCount < numSketches) {
      val sketch = stat.newSketch
      stat.sketches.put(input.readShort, sketch)
      var i = 0
      while (i < sketch.table.length) {
        readCountArray(input, sketch.table(i))
        i += 1
      }

      sketch._size = input.readLong(true)

      sketchCount += 1
    }

    stat
  }

  private def writeIteratorStackCount(output: Output, stat: IteratorStackCount): Unit =
    output.writeLong(stat.counter, true)

  private def readIteratorStackCount(input: Input, sft: SimpleFeatureType, immutable: Boolean): IteratorStackCount = {
    val stat = if (immutable) {
      new IteratorStackCount(sft) with ImmutableStat
    } else {
      new IteratorStackCount(sft)
    }
    stat.counter = input.readLong(true)
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
