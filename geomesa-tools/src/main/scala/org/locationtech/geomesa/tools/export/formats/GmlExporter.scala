/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import net.opengis.wfs.WfsFactory
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator}
import org.geotools.data.store.{DataFeatureCollection, ReTypingFeatureCollection}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.wfs.WFSConfiguration
import org.geotools.xsd.Encoder
import org.locationtech.geomesa.tools.`export`.formats.FeatureExporter.ExportStream
import org.locationtech.geomesa.tools.export.formats.FeatureExporter.ByteCounterExporter
import org.locationtech.geomesa.tools.export.formats.GmlExporter.AsyncFeatureCollection

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}
import javax.xml.namespace.QName

/**
  * GML exporter implementation.
  *
  * The geotools GML export classes only support encoding a feature collection. To support our usage
  * pattern (start, export n times, end), we create an asynchronous feature collection and do the actual
  * encoding in a separate thread. The encoder thread will block until there are more features to export,
  * so that we only get a single feature collection in the xml.
  *
  * @param stream output stream
  * @param configuration wfs configuration (gml3 vs gml2)
  */
class GmlExporter private (stream: ExportStream, configuration: WFSConfiguration)
    extends ByteCounterExporter(stream) {

  private val encoder: Encoder = {
    val props = configuration.getProperties.asInstanceOf[java.util.Set[QName]]
    props.add(org.geotools.gml2.GMLConfiguration.OPTIMIZED_ENCODING)
    props.add(org.geotools.gml2.GMLConfiguration.NO_FEATURE_BOUNDS)
    val e = new Encoder(configuration)
    e.getNamespaces.declarePrefix("geomesa", "http://geomesa.org")
    e.setEncoding(StandardCharsets.UTF_8)
    e.setIndenting(true)
    e
  }

  private val es = Executors.newSingleThreadExecutor()
  private var fc: AsyncFeatureCollection = _

  override def start(sft: SimpleFeatureType): Unit = {
    fc = new AsyncFeatureCollection(sft)
    val features = if (sft.getName.getNamespaceURI != null) { fc } else {
      val builder = new SimpleFeatureTypeBuilder()
      builder.init(sft)
      builder.setNamespaceURI("http://geomesa.org")
      new ReTypingFeatureCollection(fc, builder.buildFeatureType())
    }
    val collection = WfsFactory.eINSTANCE.createFeatureCollectionType()
    collection.getFeature.asInstanceOf[java.util.List[SimpleFeatureCollection]].add(features)

    def encode(): Unit = encoder.encode(collection, org.geotools.wfs.WFS.FeatureCollection, stream.os)

    val runnable = new Runnable() {
      override def run(): Unit = {
        if (System.getProperty(GmlExporter.TransformerProperty) != null) { encode() } else {
          // explicitly set the default java transformer, to avoid picking up saxon (which causes errors)
          // the default class is hard-coded in javax.xml.transform.TransformerFactory.newInstance() ...

          // TODO this may fail in java 9?
          System.setProperty(GmlExporter.TransformerProperty,
            "com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl")
          try { encode() } finally {
            System.clearProperty(GmlExporter.TransformerProperty)
          }
        }
      }
    }

    es.execute(runnable)
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L
    val counting = features.map { f => count += 1; f }
    while (counting.nonEmpty) {
      // export in chunks of 100 so that the exporter thread gets notified and doesn't keep blocking
      fc.addAsync(counting.take(100))
    }
    Some(count)
  }

  override def close(): Unit = {
    try {
      if (fc != null) {
        fc.endAsync()
      }
      es.shutdown()
      es.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
    } finally {
      stream.close()
    }
  }
}

object GmlExporter {

  private val TransformerProperty = classOf[javax.xml.transform.TransformerFactory].getName

  /**
    * Create a GML3 exporter
    *
    * @param stream output stream
    * @return
    */
  def apply(stream: ExportStream): GmlExporter =
    new GmlExporter(stream, new org.geotools.wfs.v1_1.WFSConfiguration())

  /**
    * Create a GML2 exporter
    *
    * @param stream output stream
    * @return
    */
  def gml2(stream: ExportStream): GmlExporter =
    new GmlExporter(stream, new org.geotools.wfs.v1_0.WFSConfiguration_1_0())

  /**
    * Feature collection that lets us add additional features in an asynchronous fashion. The consumer
    * thread will be blocked on calls to 'hasNext' until the producer thread adds features or indicates
    * completion
    *
    * @param sft simple feature type
    */
  private class AsyncFeatureCollection(sft: SimpleFeatureType) extends DataFeatureCollection(null, sft) {

    private val buffer = new ConcurrentLinkedQueue[SimpleFeature]()
    private val done = new AtomicBoolean(false)
    private val lock = new ReentrantLock()
    private val condition = lock.newCondition()

    private val iter: SimpleFeatureIterator = new SimpleFeatureIterator() {

      private var current: SimpleFeature = _

      override def hasNext: Boolean = {
        if (current != null) {
          return true
        }
        lock.lock()
        try {
          current = buffer.poll()
          // note: we need to loop here to skip 'spurious wake-ups'
          while (current == null) {
            if (done.get) {
              return false
            }
            condition.await()
            current = buffer.poll()
          }
          true
        } finally {
          lock.unlock()
        }
      }

      override def next(): SimpleFeature = {
        // note: we shouldn't need to synchronize this as next/hasNext should be a single caller thread
        val result = current
        current = null
        result
      }

      override def close(): Unit = endAsync()
    }

    /**
      * Add features to be returned from this feature collection
      *
      * @param features features
      */
    def addAsync(features: Iterator[SimpleFeature]): Unit = {
      lock.lock()
      try {
        features.foreach(buffer.add)
        condition.signal()
      } finally {
        lock.unlock()
      }
    }

    /**
      * Signal that there are no more features that will be added
      */
    def endAsync(): Unit = {
      lock.lock()
      try {
        done.set(true)
        condition.signal()
      } finally {
        lock.unlock()
      }
    }

    override def features(): SimpleFeatureIterator = iter

    override def getBounds: ReferencedEnvelope = org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope
    override def getCount: Int = 0
  }
}
