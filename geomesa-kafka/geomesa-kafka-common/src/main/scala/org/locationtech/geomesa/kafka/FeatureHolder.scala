package org.locationtech.geomesa.kafka

import com.vividsolutions.jts.geom.Envelope
import org.opengis.feature.simple.SimpleFeature

case class FeatureHolder(sf: SimpleFeature, env: Envelope) {
  override def hashCode(): Int = sf.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: FeatureHolder => sf.equals(other.sf)
    case _ => false
  }
}
