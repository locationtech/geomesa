package org.locationtech.geomesa.utils.index

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.index.quadtree.Quadtree
import org.geotools.data.store.FilteringFeatureIterator
import org.geotools.geometry.jts.JTS
import org.locationtech.geomesa.utils.geotools.{DFR, DFI, FR}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.spatial.{BBOX, BinarySpatialOperator, Within}

trait QuadTreeFeatureStore {

  def qt: Quadtree
  def sft: SimpleFeatureType

  def within(w: Within): FR = {
    val (_, geomLit) = splitBinOp(w)
    val geom = geomLit.evaluate(null).asInstanceOf[Geometry]
    val res = qt.query(geom.getEnvelopeInternal)
    val fiter = new DFI(res.asInstanceOf[java.util.List[SimpleFeature]].iterator)
    val filt = new FilteringFeatureIterator[SimpleFeature](fiter, w)
    new DFR(sft, filt)
  }

  def bbox(b: BBOX): FR = {
    val bounds = JTS.toGeometry(b.getBounds)
    val res = qt.query(bounds.getEnvelopeInternal)
    val fiter = new DFI(res.asInstanceOf[java.util.List[SimpleFeature]].iterator)
    val filt = new FilteringFeatureIterator[SimpleFeature](fiter, b)
    new DFR(sft, filt)
  }

  def splitBinOp(binop: BinarySpatialOperator): (PropertyName, Literal) =
    binop.getExpression1 match {
      case pn: PropertyName => (pn, binop.getExpression2.asInstanceOf[Literal])
      case l: Literal       => (binop.getExpression2.asInstanceOf[PropertyName], l)
    }

}
