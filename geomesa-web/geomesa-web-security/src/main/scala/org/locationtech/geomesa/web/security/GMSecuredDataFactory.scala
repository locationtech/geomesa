package org.locationtech.geomesa.web.security

import com.google.common.collect.Maps
import org.apache.accumulo.core.security.{Authorizations, ColumnVisibility, VisibilityEvaluator}
import org.geoserver.security.WrapperPolicy
import org.geoserver.security.decorators.{DecoratingFeatureSource, DefaultSecureDataFactory}
import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureIterator}
import org.geotools.feature.collection.{FilteringSimpleFeatureCollection, FilteringSimpleFeatureIterator}
import org.geotools.feature.{DefaultFeatureCollection, FeatureCollection}
import org.locationtech.geomesa.core.security.SecurityUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{Filter, FilterVisitor}
import org.springframework.security.core.context.SecurityContextHolder

class GMSecuredDataFactory extends DefaultSecureDataFactory {

  override def secure(o: scala.Any, policy: WrapperPolicy): AnyRef =
    super.secure(o, policy) match {
      case ds: DataStore =>
        new GMSecureDataStore(ds)

      case fs: FeatureSource[SimpleFeatureType, SimpleFeature] =>
        new GMSecureFeatureSource(fs)

      case fc: SimpleFeatureCollection =>
        new GMSecureFeatureCollection(fc)

      case so =>
        so
    }


  override def getPriority: Int = 1
}

object GMSecuredDataFactory {
  import scala.collection.JavaConversions._

  def getAuthorizations: Authorizations = {
    val auths = SecurityContextHolder.getContext.getAuthentication.getAuthorities.map(_.getAuthority).toList
    new Authorizations(auths: _*)
  }

  def buildVisibilityEvaluator(): VisibilityEvaluator =
    new VisibilityEvaluator(getAuthorizations)
}

class GMSecureFeatureSource(delegate: FeatureSource[SimpleFeatureType, SimpleFeature])
  extends DecoratingFeatureSource[SimpleFeatureType, SimpleFeature](delegate) {

  override def getFeatures(query: Query): FeatureCollection[SimpleFeatureType, SimpleFeature] =
    new FilteringSimpleFeatureCollection(delegate.getFeatures(query), new VisibilityFilter(GMSecuredDataFactory.buildVisibilityEvaluator()))
}

class GMSecureFeatureCollection(delegate: SimpleFeatureCollection) extends DefaultFeatureCollection(delegate) {
  override def features(): SimpleFeatureIterator = new FilteringSimpleFeatureIterator(super.features(), new VisibilityFilter(GMSecuredDataFactory.buildVisibilityEvaluator()))
}

class GMSecureDataStore(delegate: DataStore) extends AbstractDataStore {

  override def getSchema(s: String): SimpleFeatureType = delegate.getSchema(s)

  override def getFeatureReader(s: String): FeatureReader[SimpleFeatureType, SimpleFeature] =
    getFeatureReader(new Query(s, Filter.INCLUDE), Transaction.AUTO_COMMIT)

  override def getFeatureReader(query: Query, transaction: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val delegateReader = delegate.getFeatureReader(query, transaction)
    new FilteringFeatureReader[SimpleFeatureType, SimpleFeature](delegateReader, new VisibilityFilter(GMSecuredDataFactory.buildVisibilityEvaluator()))
  }

  override def getTypeNames: Array[String] = delegate.getTypeNames
}

class VisibilityFilter(ve: VisibilityEvaluator) extends Filter {
  import scala.collection.JavaConversions._
  private val vizCache = Maps.newHashMap[String, java.lang.Boolean]()

  override def evaluate(o: Any): Boolean = {
    val viz = SecurityUtils.getVisibility(o.asInstanceOf[SimpleFeature])
    if(viz != null) {
      vizCache.getOrElseUpdate(viz, ve.evaluate(new ColumnVisibility(viz)))
      ve.evaluate(new ColumnVisibility(viz))
    } else {
      false
    }
  }

  override def accept(filterVisitor: FilterVisitor, o: AnyRef): AnyRef = o

}