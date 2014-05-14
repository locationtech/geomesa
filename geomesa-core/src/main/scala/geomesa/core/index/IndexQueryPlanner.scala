package geomesa.core.index

import com.vividsolutions.jts.geom.Polygon
import geomesa.core.data._
import geomesa.core.index.QueryHints._
import geomesa.core.iterators.FEATURE_ENCODING
import geomesa.core.iterators._
import java.util.Map.Entry
import java.util.{Iterator => JIterator}
import org.apache.accumulo.core.client.{IteratorSetting, BatchScanner}
import org.apache.accumulo.core.data.{Value, Key}
import org.apache.accumulo.core.iterators.Combiner
import org.apache.accumulo.core.iterators.user.RegExFilter
import org.apache.hadoop.io.Text
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.Interval
import org.opengis.feature.simple.SimpleFeatureType
import scala.collection.JavaConversions._
import scala.util.Random

case class IndexQueryPlanner(keyPlanner: KeyPlanner,
                             cfPlanner: ColumnFamilyPlanner,
                             schema:String,
                             featureType: SimpleFeatureType,
                             featureEncoder: SimpleFeatureEncoder) extends ApportionedRanges {

  def buildFilter(poly: Polygon, interval: Interval): KeyPlanningFilter =
    (IndexSchema.somewhere(poly), IndexSchema.somewhen(interval)) match {
      case (None, None)       =>    AcceptEverythingFilter
      case (None, Some(i))    =>
        if (i.getStart == i.getEnd) DateFilter(i.getStart)
        else                        DateRangeFilter(i.getStart, i.getEnd)
      case (Some(p), None)    =>    SpatialFilter(poly)
      case (Some(p), Some(i)) =>
        if (i.getStart == i.getEnd) SpatialDateFilter(p, i.getStart)
        else                        SpatialDateRangeFilter(p, i.getStart, i.getEnd)
    }


  def netPolygon(poly: Polygon): Polygon = poly match {
    case null => null
    case p if p.covers(IndexSchema.everywhere) =>
      IndexSchema.everywhere
    case p if IndexSchema.everywhere.covers(p) => p
    case _ => poly.intersection(IndexSchema.everywhere).
      asInstanceOf[Polygon]
  }

  def netInterval(interval: Interval): Interval = interval match {
    case null => null
    case _    => IndexSchema.everywhen.overlap(interval)
  }

  // Strategy:
  // 1. Inspect the query
  // 2. Set up the base iterators/scans.
  // 3. Set up the rest of the iterator stack.
  def getIterator(bs: BatchScanner, query: Query) : JIterator[Entry[Key,Value]] = {

    val ff = CommonFactoryFinder.getFilterFactory2
    val derivedQuery =
      if(query.getHints.containsKey(BBOX_KEY)) {
        val env = query.getHints.get(BBOX_KEY).asInstanceOf[ReferencedEnvelope]
        val q1 = new Query(featureType.getTypeName, ff.bbox(ff.property(featureType.getGeometryDescriptor.getLocalName), env))
        DataUtilities.mixQueries(q1, query, "geomesa.mixed.query")
      } else query

    val simpleFeatureType = DataUtilities.encodeType(featureType)
    val filterVisitor = new FilterToAccumulo(featureType)
    val rewrittenCQL = filterVisitor.visit(derivedQuery)
    val ecql = Option(ECQL.toCQL(rewrittenCQL))

    val spatial = filterVisitor.spatialPredicate
    val temporal = filterVisitor.temporalPredicate

    // standardize the two key query arguments:  polygon and date-range
    val poly = netPolygon(spatial)
    val interval = netInterval(temporal)

    // figure out which of our various filters we intend to use
    // based on the arguments passed in
    val filter = buildFilter(poly, interval)

    val opoly = IndexSchema.somewhere(poly)
    val oint  = IndexSchema.somewhen(interval)

    // set up row ranges and regular expression filter
    planQuery(bs, filter)

    // Configure STII
    configureSpatioTemporalIntersectingIterator(bs, opoly, oint)

    // always set up the aggregating-combiner and simple-feature filtering iterator
    configureAttributeAggregator(bs)

    configureSimpleFeatureFilteringIterator(bs, simpleFeatureType, ecql, query, poly)

    bs.iterator()
  }

  def configureFeatureEncoding(cfg: IteratorSetting) =
    cfg.addOption(FEATURE_ENCODING, featureEncoder.getName)

  // establishes the regular expression that defines (minimally) acceptable rows
  def configureRowRegexIterator(bs: BatchScanner, regex: String) {
    val name = "regexRow-" + randomPrintableString(5)
    val cfg = new IteratorSetting(iteratorPriority_RowRegex, name, classOf[RegExFilter])
    RegExFilter.setRegexs(cfg, regex, null, null, null, false)
    bs.addScanIterator(cfg)
  }

  // returns only the data entries -- no index entries -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureSpatioTemporalIntersectingIterator(bs: BatchScanner, poly: Option[Polygon], interval: Option[Interval]) {
    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
      "within-" + randomPrintableString(5),
      classOf[SpatioTemporalIntersectingIterator])
    SpatioTemporalIntersectingIterator.setOptions(
      cfg, schema, poly, interval, featureType)
    bs.addScanIterator(cfg)
  }

  // transforms:  (index key, (attribute,encoded feature)) -> (index key, encoded feature)
  // (there should only be one data-row per entry:  the encoded SimpleFeature)
  def configureAttributeAggregator(bs: BatchScanner) {
    val cfg = new IteratorSetting(iteratorPriority_AttributeAggregator,
      "aggrcomb-" + randomPrintableString(5),
      classOf[AggregatingCombiner])
    Combiner.setCombineAllColumns(cfg, true)
    bs.addScanIterator(cfg)
  }

  // assumes that it receives an iterator over data-only entries, and aggregates
  // the values into a map of attribute, value pairs
  def configureSimpleFeatureFilteringIterator(bs: BatchScanner,
                                              simpleFeatureType: String,
                                              ecql: Option[String],
                                              query: Query,
                                              poly: Polygon = null) {
    val transforms = Option(query.getHints.get(TRANSFORMS)).map(_.asInstanceOf[String])
    val transformSchema = Option(query.getHints.get(TRANSFORM_SCHEMA)).map(_.asInstanceOf[SimpleFeatureType])

    val density: Boolean = query.getHints.containsKey(DENSITY_KEY)

    val clazz =
      if(density) classOf[DensityIterator]
      else classOf[SimpleFeatureFilteringIterator]

    val cfg = new IteratorSetting(iteratorPriority_SimpleFeatureFilteringIterator,
      "sffilter-" + randomPrintableString(5),
      clazz)

    configureFeatureEncoding(cfg)
    SimpleFeatureFilteringIterator.setFeatureType(cfg, simpleFeatureType)
    ecql.foreach(SimpleFeatureFilteringIterator.setECQLFilter(cfg, _))
    transforms.foreach(SimpleFeatureFilteringIterator.setTransforms(cfg, _, transformSchema))

    if(density) {
      val width = query.getHints.get(WIDTH_KEY).asInstanceOf[Integer]
      val height =  query.getHints.get(HEIGHT_KEY).asInstanceOf[Integer]
      DensityIterator.configure(cfg, poly, width, height)
    }

    bs.addScanIterator(cfg)
  }

  def randomPrintableString(length:Int=5) : String = (1 to length).
    map(i => Random.nextPrintableChar()).mkString

  def planQuery(bs: BatchScanner, filter: KeyPlanningFilter): BatchScanner = {
    val keyPlan = keyPlanner.getKeyPlan(filter)
    val columnFamilies = cfPlanner.getColumnFamiliesToFetch(filter)

    // always try to use range(s) to remove easy false-positives
    val accRanges: Seq[org.apache.accumulo.core.data.Range] = keyPlan match {
      case KeyRanges(ranges) => ranges.map(r => new org.apache.accumulo.core.data.Range(r.start, r.end))
      case _ => Seq(new org.apache.accumulo.core.data.Range())
    }
    bs.setRanges(accRanges)

    // always try to set a RowID regular expression
    //@TODO this is broken/disabled as a result of the KeyTier
    keyPlan.toRegex match {
      case KeyRegex(regex) => configureRowRegexIterator(bs, regex)
      case _ => // do nothing
    }

    // if you have a list of distinct column-family entries, fetch them
    columnFamilies match {
      case KeyList(keys) => keys.foreach(cf => bs.fetchColumnFamily(new Text(cf)))
      case _ => // do nothing
    }

    bs
  }
}
