package geomesa.core.index

import com.vividsolutions.jts.geom.Polygon
import geomesa.core._
import geomesa.core.data._
import geomesa.core.filter._
import geomesa.core.index.QueryHints._
import geomesa.core.iterators.{FEATURE_ENCODING, _}
import geomesa.core.util.{CloseableIterator, SelfClosingBatchScanner}
import java.util.Map.Entry
import org.apache.accumulo.core.client.{BatchScanner, IteratorSetting}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.user.RegExFilter
import org.apache.hadoop.io.Text
import org.apache.log4j.Logger
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.Interval
import org.opengis.feature.simple.SimpleFeatureType
import scala.collection.JavaConversions._
import scala.util.Random

object IndexQueryPlanner {
  val iteratorPriority_RowRegex                       = 0
  val iteratorPriority_ColFRegex                      = 100
  val iteratorPriority_SpatioTemporalIterator         = 200
  val iteratorPriority_SimpleFeatureFilteringIterator = 300
}

import geomesa.core.index.IndexQueryPlanner._

case class IndexQueryPlanner(keyPlanner: KeyPlanner,
                             cfPlanner: ColumnFamilyPlanner,
                             schema:String,
                             featureType: SimpleFeatureType,
                             featureEncoder: SimpleFeatureEncoder) {

  private val log = Logger.getLogger(classOf[IndexQueryPlanner])

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

  // As a pre-processing step, we examine the query/filter and split it into multiple queries.
  // TODO: Work to make the queries non-overlapping.
  def getIterator(buildBatchScanner: () => BatchScanner, query: Query) : CloseableIterator[Entry[Key,Value]] = {
    val ff = CommonFactoryFinder.getFilterFactory2
    val queries: Iterator[Query] =
      if(query.getHints.containsKey(BBOX_KEY)) {
        val env = query.getHints.get(BBOX_KEY).asInstanceOf[ReferencedEnvelope]
        val q1 = new Query(featureType.getTypeName, ff.bbox(ff.property(featureType.getGeometryDescriptor.getLocalName), env))
        Iterator(DataUtilities.mixQueries(q1, query, "geomesa.mixed.query"))
      } else splitQueryOnOrs(query)

    queries.flatMap(runQuery(buildBatchScanner, _))
  }
  
  def splitQueryOnOrs(query: Query): Iterator[Query] = {
    val originalFilter = query.getFilter

    val rewrittenFilter = rewriteFilter(originalFilter)
    
    val orSplitter = new OrSplittingFilter
    val splitFilters = orSplitter.visit(rewrittenFilter, null)

    // Let's just check quickly to see if we can eliminate any duplicates.
    val filters = splitFilters.distinct

    filters.map { filter =>
      val q = new Query(query)
      q.setFilter(filter)
      q
    }.toIterator
  }

  // Strategy:
  // 1. Inspect the query
  // 2. Set up the base iterators/scans.
  // 3. Set up the rest of the iterator stack.
  private def runQuery(buildBatchScanner: () => BatchScanner, query: Query) = {
    val bs: BatchScanner = buildBatchScanner()

    val filterVisitor = new FilterToAccumulo(featureType)
    val rewrittenCQL = filterVisitor.visit(query)
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

    if(log.isTraceEnabled) {
      log.trace("Configuring batch scanner: ")
      log.trace("Poly: "+ opoly.getOrElse("No poly"))
      log.trace("Interval: " + oint.getOrElse("No interval"))
      log.trace("Filter: " + Option(filter).getOrElse("No Filter"))
      log.trace("ECQL: " + Option(ecql).getOrElse("No ecql"))
      log.trace("Query: " + Option(query).getOrElse("no query"))
    }

    val iteratorConfig = IteratorTrigger.chooseIterator(ecql, query, featureType)

    iteratorConfig.iterator match {
      case IndexOnlyIterator  =>
        val transformedSFType = transformedSimpleFeatureType(query).getOrElse(featureType)
        configureIndexIterator(bs, opoly, oint, query, transformedSFType)
      case SpatioTemporalIterator =>
        configureSpatioTemporalIntersectingIterator(bs, opoly, oint, featureType)
    }

    if (iteratorConfig.useSFFI) {
      configureSimpleFeatureFilteringIterator(bs, featureType, ecql, query, poly)
    }

    // NB: Since we are (potentially) gluing multiple batch scanner iterators together,
    //  we wrap our calls in a SelfClosingBatchScanner.
    SelfClosingBatchScanner(bs)
  }

  def configureFeatureEncoding(cfg: IteratorSetting) =
    cfg.addOption(FEATURE_ENCODING, featureEncoder.getName)

  def configureFeatureType(cfg: IteratorSetting, featureType: SimpleFeatureType) {
    val encodedSimpleFeatureType = DataUtilities.encodeType(featureType)
    cfg.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, encodedSimpleFeatureType)
    cfg.encodeUserData(featureType.getUserData, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
  }

  // returns the SimpleFeatureType for the query's transform
  def transformedSimpleFeatureType(query: Query): Option[SimpleFeatureType] = {
    Option(query.getHints.get(TRANSFORM_SCHEMA)).map {_.asInstanceOf[SimpleFeatureType]}
  }

  // store transform information into an Iterator's settings
  def configureTransforms(query:Query,cfg: IteratorSetting) =
    for {
      transformOpt  <- Option(query.getHints.get(TRANSFORMS))
      transform     = transformOpt.asInstanceOf[String]
      _             = cfg.addOption(GEOMESA_ITERATORS_TRANSFORM, transform)
      sfType        <- transformedSimpleFeatureType(query)
      encodedSFType = DataUtilities.encodeType(sfType)
      _             = cfg.addOption(GEOMESA_ITERATORS_TRANSFORM_SCHEMA, encodedSFType)
    } yield Unit

  // establishes the regular expression that defines (minimally) acceptable rows
  def configureRowRegexIterator(bs: BatchScanner, regex: String) {
    val name = "regexRow-" + randomPrintableString(5)
    val cfg = new IteratorSetting(iteratorPriority_RowRegex, name, classOf[RegExFilter])
    RegExFilter.setRegexs(cfg, regex, null, null, null, false)
    bs.addScanIterator(cfg)
  }

  // returns an iterator over [key,value] pairs where the key is taken from the index row and the value is a SimpleFeature,
  // which is either read directory from the data row  value or generated from the encoded index row value
  // -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureIndexIterator(bs: BatchScanner,
                             poly: Option[Polygon],
                             interval: Option[Interval],
                             query: Query,
                             featureType: SimpleFeatureType) {
    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
      "within-" + randomPrintableString(5),classOf[IndexIterator])
    IndexIterator.setOptions(cfg, schema, poly, interval)
    configureFeatureType(cfg, featureType)
    configureFeatureEncoding(cfg)
    bs.addScanIterator(cfg)
  }

  // returns only the data entries -- no index entries -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureSpatioTemporalIntersectingIterator(bs: BatchScanner,
                                                  poly: Option[Polygon],
                                                  interval: Option[Interval],
                                                  featureType: SimpleFeatureType) {
    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
      "within-" + randomPrintableString(5),
      classOf[SpatioTemporalIntersectingIterator])
    SpatioTemporalIntersectingIterator.setOptions(cfg, schema, poly, interval)
    configureFeatureType(cfg, featureType)
    bs.addScanIterator(cfg)
  }
  // assumes that it receives an iterator over data-only entries, and aggregates
  // the values into a map of attribute, value pairs
  def configureSimpleFeatureFilteringIterator(bs: BatchScanner,
                                              simpleFeatureType: SimpleFeatureType,
                                              ecql: Option[String],
                                              query: Query,
                                              poly: Polygon = null) {

    val density: Boolean = query.getHints.containsKey(DENSITY_KEY)

    val clazz =
      if(density) classOf[DensityIterator]
      else classOf[SimpleFeatureFilteringIterator]

    val cfg = new IteratorSetting(iteratorPriority_SimpleFeatureFilteringIterator,
      "sffilter-" + randomPrintableString(5),
      clazz)

    configureFeatureEncoding(cfg)
    configureTransforms(query,cfg)
    configureFeatureType(cfg, simpleFeatureType)
    ecql.foreach(SimpleFeatureFilteringIterator.setECQLFilter(cfg, _))

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
