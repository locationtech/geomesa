package org.locationtech.geomesa.core.index

import org.geotools.data.Query
import org.locationtech.geomesa.core.data.AccumuloConnectorCreator
import org.locationtech.geomesa.core.index.QueryHints._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.{Filter, Id, PropertyIsLike, PropertyIsEqualTo}

object Decider {


  def chooseStrategy(acc: AccumuloConnectorCreator,
                     sft: SimpleFeatureType,
                     query: Query //filter: Filter, ///derivedQuery: Query,
                     //isDensity: Boolean,
                     /*output: ExplainerOutputType*/): Strategy = {


    if(acc.catalogTableFormat(sft)) {
      chooseNewStrategy(sft, query)
    } else {
      // datastore doesn't support attr index use spatiotemporal only
      new StIdxStrategy
    }

    //new StIdxStrategy
  }

  def chooseNewStrategy(sft: SimpleFeatureType, query: Query): Strategy = {
    // If we have attr index table try it

    val filter = query.getFilter
    val isDensity = query.getHints.containsKey(BBOX_KEY)

    filter match {
      case isEqualTo: PropertyIsEqualTo if !isDensity && attrIdxQueryEligible(isEqualTo, sft) =>
        new AttributeEqualsIdxStrategy // attrIdxEqualToQuery(acc, derivedQuery, isEqualTo, filterVisitor, output)

      case like: PropertyIsLike if !isDensity =>
        if (attrIdxQueryEligible(like, sft) && likeEligible(like))
          new AttributeLikeIdxStrategy // attrIdxLikeQuery(acc, derivedQuery, like, filterVisitor, output)
        else
          new StIdxStrategy

      case idFilter: Id =>
        new RecordIdxStrategy // recordIdFilter(acc, idFilter, output)

      case cql =>
        new StIdxStrategy
    }
  }

  // TODO try to use wildcard values from the Filter itself
  // Currently pulling the wildcard values from the filter
  // leads to inconsistent results...so use % as wildcard
  val MULTICHAR_WILDCARD = "%"
  val SINGLE_CHAR_WILDCARD = "_"
  val NULLBYTE = Array[Byte](0.toByte)

  /* Like queries that can be handled by current reverse index */
  def likeEligible(filter: PropertyIsLike) = containsNoSingles(filter) && trailingOnlyWildcard(filter)

  /* contains no single character wildcards */
  def containsNoSingles(filter: PropertyIsLike) =
    !filter.getLiteral.replace("\\\\", "").replace(s"\\$SINGLE_CHAR_WILDCARD", "").contains(SINGLE_CHAR_WILDCARD)

  def trailingOnlyWildcard(filter: PropertyIsLike) =
    (filter.getLiteral.endsWith(MULTICHAR_WILDCARD) &&
      filter.getLiteral.indexOf(MULTICHAR_WILDCARD) == filter.getLiteral.length - MULTICHAR_WILDCARD.length) ||
      filter.getLiteral.indexOf(MULTICHAR_WILDCARD) == -1


  import org.locationtech.geomesa.utils.geotools.Conversions._

  def attrIdxQueryEligible(filt: Filter, featureType: SimpleFeatureType): Boolean = filt match {
    case filter: PropertyIsEqualTo =>
      val one = filter.getExpression1
      val two = filter.getExpression2
      val prop = (one, two) match {
        case (p: PropertyName, _) => Some(p.getPropertyName)
        case (_, p: PropertyName) => Some(p.getPropertyName)
        case (_, _)               => None
      }
      prop.exists(featureType.getDescriptor(_).isIndexed)

    case filter: PropertyIsLike =>
      val prop = filter.getExpression.asInstanceOf[PropertyName].getPropertyName
      featureType.getDescriptor(prop).isIndexed
  }
}
