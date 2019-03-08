/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2

import java.lang.reflect.InvocationTargetException
import java.nio.charset.Charset
import java.util.Collections

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValueFactory}
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.locationtech.geomesa.convert.Modes.{ErrorMode, ParseMode}
import org.locationtech.geomesa.convert.SimpleFeatureValidator.{HasDtgValidator, HasGeoValidator}
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicConfig, BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.AbstractConverterFactory.{ConverterConfigConvert, ConverterOptionsConvert, FieldConvert}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.features.serialization.ObjectType
import org.locationtech.geomesa.features.serialization.ObjectType.ObjectType
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeatureType
import pureconfig._
import pureconfig.error.{CannotConvert, ConfigReaderFailures}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * Abstract converter factory implementation. Subclasses need to implement `typeToProcess` and make available
  * pureconfig readers for the converter configuration
  */
abstract class AbstractConverterFactory[S <: AbstractConverter[_, C, F, O]: ClassTag,
                                        C <: ConverterConfig: ClassTag,
                                        F <: Field,
                                        O <: ConverterOptions: ClassTag]
    extends SimpleFeatureConverterFactory with LazyLogging {

  /**
    * The converter to use is identified by the 'type' field in the config, e.g. 'xml' or 'json'
    *
    * @return
    */
  protected def typeToProcess: String

  protected implicit def configConvert: ConverterConfigConvert[C]
  protected implicit def fieldConvert: FieldConvert[F]
  protected implicit def optsConvert: ConverterOptionsConvert[O]

  override def apply(sft: SimpleFeatureType, conf: Config): Option[SimpleFeatureConverter] = {
    if (!conf.hasPath("type") || !conf.getString("type").equalsIgnoreCase(typeToProcess)) { None } else {
      val (config, fields, opts) = try {
        val c = withDefaults(conf)
        val config = pureconfig.loadConfigOrThrow[C](c)
        val fields = pureconfig.loadConfigOrThrow[Seq[F]](c)
        val opts = pureconfig.loadConfigOrThrow[O](c)
        (config, fields, opts)
      } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid configuration: ${e.getMessage}")
      }
      opts.validators.init(sft)
      val args = Array(classOf[SimpleFeatureType], implicitly[ClassTag[C]].runtimeClass,
        classOf[Seq[F]], implicitly[ClassTag[O]].runtimeClass)
      val constructor = implicitly[ClassTag[S]].runtimeClass.getConstructor(args: _*)
      try {
        Some(constructor.newInstance(sft, config, fields, opts).asInstanceOf[SimpleFeatureConverter])
      } catch {
        case e: InvocationTargetException => throw e.getCause
      }
    }
  }

  /**
    * Add default paths to the config, and handle deprecated options
    *
    * @param conf config
    * @return
    */
  protected def withDefaults(conf: Config): Config = AbstractConverterFactory.standardDefaults(conf, logger)
}

object AbstractConverterFactory extends LazyLogging {

  import scala.collection.JavaConverters._

  val InferSampleSize: SystemProperty = SystemProperty("geomesa.convert.infer.sample", "100")

  def inferSampleSize: Int = InferSampleSize.toInt.getOrElse {
    // shouldn't ever happen since the default is a valid int
    throw new IllegalStateException("Could not determine sample size from system property")
  }

  /**
    * Validate an inferred type matches an existing type
    *
    * @param sft existing type
    * @param types inferred type
    * @return true if types match, otherwise false
    */
  def validateInferredType(sft: SimpleFeatureType, types: Seq[ObjectType], raiseError: Boolean = true): Boolean = {
    val existing = sft.getAttributeDescriptors.asScala.map(ObjectType.selectType).collect {
      case Seq(ObjectType.GEOMETRY, subtype) => subtype
      case t => t.head
    }
    if (existing == types) { true } else if (!raiseError) { false } else {
      throw new IllegalArgumentException(s"Simple feature type does not match inferred schema: " +
          s"\n\tExisting types: ${existing.mkString(", ")}" +
          s"\n\tInferred types: ${types.mkString(", ")}")
    }
  }

  /**
    * Handles common deprecated values and quoting of user data keys
    *
    * @param conf conf
    * @return
    */
  def standardDefaults(conf: Config, logger: => Logger): Config = {
    import scala.collection.JavaConverters._

    val updates = ArrayBuffer.empty[Config => Config]
    if (conf.hasPath("options.validation-mode")) {
      logger.warn(s"Using deprecated option 'validation-mode'. Prefer 'error-mode'")
      updates.append(c => c.withValue("options.error-mode", conf.getValue("options.validation-mode")))
    }
    if (conf.hasPath("options.validating")) {
      logger.warn(s"Using deprecated validation key 'validating'")
      val validators = if (conf.getBoolean("options.validating")) {
        ConfigValueFactory.fromIterable(Seq(HasGeoValidator.name, HasDtgValidator.name).asJava)
      } else {
        ConfigValueFactory.fromIterable(Collections.emptyList())
      }
      updates.append(c => c.withValue("options.validators", validators))
    }

    if (conf.hasPath("user-data")) {
      // re-write user data so that it doesn't have to be quoted
      val kvs = new java.util.HashMap[String, AnyRef]
      conf.getConfig("user-data").entrySet.asScala.foreach(e => kvs.put(e.getKey, e.getValue.unwrapped()))
      val fallback = ConfigFactory.empty().withValue("user-data", ConfigValueFactory.fromMap(kvs))
      updates.append(c => c.withoutPath("user-data").withFallback(fallback))
    }

    updates.foldLeft(conf)((c, mod) => mod.apply(c)).withFallback(ConfigFactory.load("base-converter-defaults"))
  }

  /**
    * Default pureconfig convert for a basic configuration, for converters that don't have
    * any additional config options
    */
  implicit object BasicConfigConvert extends ConverterConfigConvert[BasicConfig] {

    override protected def decodeConfig(cur: ConfigObjectCursor,
                                        `type`: String,
                                        idField: Option[Expression],
                                        caches: Map[String, Config],
                                        userData: Map[String, Expression]): Either[ConfigReaderFailures, BasicConfig] = {
      Right(BasicConfig(`type`, idField, caches, userData))
    }

    override protected def encodeConfig(config: BasicConfig, base: java.util.Map[String, AnyRef]): Unit = {}
  }

  /**
    * Default pureconfig convert for a basic field, for converters that don't have
    * any additional field options
    */
  implicit object BasicFieldConvert extends FieldConvert[BasicField] {

    override protected def decodeField(cur: ConfigObjectCursor,
                                       name: String,
                                       transform: Option[Expression]): Either[ConfigReaderFailures, BasicField] = {
      Right(BasicField(name, transform))
    }

    override protected def encodeField(field: BasicField, base: java.util.Map[String, AnyRef]): Unit = {}
  }

  /**
    * Default pureconfig convert for basic options, for converters that don't have
    * any additional options
    */
  implicit object BasicOptionsConvert extends ConverterOptionsConvert[BasicOptions] {

    override protected def decodeOptions(
        cur: ConfigObjectCursor,
        validators: SimpleFeatureValidator,
        parseMode: ParseMode,
        errorMode: ErrorMode,
        encoding: Charset): Either[ConfigReaderFailures, BasicOptions] = {
      Right(BasicOptions(validators, parseMode, errorMode, encoding))
    }

    override protected def encodeOptions(options: BasicOptions, base: java.util.Map[String, AnyRef]): Unit = {}
  }

  /**
    * Pureconfig convert that parses out basic config. Subclasses must implement `decodeConfig` and `encodeConfig`
    * to read/write any custom config
    *
    * @tparam C config class
    */
  abstract class ConverterConfigConvert[C <: ConverterConfig] extends ConfigConvert[C] with ExpressionConvert {

    protected def decodeConfig(cur: ConfigObjectCursor,
                               `type`: String,
                               idField: Option[Expression],
                               caches: Map[String, Config],
                               userData: Map[String, Expression]): Either[ConfigReaderFailures, C]

    protected def encodeConfig(config: C, base: java.util.Map[String, AnyRef]): Unit

    override def from(cur: ConfigCursor): Either[ConfigReaderFailures, C] = {
      for {
        obj      <- cur.asObjectCursor.right
        typ      <- obj.atKey("type").right.flatMap(_.asString).right
        idField  <- idFieldFrom(obj.atKeyOrUndefined("id-field")).right
        userData <- userDataFrom(obj.atKeyOrUndefined("user-data")).right
        caches   <- cachesFrom(obj.atKeyOrUndefined("caches")).right
        config   <- decodeConfig(obj, typ, idField, caches, userData).right
      } yield {
        config
      }
    }

    override def to(obj: C): ConfigObject = ConfigValueFactory.fromMap(configTo(obj))

    private def configTo(config: C): java.util.Map[String, AnyRef] = {
      val map = new java.util.HashMap[String, AnyRef]
      map.put("type", config.`type`)
      config.idField.foreach(f => map.put("id-field", f.toString))
      if (config.userData.nonEmpty) {
        map.put("user-data", config.userData.map { case (k, v) => (k, v.toString.asInstanceOf[AnyRef]) }.asJava)
      }
      if (config.caches.nonEmpty) {
        map.put("caches", config.caches.map { case (k, v) => (k, v.root().unwrapped()) })
      }
      encodeConfig(config, map)
      map
    }

    private def idFieldFrom(cur: ConfigCursor): Either[ConfigReaderFailures, Option[Expression]] = {
      if (cur.isUndefined) { Right(None) } else {
        for { expr <- exprFrom(cur).right } yield { Some(expr) }
      }
    }

    private def userDataFrom(cur: ConfigCursor): Either[ConfigReaderFailures, Map[String, Expression]] = {
      if (cur.isUndefined) { Right(Map.empty) } else {
        def merge(cur: ConfigObjectCursor): Either[ConfigReaderFailures, Map[String, Expression]] = {
          cur.map.foldLeft[Either[ConfigReaderFailures, Map[String, Expression]]](Right(Map.empty)) {
            case (map, (k, v)) => for { m <- map.right; d <- exprFrom(v).right } yield { m + (k -> d) }
          }
        }
        for { obj <- cur.asObjectCursor.right; data <- merge(obj).right } yield { data }
      }
    }

    private def cachesFrom(cur: ConfigCursor): Either[ConfigReaderFailures, Map[String, Config]] = {
      if (cur.isUndefined) { Right(Map.empty) } else {
        def merge(cur: ConfigObjectCursor): Either[ConfigReaderFailures, Map[String, Config]] = {
          cur.map.foldLeft[Either[ConfigReaderFailures, Map[String, Config]]](Right(Map.empty)) {
            case (map, (k, v)) => for { m <- map.right; c <- v.asObjectCursor.right } yield { m + (k -> c.value.toConfig) }
          }
        }
        for { obj <- cur.asObjectCursor.right; caches <- merge(obj).right } yield { caches }
      }
    }
  }

  /**
    * Pureconfig convert that parses out basic fields. Subclasses must implement `decodeField` and `encodeField`
    * to read/write any custom field values
    *
    * @tparam F field type
    */
  abstract class FieldConvert[F <: Field] extends ConfigConvert[Seq[F]] with ExpressionConvert {

    protected def decodeField(cur: ConfigObjectCursor,
                              name: String,
                              transform: Option[Expression]): Either[ConfigReaderFailures, F]

    protected def encodeField(field: F, base: java.util.Map[String, AnyRef]): Unit

    override def from(cur: ConfigCursor): Either[ConfigReaderFailures, Seq[F]] = {
      for {
        obj    <- cur.asObjectCursor.right
        fields <- obj.atKey("fields").right.flatMap(_.asListCursor).right.flatMap(fieldsFrom).right
      } yield {
        fields
      }
    }

    override def to(obj: Seq[F]): ConfigObject = {
      val map = new java.util.HashMap[String, AnyRef]
      map.put("fields", obj.map(fieldTo).asJava)
      ConfigValueFactory.fromMap(map)
    }

    private def fieldsFrom(cur: ConfigListCursor): Either[ConfigReaderFailures, Seq[F]] = {
      cur.list.foldLeft[Either[ConfigReaderFailures, Seq[F]]](Right(Seq.empty)) {
        (list, field) => for { li <- list.right; f <- fieldFrom(field).right } yield { li :+ f }
      }
    }

    private def fieldFrom(cur: ConfigCursor): Either[ConfigReaderFailures, F] = {
      def transformFrom(cur: ConfigCursor): Either[ConfigReaderFailures, Option[Expression]] = {
        if (cur.isUndefined) { Right(None) } else {
          exprFrom(cur).right.map(t => Some(t))
        }
      }

      for {
        obj       <- cur.asObjectCursor.right
        name      <- obj.atKey("name").right.flatMap(_.asString).right
        transform <- transformFrom(obj.atKeyOrUndefined("transform")).right
        field     <- decodeField(obj, name, transform).right
      } yield {
        field
      }
    }

    private def fieldTo(field: F): java.util.Map[String, AnyRef] = {
      val map = new java.util.HashMap[String, AnyRef]
      map.put("name", field.name)
      field.transforms.foreach(t => map.put("transform", t.toString))
      encodeField(field, map)
      map
    }
  }

  /**
    * Pureconfig convert that parses out basic options. Subclasses must implement `decodeOptions` and `encodeOptions`
    * to read/write any custom converter options
    *
    * @tparam O options class
    */
  abstract class ConverterOptionsConvert[O <: ConverterOptions] extends ConfigConvert[O] {

    protected def decodeOptions(
        cur: ConfigObjectCursor,
        validators: SimpleFeatureValidator,
        parseMode: ParseMode,
        errorMode: ErrorMode,
        encoding: Charset): Either[ConfigReaderFailures, O]

    protected def encodeOptions(options: O, base: java.util.Map[String, AnyRef]): Unit

    override def from(cur: ConfigCursor): Either[ConfigReaderFailures, O] = {
      for {
        obj     <- cur.asObjectCursor.right
        options <- obj.atKey("options").right.flatMap(_.asObjectCursor).right.flatMap(optionsFrom).right
      } yield {
        options
      }
    }

    override def to(obj: O): ConfigObject = {
      val map = new java.util.HashMap[String, AnyRef]
      map.put("options", optionsTo(obj))
      ConfigValueFactory.fromMap(map)
    }

    private def optionsFrom(cur: ConfigObjectCursor): Either[ConfigReaderFailures, O] = {

      def mergeValidators(cur: ConfigListCursor): Either[ConfigReaderFailures, SimpleFeatureValidator] = {
        val strings = cur.list.foldLeft[Either[ConfigReaderFailures, Seq[String]]](Right(Seq.empty)) {
          case (seq, v) => for { s <- seq.right; string <- v.asString.right } yield { s :+ string }
        }
        strings.right.flatMap { s =>
          try { Right(SimpleFeatureValidator(s)) } catch {
            case NonFatal(e) => cur.failed(CannotConvert(cur.value.toString, "SimpleFeatureValidator", e.getMessage))
          }
        }
      }

      def parse[T](key: String, values: Iterable[T]): Either[ConfigReaderFailures, T] = {
        cur.atKey(key).right.flatMap { value =>
          value.asString.right.flatMap { string =>
            values.find(_.toString.equalsIgnoreCase(string)) match {
              case Some(v) => Right(v.asInstanceOf[T])
              case None => value.failed(CannotConvert(value.value.toString, values.head.getClass.getSimpleName, s"Must be one of: ${values.mkString(", ")}"))
            }
          }
        }
      }

      if (cur.atKey("verbose").isRight) {
        logger.warn("'verbose' option is deprecated - please use logging levels instead")
      }

      for {
        validators <- cur.atKey("validators").right.flatMap(_.asListCursor).right.flatMap(mergeValidators).right
        parseMode  <- parse("parse-mode", ParseMode.values).right
        errorMode  <- parse("error-mode", ErrorMode.values).right
        encoding   <- cur.atKey("encoding").right.flatMap(_.asString).right.map(Charset.forName).right
        options    <- decodeOptions(cur, validators, parseMode, errorMode, encoding).right
      } yield {
        options
      }
    }

    private def optionsTo(options: O): java.util.Map[String, AnyRef] = {
      val map = new java.util.HashMap[String, AnyRef]
      map.put("parse-mode", options.parseMode.toString)
      map.put("error-mode", options.errorMode.toString)
      map.put("encoding", options.encoding.name)
      options.validators match {
        // use unapplySeq to extract names
        case SimpleFeatureValidator(names@_*) => map.put("validators", names.asJava)
      }
      encodeOptions(options, map)
      map
    }
  }

  /**
    * Convert a transformer expression
    */
  trait ExpressionConvert {
    protected def exprFrom(cur: ConfigCursor): Either[ConfigReaderFailures, Expression] = {
      def parse(expr: String): Either[ConfigReaderFailures, Expression] =
        try { Right(Expression(expr)) } catch {
          case NonFatal(e) => cur.failed(CannotConvert(cur.value.toString, "Expression", e.getMessage))
        }
      for { raw  <- cur.asString.right; expr <- parse(raw).right } yield { expr }
    }
  }

  /**
    * Convert an optional path, as a string
    */
  trait OptionConvert {
    protected def optional(cur: ConfigObjectCursor, key: String): Either[ConfigReaderFailures, Option[String]] = {
      val optCur = cur.atKeyOrUndefined(key)
      if (optCur.isUndefined) { Right(None) } else {
        optCur.asString.right.map(Option.apply)
      }
    }
  }

  /**
    * Access to primitive converts
    */
  object PrimitiveConvert extends PrimitiveReaders with PrimitiveWriters
}
