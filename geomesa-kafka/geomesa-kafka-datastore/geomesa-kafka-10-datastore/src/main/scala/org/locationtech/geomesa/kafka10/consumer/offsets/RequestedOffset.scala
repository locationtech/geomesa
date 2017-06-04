/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10.consumer.offsets

import java.util.Locale

import com.typesafe.config.Config
import kafka.message.Message
import kafka.serializer.Decoder
import org.locationtech.geomesa.kafka10.consumer.offsets.FindOffset.MessagePredicate

import scala.util.Try

sealed trait RequestedOffset

object RequestedOffset {
  def apply(conf: Config): Option[RequestedOffset] = {
    if (!conf.hasPath("offset")) {
      None
    } else {
      conf.getString("offset").toLowerCase(Locale.US) match {
        case "earliest" => Some(EarliestOffset)
        case "latest" => Some(LatestOffset)
        case "group" => Some(GroupOffset)
        case d if d.startsWith("date") => Try(d.substring(5).trim.toLong).map(DateOffset).toOption
        case _ =>
          val o = conf.getString("offset")
          Try(Class.forName(o).newInstance().asInstanceOf[FindMessage])
            .map(f => FindOffset(f.predicate))
            .orElse(Try(SpecificOffset(o.toLong))).toOption
      }
    }
  }
}

case object EarliestOffset extends RequestedOffset

case object LatestOffset extends RequestedOffset

case object GroupOffset extends RequestedOffset

// NOTE: date offset is only to the log level resolution
case class DateOffset(date: Long) extends RequestedOffset

case class FindOffset(predicate: MessagePredicate) extends RequestedOffset

case class SpecificOffset(offset: Long) extends RequestedOffset

object FindOffset {
  // 0 indicates a match, -1 indicates less than, 1 indicates greater than
  type MessagePredicate = Message => Int

  def apply[T](decoder: Decoder[T], predicate: T => Int): FindOffset = apply {
    (m: Message) => {
      val bb = Array.ofDim[Byte](m.payload.remaining())
      m.payload.get(bb)
      predicate(decoder.fromBytes(bb))
    }
  }
}

trait FindMessage {
  def predicate: MessagePredicate
}
