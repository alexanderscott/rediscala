package com.crunchdevelopment.redis.commands

import com.crunchdevelopment.redis.{ByteStringDeserializer, Request}
import scala.concurrent.Future
import scala.concurrent.duration._
import com.crunchdevelopment.redis.api.blists._

/**
 * Blocking commands on the Lists
 */
trait BLists extends Request {

  // TODO Future[Option[(KK, ByteString)]]
  def blpop[R: ByteStringDeserializer](keys: Seq[String], timeout: FiniteDuration = Duration.Zero): Future[Option[(String, R)]] =
    send(Blpop(keys, timeout))

  def brpop[R: ByteStringDeserializer](keys: Seq[String], timeout: FiniteDuration = Duration.Zero): Future[Option[(String, R)]] =
    send(Brpop(keys, timeout))

  def brpopplush[R: ByteStringDeserializer](source: String, destination: String, timeout: FiniteDuration = Duration.Zero): Future[Option[R]] =
    send(Brpopplush(source, destination, timeout))
}