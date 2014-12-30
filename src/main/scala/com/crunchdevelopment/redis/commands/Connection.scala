package com.crunchdevelopment.redis.commands

import com.crunchdevelopment.redis.{ByteStringDeserializer, ByteStringSerializer, Request}
import scala.concurrent.Future
import com.crunchdevelopment.redis.protocol.Status
import com.crunchdevelopment.redis.api.connection._

trait Connection extends Request {
  def auth[V: ByteStringSerializer](value: V): Future[Status] =
    send(Auth(value))

  def echo[V: ByteStringSerializer, R: ByteStringDeserializer](value: V): Future[Option[R]] =
    send(Echo(value))

  def ping(): Future[String] =
    send(Ping)

  // commands sent after will fail with [[redis.protocol.NoConnectionException]]
  def quit(): Future[Boolean] =
    send(Quit)

  def select(index: Int): Future[Boolean] =
    send(Select(index))
}

