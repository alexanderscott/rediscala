package com.crunchdevelopment.redis.api.publish

import com.crunchdevelopment.redis.{RedisCommandIntegerLong, ByteStringSerializer}
import akka.util.ByteString

case class Publish[A](channel: String, value: A)(implicit convert: ByteStringSerializer[A]) extends RedisCommandIntegerLong {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("PUBLISH", Seq(ByteString(channel), convert.serialize(value)))
}
