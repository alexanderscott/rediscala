package com.crunchdevelopment.redis

import akka.util.ByteString
import com.crunchdevelopment.redis.protocol._

trait RedisCommand[RedisReplyT <: RedisReply, T] {
  val isMasterOnly: Boolean
  val encodedRequest: ByteString

  def decodeReply(r: RedisReplyT): T

  val decodeRedisReply: PartialFunction[ByteString, Option[(RedisReplyT, ByteString)]]

  def encode(command: String) = RedisProtocolRequest.inline(command)

  def encode(command: String, args: Seq[ByteString]) = RedisProtocolRequest.multiBulk(command, args)
}


trait RedisCommandStatus[T] extends RedisCommand[Status, T] {
  val decodeRedisReply: PartialFunction[ByteString, Option[(Status, ByteString)]] = RedisProtocolReply.decodeReplyStatus
}

trait RedisCommandInteger[T] extends RedisCommand[Integer, T] {
  val decodeRedisReply: PartialFunction[ByteString, Option[(Integer, ByteString)]] = RedisProtocolReply.decodeReplyInteger
}

trait RedisCommandBulk[T] extends RedisCommand[Bulk, T] {
  val decodeRedisReply: PartialFunction[ByteString, Option[(Bulk, ByteString)]] = RedisProtocolReply.decodeReplyBulk
}

trait RedisCommandMultiBulk[T] extends RedisCommand[MultiBulk, T] {
  val decodeRedisReply: PartialFunction[ByteString, Option[(MultiBulk, ByteString)]] = RedisProtocolReply.decodeReplyMultiBulk
}

trait RedisCommandRedisReply[T] extends RedisCommand[RedisReply, T] {
  val decodeRedisReply: PartialFunction[ByteString, Option[(RedisReply, ByteString)]] = RedisProtocolReply.decodeReplyPF
}

trait RedisCommandStatusString extends RedisCommandStatus[String] {
  def decodeReply(s: Status) = s.toString
}

trait RedisCommandStatusBoolean extends RedisCommandStatus[Boolean] {
  def decodeReply(s: Status): Boolean = s.toBoolean
}

trait RedisCommandIntegerBoolean extends RedisCommandInteger[Boolean] {
  def decodeReply(i: Integer): Boolean = i.toBoolean
}

trait RedisCommandIntegerLong extends RedisCommandInteger[Long] {
  def decodeReply(i: Integer) = i.toLong
}

trait RedisCommandBulkOptionByteString[R] extends RedisCommandBulk[Option[R]] {
  val deserializer: ByteStringDeserializer[R]

  def decodeReply(bulk: Bulk) = bulk.response.map(deserializer.deserialize)
}

trait RedisCommandBulkDouble extends RedisCommandBulk[Double] {
  def decodeReply(bulk: Bulk) = bulk.response.map(v => java.lang.Double.parseDouble(v.utf8String)).get
}

trait RedisCommandBulkOptionDouble extends RedisCommandBulk[Option[Double]] {
  def decodeReply(bulk: Bulk) = bulk.response.map(v => java.lang.Double.parseDouble(v.utf8String))
}

trait RedisCommandMultiBulkSeqByteString[R] extends RedisCommandMultiBulk[Seq[R]] {
  val deserializer: ByteStringDeserializer[R]

  def decodeReply(mb: MultiBulk) = MultiBulkConverter.toSeqByteString(mb)(deserializer)
}

trait RedisCommandMultiBulkSeqByteStringDouble[R] extends RedisCommandMultiBulk[Seq[(R, Double)]] {
  val deserializer: ByteStringDeserializer[R]

  def decodeReply(mb: MultiBulk) = MultiBulkConverter.toSeqTuple2ByteStringDouble(mb)(deserializer)
}

trait RedisCommandRedisReplyOptionLong extends RedisCommandRedisReply[Option[Long]] {
  def decodeReply(redisReply: RedisReply): Option[Long] = redisReply match {
    case i: Integer => Some(i.toLong)
    case _ => None
  }
}

trait RedisCommandRedisReplyRedisReply[R] extends RedisCommandRedisReply[R] {
  val deserializer: RedisReplyDeserializer[R]

  def decodeReply(redisReply: RedisReply): R = {
    if(deserializer.deserialize.isDefinedAt(redisReply))
      deserializer.deserialize.apply(redisReply)
    else
      throw new RuntimeException("Could not deserialize") // todo make own type
  }
}