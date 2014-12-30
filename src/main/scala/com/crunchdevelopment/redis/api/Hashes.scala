package com.crunchdevelopment.redis.api.hashes

import com.crunchdevelopment.redis._
import akka.util.ByteString
import scala.collection.mutable
import scala.annotation.tailrec
import com.crunchdevelopment.redis.protocol.MultiBulk

case class Hdel[K, KK](key: K, fields: Seq[KK])(implicit redisKey: ByteStringSerializer[K], redisFields: ByteStringSerializer[KK]) extends RedisCommandIntegerLong {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("HDEL", redisKey.serialize(key) +: fields.map(redisFields.serialize))
}

case class Hexists[K, KK](key: K, field: KK)(implicit redisKey: ByteStringSerializer[K], redisFields: ByteStringSerializer[KK]) extends RedisCommandIntegerBoolean {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("HEXISTS", Seq(redisKey.serialize(key), redisFields.serialize(field)))
}

case class Hget[K, KK, R](key: K, field: KK)(implicit redisKey: ByteStringSerializer[K], redisFields: ByteStringSerializer[KK], deserializerR: ByteStringDeserializer[R])
  extends RedisCommandBulkOptionByteString[R] {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("HGET", Seq(redisKey.serialize(key), redisFields.serialize(field)))
  val deserializer: ByteStringDeserializer[R] = deserializerR
}

case class Hgetall[K, R](key: K)(implicit redisKey: ByteStringSerializer[K], deserializerR: ByteStringDeserializer[R]) extends RedisCommandMultiBulk[Map[String, R]] {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("HGETALL", Seq(redisKey.serialize(key)))

  def decodeReply(mb: MultiBulk) = mb.responses.map(r => {
    val seq = r.map(_.toByteString)
    val builder = Map.newBuilder[String, R]
    seqToMap(seq, builder)
    builder.result()
  }).get

  @tailrec
  private def seqToMap(seq: Seq[ByteString], builder: mutable.Builder[(String, R), Map[String, R]]): Unit = {
    if (seq.nonEmpty) {
      builder += (seq.head.utf8String -> deserializerR.deserialize(seq.tail.head))
      seqToMap(seq.tail.tail, builder)
    }
  }
}

case class Hincrby[K, KK](key: K, fields: KK, increment: Long)(implicit redisKey: ByteStringSerializer[K], redisFields: ByteStringSerializer[KK])
  extends RedisCommandIntegerLong {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("HINCRBY", Seq(redisKey.serialize(key), redisFields.serialize(fields), ByteString(increment.toString)))
}

case class Hincrbyfloat[K, KK](key: K, fields: KK, increment: Double)(implicit redisKey: ByteStringSerializer[K], redisFields: ByteStringSerializer[KK])
  extends RedisCommandBulkDouble {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("HINCRBYFLOAT", Seq(redisKey.serialize(key), redisFields.serialize(fields), ByteString(increment.toString)))
}

case class Hkeys[K](key: K)(implicit redisKey: ByteStringSerializer[K]) extends RedisCommandMultiBulk[Seq[String]] {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("HKEYS", Seq(redisKey.serialize(key)))

  def decodeReply(mb: MultiBulk) = MultiBulkConverter.toSeqString(mb)
}

case class Hlen[K](key: K)(implicit redisKey: ByteStringSerializer[K]) extends RedisCommandIntegerLong {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("HLEN", Seq(redisKey.serialize(key)))
}

case class Hmget[K, KK, R](key: K, fields: Seq[KK])(implicit redisKey: ByteStringSerializer[K], redisFields: ByteStringSerializer[KK], deserializerR: ByteStringDeserializer[R])
  extends RedisCommandMultiBulk[Seq[Option[R]]] {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("HMGET", redisKey.serialize(key) +: fields.map(redisFields.serialize))

  def decodeReply(mb: MultiBulk) = MultiBulkConverter.toSeqOptionByteString(mb)
}

case class Hmset[K, KK, V](key: K, keysValues: Map[KK, V])(implicit redisKey: ByteStringSerializer[K], redisFields: ByteStringSerializer[KK], convert: ByteStringSerializer[V])
  extends RedisCommandStatusBoolean {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("HMSET", redisKey.serialize(key) +: keysValues.foldLeft(Seq.empty[ByteString])({
    case (acc, e) => redisFields.serialize(e._1) +: convert.serialize(e._2) +: acc
  }))
}

case class Hset[K, KK, V](key: K, field: KK, value: V)(implicit redisKey: ByteStringSerializer[K], redisFields: ByteStringSerializer[KK], convert: ByteStringSerializer[V])
  extends RedisCommandIntegerBoolean {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("HSET", Seq(redisKey.serialize(key), redisFields.serialize(field), convert.serialize(value)))
}

case class Hsetnx[K, KK, V](key: K, field: KK, value: V)(implicit redisKey: ByteStringSerializer[K], redisFields: ByteStringSerializer[KK], convert: ByteStringSerializer[V])
  extends RedisCommandIntegerBoolean {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("HSETNX", Seq(redisKey.serialize(key), redisFields.serialize(field), convert.serialize(value)))
}

case class Hvals[K, R](key: K)(implicit redisKey: ByteStringSerializer[K], deserializerR: ByteStringDeserializer[R]) extends RedisCommandMultiBulkSeqByteString[R] {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("HVALS", Seq(redisKey.serialize(key)))
  val deserializer: ByteStringDeserializer[R] = deserializerR
}