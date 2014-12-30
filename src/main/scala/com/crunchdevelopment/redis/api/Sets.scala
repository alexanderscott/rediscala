package com.crunchdevelopment.redis.api.sets

import com.crunchdevelopment.redis._
import akka.util.ByteString

case class Sadd[K, V](key: K, members: Seq[V])(implicit redisKey: ByteStringSerializer[K], convert: ByteStringSerializer[V]) extends RedisCommandIntegerLong {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("SADD", redisKey.serialize(key) +: members.map(v => convert.serialize(v)))
}

case class Scard[K](key: K)(implicit redisKey: ByteStringSerializer[K]) extends RedisCommandIntegerLong {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("SCARD", Seq(redisKey.serialize(key)))
}

case class Sdiff[K, KK, R](key: K, keys: Seq[KK])(implicit redisKey: ByteStringSerializer[K], redisKeys: ByteStringSerializer[KK], deserializerR: ByteStringDeserializer[R])
  extends RedisCommandMultiBulkSeqByteString[R] {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("SDIFF", redisKey.serialize(key) +: keys.map(redisKeys.serialize))
  val deserializer: ByteStringDeserializer[R] = deserializerR
}

case class Sdiffstore[KD, K, KK](destination: KD, key: K, keys: Seq[KK])
                                (implicit redisDest: ByteStringSerializer[KD], redisKey: ByteStringSerializer[K], redisKeys: ByteStringSerializer[KK])
  extends RedisCommandIntegerLong {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("SDIFFSTORE", redisDest.serialize(destination) +: redisKey.serialize(key) +: keys.map(redisKeys.serialize))
}

case class Sinter[K, KK, R](key: K, keys: Seq[KK])(implicit redisKey: ByteStringSerializer[K], redisKeys: ByteStringSerializer[KK], deserializerR: ByteStringDeserializer[R])
  extends RedisCommandMultiBulkSeqByteString[R] {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("SINTER", redisKey.serialize(key) +: keys.map(redisKeys.serialize))
  val deserializer: ByteStringDeserializer[R] = deserializerR
}

case class Sinterstore[KD, K, KK](destination: KD, key: K, keys: Seq[KK])
                                 (implicit redisDest: ByteStringSerializer[KD], redisKey: ByteStringSerializer[K], redisKeys: ByteStringSerializer[KK])
  extends RedisCommandIntegerLong {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("SINTERSTORE", redisDest.serialize(destination) +: redisKey.serialize(key) +: keys.map(redisKeys.serialize))
}

case class Sismember[K, V](key: K, member: V)(implicit redisKey: ByteStringSerializer[K], convert: ByteStringSerializer[V]) extends RedisCommandIntegerBoolean {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("SISMEMBER", Seq(redisKey.serialize(key), convert.serialize(member)))
}

case class Smembers[K, R](key: K)(implicit redisKey: ByteStringSerializer[K], deserializerR: ByteStringDeserializer[R]) extends RedisCommandMultiBulkSeqByteString[R] {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("SMEMBERS", Seq(redisKey.serialize(key)))
  val deserializer: ByteStringDeserializer[R] = deserializerR
}

case class Smove[KS, KD, V](source: KS, destination: KD, member: V)(implicit redisSource: ByteStringSerializer[KS], redisDest: ByteStringSerializer[KD], convert: ByteStringSerializer[V])
  extends RedisCommandIntegerBoolean {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("SMOVE", Seq(redisSource.serialize(source), redisDest.serialize(destination), convert.serialize(member)))
}

case class Spop[K, R](key: K)(implicit redisKey: ByteStringSerializer[K], deserializerR: ByteStringDeserializer[R]) extends RedisCommandBulkOptionByteString[R] {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("SPOP", Seq(redisKey.serialize(key)))
  val deserializer: ByteStringDeserializer[R] = deserializerR
}

case class Srandmember[K, R](key: K)(implicit redisKey: ByteStringSerializer[K], deserializerR: ByteStringDeserializer[R]) extends RedisCommandBulkOptionByteString[R] {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("SRANDMEMBER", Seq(redisKey.serialize(key)))
  val deserializer: ByteStringDeserializer[R] = deserializerR
}

case class Srandmembers[K, R](key: K, count: Long)(implicit redisKey: ByteStringSerializer[K], deserializerR: ByteStringDeserializer[R]) extends RedisCommandMultiBulkSeqByteString[R] {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("SRANDMEMBER", Seq(redisKey.serialize(key), ByteString(count.toString)))
  val deserializer: ByteStringDeserializer[R] = deserializerR
}

case class Srem[K, V](key: K, members: Seq[V])(implicit redisKey: ByteStringSerializer[K], convert: ByteStringSerializer[V]) extends RedisCommandIntegerLong {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("SREM", redisKey.serialize(key) +: members.map(v => convert.serialize(v)))
}

case class Sunion[K, KK, R](key: K, keys: Seq[KK])(implicit redisKey: ByteStringSerializer[K], redisKeys: ByteStringSerializer[KK], deserializerR: ByteStringDeserializer[R])
  extends RedisCommandMultiBulkSeqByteString[R] {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("SUNION", redisKey.serialize(key) +: keys.map(redisKeys.serialize))
  val deserializer: ByteStringDeserializer[R] = deserializerR
}


case class Sunionstore[KD, K, KK](destination: KD, key: K, keys: Seq[KK])
                                 (implicit redisDest: ByteStringSerializer[KD], redisKey: ByteStringSerializer[K], redisKeys: ByteStringSerializer[KK])
  extends RedisCommandIntegerLong {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("SUNIONSTORE", redisDest.serialize(destination) +: redisKey.serialize(key) +: keys.map(redisKeys.serialize))
}