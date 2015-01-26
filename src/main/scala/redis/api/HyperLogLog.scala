package redis.api.hyperloglog

import redis._
import akka.util.ByteString

case class Pfadd[K, V](key: K, elements: Seq[V])(implicit redisKey: ByteStringSerializer[K], convert: ByteStringSerializer[V]) extends RedisCommandIntegerBoolean {
  val isMasterOnly = true
  val encodedRequest: ByteString = encode("PFADD", redisKey.serialize(key) +: elements.map(v => convert.serialize(v)))
}

case class Pfcount[K](key: K)(implicit redisKey: ByteStringSerializer[K]) extends RedisCommandIntegerLong {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("PFCOUNT", Seq(redisKey.serialize(key)))
}

case class Pfmerge[K, KK](key: K, keys: Seq[KK])(implicit redisKey: ByteStringSerializer[K], redisKeys: ByteStringSerializer[KK])
  extends RedisCommandStatusBoolean {
  val isMasterOnly = false
  val encodedRequest: ByteString = encode("PFMERGE", redisKey.serialize(key) +: keys.map(redisKeys.serialize))
}
