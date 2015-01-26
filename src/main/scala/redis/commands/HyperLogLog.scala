package redis.commands

import redis.{ByteStringSerializer, Request}
import scala.concurrent.Future
import redis.api.hyperloglog._

trait HyperLogLog extends Request {

  def pfadd[V: ByteStringSerializer](key: String, elements: V*): Future[Boolean] =
    send(Pfadd(key, elements))

  def pfcount(key: String): Future[Long] =
    send(Pfcount(key))

  def pfmerge(destKey: String, sourceKeys: String*): Future[Boolean] =
    send(Pfmerge(destKey, sourceKeys))

}

