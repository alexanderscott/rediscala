package redis.commands

import redis._
import scala.concurrent.Await

class HyperLogLogSpec extends RedisSpec {

  "HyperLogLog commands" should {
    "PFADD" in {
      val r = for {
        _ <- redis.del("pfaddKey")
        pf1 <- redis.pfadd("pfaddKey", "a", "b", "c", "d", "e", "f", "g")
        pf2 <- redis.pfadd("pfaddKey", "a")
        count <- redis.pfcount("pfaddKey")
      } yield {
        pf1 mustEqual 7
        pf2 mustEqual 0
        count mustEqual 7
      }
      Await.result(r, timeOut)
    }

    "PFCOUNT" in {
      val r = for {
        _ <- redis.del("pfcountKey")
        add <- redis.pfadd("pfcountKey", "hello", "world")
        count <- redis.pfcount("pfcountKey")
      } yield {
        add mustEqual 2
        count mustEqual 2
      }
      Await.result(r, timeOut)
    }

    "PFMERGE" in {
      val r = for {
        _ <- redis.del("pfmergeKey")
        _ <- redis.del("pfmergeKey1")
        _ <- redis.del("pfmergeKey2")
        pf1 <- redis.pfadd("pfmergeKey1", "a", "b", "c", "d", "e", "f", "g")
        pf2 <- redis.pfadd("pfmergeKey2", "a", "h", "i", "j")
        merge <- redis.pfmerge("pfmergeKey", "pfmergeKey1", "pfmergeKey2")
        count <- redis.pfcount("pfmergeKey")
      } yield {
        pf1 mustEqual 7
        pf2 mustEqual 4
        merge mustEqual true
        count mustEqual 10
      }
      Await.result(r, timeOut)
    }
  }
}
