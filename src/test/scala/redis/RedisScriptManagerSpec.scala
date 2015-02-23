package redis

import redis.api.scripting.RedisScript
import scala.concurrent.Await
import scala.concurrent.duration._

class RedisScriptManagerSpec extends RedisSpec {

  sequential

  class TestScriptManager(redisClient: RedisClient) extends RedisScriptManager(redisClient) {
    def getScriptCache = scriptCache
  }

  "RedisScriptManager" should {
    "translate a script name to path and path to name" in {
      val scriptManager = new TestScriptManager(redis)
      val scriptName = "test1"
      val expectedScriptPath = s"/lua/$scriptName.lua"

      scriptManager.scriptNameToPath(scriptName) must_== expectedScriptPath
      scriptManager.scriptPathToName(expectedScriptPath) must_== scriptName
    }
    "load a cript and cache its name" in {
      val scriptManager = new TestScriptManager(redis)
      val script1Name = "test1"
      val script2Name = "test2"
      val testScript2Path = s"/lua/$script2Name.lua"

      scriptManager.loadScriptByName(script1Name)
      scriptManager.loadScriptByPath(testScript2Path)

      scriptManager.getScriptCache(script1Name) must be (anInstanceOf[RedisScript])
      scriptManager.getScriptCache(script2Name) must be (anInstanceOf[RedisScript])
    }

    "register all scripts in a directory" in {
      val scriptManager = new TestScriptManager(redis)
      val script1Name = "test1"
      val script2Name = "test2"

      scriptManager.registerScripts()
      scriptManager.getScriptCache(script1Name) must be (anInstanceOf[RedisScript])
      scriptManager.getScriptCache(script2Name) must be (anInstanceOf[RedisScript])
    }

    "run a script that has been cached" in {
      val scriptManager = new TestScriptManager(redis)
      scriptManager.registerScripts()
      val result = Await.result(scriptManager.runScript[String]("test1"), 2.seconds)
      result must_== "test1-out"
    }

    "run and cache a script which has not already been cached" in {
      val scriptManager = new TestScriptManager(redis)
      val result = Await.result(scriptManager.runScript("test1"), 2.seconds)
      result must_== "test1-out"
      scriptManager.getScriptCache("test") must be (anInstanceOf[RedisScript])
    }

  }


}
