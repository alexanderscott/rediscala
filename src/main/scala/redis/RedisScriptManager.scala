package redis

import akka.actor.ActorSystem
import java.io.File
import redis.api.scripting.RedisScript
import redis.protocol.RedisReply
import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutor, Future}
import redis.util.FileUtils

object RedisScriptManager {
  val defaultScriptPath = "/lua"

  def apply(redis: RedisClient, scriptPath: String = defaultScriptPath): RedisScriptManager =
    new RedisScriptManager(redis, scriptPath)
}

class RedisScriptManager(redis: RedisClient, scriptPath: String = RedisScriptManager.defaultScriptPath) extends FileUtils {
  protected val scriptCache = mutable.Map.empty[String, RedisScript]

  def scriptNameToPath(name: String)(implicit system: ActorSystem): String = {
    if(scriptPath.takeRight(1) == "/") scriptPath + name + ".lua"
    else scriptPath + "/" + name + ".lua"
  }

  def scriptPathToName(path: String): String = {
    path.split("/").last.dropRight(4)
  }

  def loadScriptByPath(path: String): Future[String] = {
    scriptCache.update(scriptPathToName(path), RedisScript(fileToString(path)))
    redis.scriptLoad(path)
  }

  def loadScriptByName(name: String)(implicit system: ActorSystem): Future[String] = {
    scriptCache.update(name, RedisScript(fileToString(scriptNameToPath(name))))
    redis.scriptLoad(scriptNameToPath(name))
  }

  def registerScripts()(implicit system: ActorSystem, dispatcher: ExecutionContextExecutor): Future[List[String]] = {
    val dir = new File(scriptPath)

    Future.sequence(
      dir.listFiles
        .filter(_.getName.endsWith(".lua"))
        .map(_.getPath).map(loadScriptByPath).toList
    )
  }

  def runScript[T: RedisReplyDeserializer](scriptName: String, keys: Seq[String] = Seq(), args: Seq[String] = Seq())
                  (implicit system: ActorSystem, dispatcher: ExecutionContextExecutor): Future[T] = {
    val redisScript = scriptCache.getOrElseUpdate(scriptName, RedisScript(fileToString(scriptNameToPath(scriptName))))
    redis.evalshaOrEval[T](redisScript, keys, args)
  }
}

