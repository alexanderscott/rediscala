package redis.util

import java.io._
import scala.language.postfixOps
import scala.util.matching.Regex

trait FileUtils {

  def getFileTree(f: File): Stream[File] = {
    f #:: (if (f.isDirectory) f.listFiles().toStream.flatMap(getFileTree)
    else Stream.empty)
  }

  def getFileTree(dir: String): Stream[File] = getFileTree(new File(dir))

  def recursiveListFiles(dir: File, r: Regex): Array[File] = {
    val these = dir.listFiles
    val good = these.filter(f => r.findFirstIn(f.getName).isDefined)
    good ++ these.filter(_.isDirectory).flatMap(recursiveListFiles(_,r))
  }

  def fileToString(path: String): String = scala.io.Source.fromURL(getClass.getResource(path)).mkString
}
