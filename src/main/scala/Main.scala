import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Try

object Main {

  def main(args: Array[String]): Unit = {
    val cores = Runtime.getRuntime.availableProcessors()
    val spark = SparkSession.builder.appName("wiki-spark").master(s"local[$cores]").getOrCreate()
    import spark.implicits._
    val inputFiles = ???
    val dataset = spark.read.text(inputFiles)
      .map(splitByWhitespace)
      .map(createWiki)
      .filter(_.project == "ru")
      .filter(_.title.head.isUpper)
      .filter(w => excludeByTitle(w))
      .filter(w => excludeByExtension(w))
      .groupBy($"title")
      .sum()
      .sort($"sum(accesses)".desc)
      .select($"title", $"sum(accesses)")
      .cache()

    dataset.take(20)

    spark.stop()
  }

  private def splitByWhitespace(row: Row) = row.getString(0).split(" ")

  private def createWiki(xs: Array[String]) = {
    val accesses = Try(xs(2).toInt).getOrElse(1)
    val responseSize = Try(xs(3).toInt).getOrElse(1)
    Wiki(xs(0), xs(1), accesses, responseSize)
  }

  private def excludeByExtension(w: Wiki) = !excludedExtensions.exists(ext => w.title.contains(ext))

  private def excludeByTitle(w: Wiki) = !excludedTitles.exists(w.title.startsWith)

  val excludedTitles = Array("Media:", "Special:", "Talk:", "User:", "User_talk:", "Project:", "Project_talk", "File:", "File_talk:",
    "MediaWiki:", "MediaWiki_talk:", "Template:", "Template_talk:", "Help:", "Help_talk:", "Category:", "Category_talk:", "Portal:", "Wikipedia:", "Wikipedia_talk:",
    "404_error/", "Main_Page", "Hypertext_Transfer_Protocol", "Search", "Служебная:")

  val excludedExtensions = Array(".png,", ".PNG", ".txt", ".JPG", ".gif", ".GIF", ".ico")

  case class Wiki(project: String, title: String, accesses: Int, responseSize: Int)

}
