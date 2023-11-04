package com.devcode.accountiq.settlement.util

import java.nio.file.Paths

object FileUtil {
  /**
   * Check if a file has been downloaded previously
   * note: if downloaded previously but put into failed then returns false
   *
   * @param fileName name of file to check if downloaded before (just file name without path)
   * @return
   */
  def fileDownloaded(directory: String, fileName: String): Boolean =
    java.nio.file.Files.exists(Paths.get(s"$directory/$fileName"))


  private val PathWithFilePattern = """^\/?(?:[^\/]+\/)*([^\/]+)$""".r

  def getFileNamePart(path: String): Option[String] =
    path match {
      case PathWithFilePattern(fileName) => Some(fileName)
      case _ => None
    }

}
