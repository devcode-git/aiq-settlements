package com.devcode.accountiq.settlement.sftp

import com.devcode.accountiq.settlement.util.FileUtil
import zio.ftp._
import zio.stream.ZSink
import zio.{Scope, ZIO}

object SftpDownloader {

  case class SFTPConfig(host: String, port: Int, overrideFiles: Boolean, ignoreDownloadErrors: Boolean = false, fileMask: String = ".*")

  val host = "s-050dbb81072240e89.server.transfer.eu-west-1.amazonaws.com"
  val port = 22
  val username = "fastpay_aiq"
  val password = "b5dd33d9995cc68c4908910488869636"
  val ftpSecureSettings = SecureFtpSettings(host, port, FtpCredentials(username, password))
  val aiqSftpConfig = SFTPConfig(host, port, false)
  val sourceDirectory = "home/upload"
  val directory: String = "/Users/white/Desktop/win"

  def downloadAccount(): ZIO[Scope, Throwable, Unit] = for {
    _ <- ZIO.logInfo(s"Starting to download files")
    ftpResource <- SFtp.ls(sourceDirectory).runCollect.provideLayer(secure(ftpSecureSettings))
    _ <- ZIO.foreachPar(ftpResource.toList)(r => SftpDownloader.downloadFile(aiqSftpConfig, r).provideLayer(secure(ftpSecureSettings)))
    _ <- ZIO.logInfo(s"Files have been downloaded")
  } yield ()

  private def downloadFile(configuration: SFTPConfig, ftpResource: FtpResource): ZIO[SFtp, Throwable, Unit] = {
    val path = ftpResource.path
    if (ftpResource.isDirectory.getOrElse(false)) {
      return ZIO.logInfo(s"${ftpResource.path} is a directory, skipping...")
    }
    FileUtil.getFileNamePart(path) flatMap {
      case ignoredFileName if !ignoredFileName.matches(configuration.fileMask) =>
        ZIO.logInfo(s"$ignoredFileName isn't configured to be downloaded, skipping...")
      case fileName if !FileUtil.fileDownloaded(directory, fileName) || configuration.overrideFiles =>
        SFtp.readFile(path).run(ZSink.fromFileName(s"$directory/$fileName")).flatMap(_ => ZIO.logInfo(s"File downloaded $directory/$fileName"))
      case alreadyDownloadedFileName =>
        ZIO.logInfo(s"$alreadyDownloadedFileName already downloaded, skipping...")
    }
  }

}
