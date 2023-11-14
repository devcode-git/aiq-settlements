package com.devcode.accountiq.settlement.sftp

import com.devcode.accountiq.settlement.util.FileUtil
import zio.ftp._
import zio.stream.ZSink
import zio.ZIO

object SftpDownloader {

  case class SFTPAccount(host: String, port: Int, username: String, password: String, sourceDirectory: String, destinationDirectory: String, overrideFiles: Boolean = false, fileMask: String = ".*")

  def downloadAccount(): ZIO[SFTPAccount, Throwable, Unit] = ZIO.scoped(for {
    sftpAccount <- ZIO.service[SFTPAccount]
    ftpSecureSettings = SecureFtpSettings(sftpAccount.host, sftpAccount.port, FtpCredentials(sftpAccount.username, sftpAccount.password))
    _ <- ZIO.logInfo(s"Starting to download files")
    ftpResource <- SFtp.ls(sftpAccount.sourceDirectory).runCollect.provideLayer(secure(ftpSecureSettings))
    _ <- ZIO.logInfo(s"Files to download: ${ftpResource.toList.map(_.path).mkString(",")}")
    _ <- ZIO.foreachParDiscard(ftpResource.toList)(r => SftpDownloader.downloadFile(r, sftpAccount).provideLayer(secure(ftpSecureSettings)))
    _ <- ZIO.logInfo(s"Files have been downloaded")
  } yield ())

  private def downloadFile(ftpResource: FtpResource, sftpAccount: SFTPAccount): ZIO[SFtp, Throwable, Unit] = {
    val path = ftpResource.path
    if (ftpResource.isDirectory.getOrElse(false)) {
      return ZIO.logInfo(s"${ftpResource.path} is a directory, skipping...")
    }
    FileUtil.getFileNamePart(path) flatMap {
      case ignoredFileName if !ignoredFileName.matches(sftpAccount.fileMask) =>
        ZIO.logInfo(s"$ignoredFileName isn't configured to be downloaded, skipping...")
      case fileName if !FileUtil.fileDownloaded(sftpAccount.destinationDirectory, fileName) || sftpAccount.overrideFiles =>
        ZIO.scoped(ZIO.logInfo(s"File to download: ${sftpAccount.destinationDirectory}/$fileName") *>
          SFtp.readFile(path).run(ZSink.fromFileName(s"${sftpAccount.destinationDirectory}/$fileName")) *>
          ZIO.logInfo(s"File downloaded ${sftpAccount.destinationDirectory}/$fileName"))
      case alreadyDownloadedFileName =>
        ZIO.logInfo(s"$alreadyDownloadedFileName already downloaded, skipping...")
    }
  }

}
