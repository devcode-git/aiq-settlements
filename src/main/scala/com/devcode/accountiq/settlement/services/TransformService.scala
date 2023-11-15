package com.devcode.accountiq.settlement.services

import com.devcode.accountiq.settlement.elastic.reports.batch.BatchSalesToPayoutReportRow
import com.devcode.accountiq.settlement.elastic.reports.settlement.SettlementDetailReportRow
import com.devcode.accountiq.settlement.elastic.{ESDoc, ElasticSearchDAO}
import com.devcode.accountiq.settlement.transformer.CSVParser
import com.devcode.accountiq.settlement.util.FileUtil
import zio._

import java.io.File

object TransformService {

  def saveBatchSalesToPayoutReport(esdocs: List[ESDoc]): ZIO[ElasticSearchDAO[BatchSalesToPayoutReportRow], Throwable, List[BatchSalesToPayoutReportRow]] = {
    val batchReports = esdocs.map(BatchSalesToPayoutReportRow.fromESDocRaw)
    for {
      dao <- ZIO.service[ElasticSearchDAO[BatchSalesToPayoutReportRow]]
      response <- dao.addBulk(batchReports)
      _ <- ZIO.logDebug(response.toString)
    } yield batchReports
  }

  def settlementDetailReport(esdocs: List[ESDoc]): ZIO[ElasticSearchDAO[SettlementDetailReportRow], Throwable, List[SettlementDetailReportRow]] = {
    val settlementReports = esdocs.map(SettlementDetailReportRow.fromESDocRaw)
    for {
      dao <- ZIO.service[ElasticSearchDAO[SettlementDetailReportRow]]
      response <- dao.addBulk(settlementReports)
      _ <- ZIO.logDebug(response.toString)
    } yield settlementReports
  }

  def saveRaw(file: File): ZIO[ElasticSearchDAO[ESDoc], Throwable, List[ESDoc]] = for {
    fileId <- FileUtil.getFileNamePart(file.getPath)
    rows <- CSVParser.parse(file.toPath)
    esdocs = ESDoc.parseESDocs(rows, fileId)
    _ <- saveESDocs(esdocs)
  } yield esdocs


  private def saveESDocs(esDocs: List[ESDoc]): ZIO[ElasticSearchDAO[ESDoc], Throwable, Unit] = for {
    dao <- ZIO.service[ElasticSearchDAO[ESDoc]]
    response <- dao.addBulk(esDocs)
    _ <- ZIO.logDebug(response.toString)
  } yield ()

}
