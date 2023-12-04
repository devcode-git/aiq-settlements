package com.devcode.accountiq.settlement.services

import com.devcode.accountiq.settlement.elastic.reports.ESDoc
import com.devcode.accountiq.settlement.elastic.reports.batch.BatchSalesToPayoutReportRow
import com.devcode.accountiq.settlement.elastic.reports.merchant.MerchantPaymentTransactionsReportRow
import com.devcode.accountiq.settlement.elastic.reports.settlement.SettlementDetailReportRow
import com.devcode.accountiq.settlement.elastic.ElasticSearchDAO
import com.devcode.accountiq.settlement.elastic.reports.ESDoc
import com.devcode.accountiq.settlement.transformer.CSVParser
import com.devcode.accountiq.settlement.util.FileUtil
import zio._

import java.io.File

object TransformService {

  private def parseESDocs(file: File, provider: String, merchant: String): ZIO[Any, Throwable, List[ESDoc]] = for {
    fileId <- FileUtil.getFileNamePart(file.getPath)
    rows <- CSVParser.parse(file.toPath)
  } yield ESDoc.parseESDocs(rows, fileId, provider, merchant)

  def saveBatchSalesToPayoutReport(file: File, provider: String, merchant: String): ZIO[ElasticSearchDAO[BatchSalesToPayoutReportRow], Throwable, List[BatchSalesToPayoutReportRow]] = {
    for {
      esDocs <- parseESDocs(file, provider, merchant)
      batchReports = esDocs.map(BatchSalesToPayoutReportRow.fromESDocRaw)
      dao <- ZIO.service[ElasticSearchDAO[BatchSalesToPayoutReportRow]]
      response <- dao.addBulk(batchReports)
      _ <- ZIO.logDebug(response.toString)
    } yield batchReports
  }

  def saveSettlementDetailReport(file: File, provider: String, merchant: String): ZIO[ElasticSearchDAO[SettlementDetailReportRow], Throwable, List[SettlementDetailReportRow]] = {
    for {
      esDocs <- parseESDocs(file, provider, merchant)
      settlementReports = esDocs.map(SettlementDetailReportRow.fromESDocRaw)
      dao <- ZIO.service[ElasticSearchDAO[SettlementDetailReportRow]]
      response <- dao.addBulk(settlementReports)
      _ <- ZIO.logDebug(response.toString)
    } yield settlementReports
  }

  def saveMerchantPaymentTransactionsReport(file: File, provider: String, merchant: String): ZIO[ElasticSearchDAO[MerchantPaymentTransactionsReportRow], Throwable, List[MerchantPaymentTransactionsReportRow]] = {
    for {
      esDocs <- parseESDocs(file, provider, merchant)
      settlementReports = esDocs.map(MerchantPaymentTransactionsReportRow.fromESDocRaw)
      dao <- ZIO.service[ElasticSearchDAO[MerchantPaymentTransactionsReportRow]]
      response <- dao.addBulk(settlementReports)
      _ <- ZIO.logDebug(response.toString)
    } yield settlementReports
  }

}
