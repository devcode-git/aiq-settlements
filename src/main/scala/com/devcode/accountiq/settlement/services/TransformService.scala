package com.devcode.accountiq.settlement.services

import com.devcode.accountiq.settlement.elastic.dao.ElasticSearchDAO
import com.devcode.accountiq.settlement.elastic.reports.ESDoc
import com.devcode.accountiq.settlement.elastic.reports.batch.BatchSalesToPayoutReportRow
import com.devcode.accountiq.settlement.elastic.reports.merchant.MerchantPaymentTransactionsReportRow
import com.devcode.accountiq.settlement.elastic.reports.settlement.{SettlementDetailReport, SettlementDetailReportRow}
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

  def saveBatchSalesToPayoutReport(file: File, provider: String, merchant: String) = {
    for {
      esDocs <- parseESDocs(file, provider, merchant)
      batchReports = esDocs.map(BatchSalesToPayoutReportRow.fromESDocRaw)
      dao <- ZIO.service[ElasticSearchDAO[BatchSalesToPayoutReportRow]]
      response <- dao.addBulk(batchReports)
      errors = response.toOption.map(_.items).map(_.map(_.error).collect{ case Some(e) => e }.map(_.reason))
    } yield if (errors.isEmpty || errors.exists(_.isEmpty)) Right(batchReports) else Left(errors)
  }

  def saveSettlementDetailReport(file: File, provider: String, merchant: String) = {
    for {
      esDocs <- parseESDocs(file, provider, merchant)
      settlementReports = esDocs.map(SettlementDetailReport.fromESDocRaw)
      dao <- ZIO.service[ElasticSearchDAO[SettlementDetailReport]]
      response <- dao.addBulk(settlementReports)
//      errors = response.result.items.map(_.error).collect{ case Some(e) => e }.map(_.reason)
      errors = response.toOption.map(_.items).map(_.map(_.error).collect{ case Some(e) => e }.map(_.reason))
    } yield if (errors.isEmpty || errors.exists(_.isEmpty)) Right(settlementReports) else Left(errors)
  }

  def saveMerchantPaymentTransactionsReport(file: File, provider: String, merchant: String) = {
    for {
      esDocs <- parseESDocs(file, provider, merchant)
      settlementReports = esDocs.map(MerchantPaymentTransactionsReportRow.fromESDocRaw)
      dao <- ZIO.service[ElasticSearchDAO[MerchantPaymentTransactionsReportRow]]
      response <- dao.addBulk(settlementReports)
      errors = response.toOption.map(_.items).map(_.map(_.error).collect{ case Some(e) => e }.map(_.reason))
    } yield if (errors.isEmpty || errors.exists(_.isEmpty)) Right(settlementReports) else Left(errors)
  }


}
