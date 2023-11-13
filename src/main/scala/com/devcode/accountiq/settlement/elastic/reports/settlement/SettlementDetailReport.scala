package com.devcode.accountiq.settlement.elastic.reports.settlement

import com.devcode.accountiq.settlement.elastic.ESDoc
import com.devcode.accountiq.settlement.transformer.AIQParserUtil

import java.util.Date

case class SettlementDetailReport(companyAccount: String,
                                  merchantAccount: String,
                                  pspReference: String,
                                  merchantReference: Long,
                                  paymentMethod: String,
                                  creationDate: Date,
                                  timeZone: String,
                                  `type`: String,
                                  modificationReference: String,
                                  grossCurrency: String,
                                  grossDebit: Option[Double],
                                  grossCredit: Double,
                                  exchangeRate: Double,
                                  netCurrency: String,
                                  netDebit: Option[Double],
                                  netCredit: Double,
                                  commission: Double,
                                  markup: Option[String],
                                  schemeFees: Option[String],
                                  interchange: Option[String],
                                  paymentMethodVariant: String,
                                  modificationMerchantReference: String,
                                  batchNumber: Long,
                                  shopperReference: Long)

object SettlementDetailReport {
  def fromESDocRaw(esDoc: ESDoc): SettlementDetailReport = {
    val doc = esDoc.doc
    SettlementDetailReport(
      doc(SettlementDetailReportField.companyAccount),
      doc(SettlementDetailReportField.merchantAccount),
      doc(SettlementDetailReportField.pspReference),
      doc(SettlementDetailReportField.merchantReference).toLong,
      doc(SettlementDetailReportField.paymentMethod),
      AIQParserUtil.dateFormat.parse(doc(SettlementDetailReportField.creationDate)),
      doc(SettlementDetailReportField.timeZone),
      doc(SettlementDetailReportField.`type`),
      doc(SettlementDetailReportField.modificationReference),
      doc(SettlementDetailReportField.grossCurrency),
      Option(doc(SettlementDetailReportField.grossDebit)).filter(_.nonEmpty).map(_.toDouble),
      doc(SettlementDetailReportField.grossCredit).toDouble,
      doc(SettlementDetailReportField.exchangeRate).toDouble,
      doc(SettlementDetailReportField.netCurrency),
      Option(doc(SettlementDetailReportField.netDebit)).filter(_.nonEmpty).map(_.toDouble),
      doc(SettlementDetailReportField.netCredit).toDouble,
      doc(SettlementDetailReportField.commission).toDouble,
      Option(doc(SettlementDetailReportField.markup)).filter(_.nonEmpty),
      Option(doc(SettlementDetailReportField.schemeFees)).filter(_.nonEmpty),
      Option(doc(SettlementDetailReportField.interchange)).filter(_.nonEmpty),
      doc(SettlementDetailReportField.paymentMethodVariant),
      doc(SettlementDetailReportField.modificationMerchantReference),
      doc(SettlementDetailReportField.batchNumber).toLong,
      doc(SettlementDetailReportField.shopperReference).toLong
    )
  }
}
