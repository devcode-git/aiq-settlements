package com.devcode.accountiq.settlement.elastic.reports.settlement

import com.devcode.accountiq.settlement.elastic.ESDoc
import com.sksamuel.elastic4s.Indexable
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, EncoderOps, JsonDecoder, JsonEncoder}

import java.text.SimpleDateFormat

case class SettlementDetailReportRow(companyAccount: String,
                                     merchantAccount: String,
                                     pspReference: String,
                                     merchantReference: Long,
                                     paymentMethod: String,
                                     creationDateTimestamp: Long,
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

object SettlementDetailReportRow {

  implicit val decoder: JsonDecoder[SettlementDetailReportRow] =
    DeriveJsonDecoder.gen[SettlementDetailReportRow]

  implicit val encoder: JsonEncoder[SettlementDetailReportRow] =
    DeriveJsonEncoder.gen[SettlementDetailReportRow]

  implicit val formatter: Indexable[SettlementDetailReportRow] = (t: SettlementDetailReportRow) => {
    t.toJson
  }

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def fromESDocRaw(esDoc: ESDoc): SettlementDetailReportRow = {
    val doc = esDoc.doc
    SettlementDetailReportRow(
      doc(SettlementDetailReportField.companyAccount),
      doc(SettlementDetailReportField.merchantAccount),
      doc(SettlementDetailReportField.pspReference),
      doc(SettlementDetailReportField.merchantReference).toLong,
      doc(SettlementDetailReportField.paymentMethod),
      dateFormat.parse(doc(SettlementDetailReportField.creationDate)).getTime,
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
