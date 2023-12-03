package com.devcode.accountiq.settlement.elastic.reports.settlement

import com.devcode.accountiq.settlement.elastic.ESDoc
import com.devcode.accountiq.settlement.util.DateUtil.LocalDateConverter
import com.sksamuel.elastic4s.Indexable
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, EncoderOps, JsonDecoder, JsonEncoder}
import com.sksamuel.elastic4s.ElasticApi.{dateField, properties}
import com.sksamuel.elastic4s.ElasticDsl.keywordField

import java.text.SimpleDateFormat
import java.time.LocalDateTime

case class SettlementDetailReportRow(companyAccount: String,
                                     merchantAccount: String,
                                     pspReference: String,
                                     merchantReference: Long,
                                     paymentMethod: String,
                                     creationDate: LocalDateTime,
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

  val mapping = properties(
      keywordField("companyAccount"),
      keywordField("merchantAccount"),
      keywordField("pspReference"),
      keywordField("merchantReference"),
      keywordField("paymentMethod"),
      dateField("creationDate"),
      keywordField("timeZone"),
      keywordField("type"),
      keywordField("modificationReference"),
      keywordField("grossCurrency"),
      keywordField("grossDebit"),
      keywordField("grossCredit"),
      keywordField("exchangeRate"),
      keywordField("netCurrency"),
      keywordField("netDebit"),
      keywordField("netCredit"),
      keywordField("commission"),
      keywordField("markup"),
      keywordField("schemeFees"),
      keywordField("interchange"),
      keywordField("paymentMethodVariant"),
      keywordField("modificationMerchantReference"),
      keywordField("batchNumber"),
      keywordField("shopperReference")
  )

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
      dateFormat.parse(doc(SettlementDetailReportField.creationDate)).toLocalDateTime,
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
