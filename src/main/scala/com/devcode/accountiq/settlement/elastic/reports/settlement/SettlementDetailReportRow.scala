package com.devcode.accountiq.settlement.elastic.reports.settlement

import com.devcode.accountiq.settlement.elastic.reports.ESDoc
import com.devcode.accountiq.settlement.elastic.reports.{AIQField, Version}
import com.devcode.accountiq.settlement.util.DateUtil.LocalDateConverter
import com.sksamuel.elastic4s.{Hit, HitReader, Indexable}
import zio.json.{DecoderOps, DeriveJsonDecoder, DeriveJsonEncoder, EncoderOps, JsonDecoder, JsonEncoder}
import com.sksamuel.elastic4s.ElasticApi.{dateField, properties}
import com.sksamuel.elastic4s.ElasticDsl.keywordField
import zio.json.ast.Json

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}

sealed trait SettlementDetailReport

case class SettlementDetailReportRow(
                                    _id: Option[String] = None,
                                    version: Option[Version] = None,
                                    companyAccount: String,
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
                                     shopperReference: Long,
                                     aiqFilename: String,
                                     aiqProvider: String,
                                     aiqMerchant: String) extends SettlementDetailReport

object SettlementDetailReportRow {
  implicit val decoder: JsonDecoder[SettlementDetailReportRow] =
    DeriveJsonDecoder.gen[SettlementDetailReportRow]

  implicit val encoder: JsonEncoder[SettlementDetailReportRow] =
    DeriveJsonEncoder.gen[SettlementDetailReportRow]
}

object SettlementDetailReportMerchantPayoutRow {
  implicit val decoder: JsonDecoder[SettlementDetailReportMerchantPayoutRow] =
    DeriveJsonDecoder.gen[SettlementDetailReportMerchantPayoutRow]

  implicit val encoder: JsonEncoder[SettlementDetailReportMerchantPayoutRow] =
    DeriveJsonEncoder.gen[SettlementDetailReportMerchantPayoutRow]

  def create(esDoc: ESDoc) = {
    val doc = esDoc.doc
    SettlementDetailReportMerchantPayoutRow(
      None,
      None,
      doc(SettlementDetailReportField.companyAccount),
      doc(SettlementDetailReportField.merchantAccount),
      SettlementDetailReport.dateFormat.parse(doc(SettlementDetailReportField.creationDate)).toLocalDateTime,
      doc(SettlementDetailReportField.timeZone),
      doc(SettlementDetailReportField.`type`),
      doc(SettlementDetailReportField.modificationReference),
      doc(SettlementDetailReportField.netCurrency),
      Option(doc(SettlementDetailReportField.netDebit)).filter(_.nonEmpty).map(_.toDouble),
      doc(SettlementDetailReportField.batchNumber).toLong,
      doc(AIQField.filename),
      doc(AIQField.provider),
      doc(AIQField.merchant)
    )
  }
}

object SettlementDetailReportFeeRow {
  implicit val decoder: JsonDecoder[SettlementDetailReportFeeRow] =
    DeriveJsonDecoder.gen[SettlementDetailReportFeeRow]

  implicit val encoder: JsonEncoder[SettlementDetailReportFeeRow] =
    DeriveJsonEncoder.gen[SettlementDetailReportFeeRow]

  def create(esDoc: ESDoc) = {
    val doc = esDoc.doc
    SettlementDetailReportFeeRow(
      None,
      None,
      doc(SettlementDetailReportField.companyAccount),
      doc(SettlementDetailReportField.merchantAccount),
      SettlementDetailReport.dateFormat.parse(doc(SettlementDetailReportField.creationDate)).toLocalDateTime,
      doc(SettlementDetailReportField.timeZone),
      doc(SettlementDetailReportField.`type`),
      doc(SettlementDetailReportField.modificationReference),
      doc(SettlementDetailReportField.netCurrency),
      Option(doc(SettlementDetailReportField.netDebit)).filter(_.nonEmpty).map(_.toDouble),
      doc(SettlementDetailReportField.batchNumber).toLong,
      doc(AIQField.filename),
      doc(AIQField.provider),
      doc(AIQField.merchant)
    )
  }
}

case class SettlementDetailReportMerchantPayoutRow(
                                                    _id: Option[String] = None,
                                                    version: Option[Version] = None,
                                                    companyAccount: String,
                                                    merchantAccount: String,
                                                    creationDate: LocalDateTime,
                                                    timeZone: String,
                                                    `type`: String,
                                                    modificationReference: String,
                                                    netCurrency: String,
                                                    netDebit: Option[Double],
                                                    batchNumber: Long,
                                                    aiqFilename: String,
                                                    aiqProvider: String,
                                                    aiqMerchant: String) extends SettlementDetailReport

case class SettlementDetailReportFeeRow(
                                         _id: Option[String] = None,
                                         version: Option[Version] = None,
                                         companyAccount: String,
                                         merchantAccount: String,
                                         creationDate: LocalDateTime,
                                         timeZone: String,
                                         `type`: String,
                                         modificationReference: String,
                                         netCurrency: String,
                                         netDebit: Option[Double],
                                         batchNumber: Long,
                                         aiqFilename: String,
                                         aiqProvider: String,
                                         aiqMerchant: String) extends SettlementDetailReport

object SettlementDetailReport {

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

  private def create(esDoc: ESDoc) = {
    val doc = esDoc.doc
    SettlementDetailReportRow(
      None,
      None,
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
      doc(SettlementDetailReportField.shopperReference).toLong,
      doc(AIQField.filename),
      doc(AIQField.provider),
      doc(AIQField.merchant)
    )
  }

  implicit val decoder: JsonDecoder[SettlementDetailReport] =
    DeriveJsonDecoder.gen[SettlementDetailReport]

  implicit val encoder: JsonEncoder[SettlementDetailReport] =
    DeriveJsonEncoder.gen[SettlementDetailReport]

  implicit val formatter: Indexable[SettlementDetailReport] = {
    case r: SettlementDetailReportRow => r.toJson
    case r: SettlementDetailReportMerchantPayoutRow => r.toJson
    case r: SettlementDetailReportFeeRow => r.toJson
  }

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def fromESDocRaw(esDoc: ESDoc): SettlementDetailReport = {
    val doc = esDoc.doc
    doc(SettlementDetailReportField.`type`) match {
      case t if t.equalsIgnoreCase("Fee") => SettlementDetailReportFeeRow.create(esDoc)
      case t if t.equalsIgnoreCase("MerchantPayout") => SettlementDetailReportMerchantPayoutRow.create(esDoc)
      case _ => create(esDoc)
    }
  }

}
