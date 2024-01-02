package com.devcode.accountiq.settlement.elastic.reports.merchant

import com.devcode.accountiq.settlement.elastic.reports.{AIQField, ESDoc, Mappings, Version}
import com.devcode.accountiq.settlement.elastic.Money
import com.devcode.accountiq.settlement.util.DateUtil.LocalDateConverter
import com.sksamuel.elastic4s.{Hit, HitReader, Indexable}
import zio.json.{DecoderOps, DeriveJsonDecoder, DeriveJsonEncoder, EncoderOps, JsonDecoder, JsonEncoder}
import com.sksamuel.elastic4s.ElasticApi.{dateField, properties}
import com.sksamuel.elastic4s.ElasticDsl.keywordField
import zio.json.ast.Json

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}

case class MerchantPaymentTransactionsReportRow(
                                                 _id: Option[String] = None,
                                                 version: Option[Version] = None,
                                                 operator: String,
                                                 provider: String,
                                                 txRef: Long,
                                                 txId: Option[Long],
                                                 providerRef: Option[Long],
                                                 created: LocalDateTime,
                                                 booked: LocalDateTime,
                                                 amount: Money,
                                                 amountBase: Money,
                                                 txAmount: Option[String],
                                                 txAmountBase: Option[String],
                                                 fee: String,
                                                 feeBase: String,
                                                 method: Option[String],
                                                 txType: String,
                                                 userId: Long,
                                                 jurisdiction: String,
                                                 aiqFilename: String,
                                                 aiqProvider: String,
                                                 aiqMerchant: String)

object MerchantPaymentTransactionsReportRow {

  val mapping = properties(
    keywordField("operator"),
    keywordField("provider"),
    keywordField("txRef"),
    keywordField("txId"),
    keywordField("providerRef"),
    dateField("created"),
    dateField("booked"),
    Mappings.moneyField("amount"),
    Mappings.moneyField("amountBase"),
    keywordField("txAmount"),
    keywordField("txAmountBase"),
    keywordField("fee"),
    keywordField("feeBase"),
    keywordField("method"),
    keywordField("txType"),
    keywordField("userId"),
    keywordField("jurisdiction")
  )

  implicit val decoderMoney: JsonDecoder[Money] =
    DeriveJsonDecoder.gen[Money]

  implicit val encoderMoney: JsonEncoder[Money] =
    DeriveJsonEncoder.gen[Money]

  implicit val decoder: JsonDecoder[MerchantPaymentTransactionsReportRow] =
    DeriveJsonDecoder.gen[MerchantPaymentTransactionsReportRow]

  implicit val encoder: JsonEncoder[MerchantPaymentTransactionsReportRow] =
    DeriveJsonEncoder.gen[MerchantPaymentTransactionsReportRow]

  implicit val formatter: Indexable[MerchantPaymentTransactionsReportRow] = (t: MerchantPaymentTransactionsReportRow) => {
    t.toJson
  }

  val dateFormat = new SimpleDateFormat("dd/MM/yy HH:mm")

  def fromESDocRaw(esDocs: List[ESDoc]): List[MerchantPaymentTransactionsReportRow] = {
    esDocs.map { esDoc =>
      val doc = esDoc.doc
      MerchantPaymentTransactionsReportRow(
          None,
          None,
          doc(MerchantPaymentTransactionsReportField.operator),
          doc(MerchantPaymentTransactionsReportField.provider),
          doc(MerchantPaymentTransactionsReportField.txRef).toLong,
          Option(doc(MerchantPaymentTransactionsReportField.txId)).filter(_.nonEmpty).map(_.toLong),
          Option(doc(MerchantPaymentTransactionsReportField.providerRef)).filter(_.nonEmpty).map(_.toLong),
          dateFormat.parse(doc(MerchantPaymentTransactionsReportField.created)).toLocalDateTime,
          dateFormat.parse(doc(MerchantPaymentTransactionsReportField.booked)).toLocalDateTime,
          Money.parse(doc(MerchantPaymentTransactionsReportField.amount)),
          Money.parse(doc(MerchantPaymentTransactionsReportField.amountBase)),
          Option(doc(MerchantPaymentTransactionsReportField.txAmount)).filter(_.nonEmpty),
          Option(doc(MerchantPaymentTransactionsReportField.txAmountBase)).filter(_.nonEmpty),
          doc(MerchantPaymentTransactionsReportField.fee),
          doc(MerchantPaymentTransactionsReportField.feeBase),
          Option(doc(MerchantPaymentTransactionsReportField.method)).filter(_.nonEmpty),
          doc(MerchantPaymentTransactionsReportField.txType),
          doc(MerchantPaymentTransactionsReportField.userId).toLong,
          doc(MerchantPaymentTransactionsReportField.jurisdiction),
          doc(AIQField.filename),
          doc(AIQField.provider),
          doc(AIQField.merchant)
      )
    }
  }
}
