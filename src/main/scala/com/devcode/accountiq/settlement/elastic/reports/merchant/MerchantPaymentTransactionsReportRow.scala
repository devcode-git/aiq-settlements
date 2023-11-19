package com.devcode.accountiq.settlement.elastic.reports.merchant

import com.devcode.accountiq.settlement.elastic.ESDoc
import com.devcode.accountiq.settlement.util.DateUtil.LocalDateConverter
import com.sksamuel.elastic4s.Indexable
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, EncoderOps, JsonDecoder, JsonEncoder}

import java.text.SimpleDateFormat
import java.time.LocalDate

case class MerchantPaymentTransactionsReportRow(
                                    operator: String,
                                    provider: String,
                                    txRef: Long,
                                    txId: Option[Long],
                                    providerRef: Option[Long],
                                    created: LocalDate,
                                    booked: LocalDate,
                                    amount: String,
                                    amountBase: String,
                                    txAmount: Option[String],
                                    txAmountBase: Option[String],
                                    fee: String,
                                    feeBase: String,
                                    method: Option[String],
                                    txType: String,
                                    userId: Long,
                                    jurisdiction: String)

object MerchantPaymentTransactionsReportRow {

  implicit val decoder: JsonDecoder[MerchantPaymentTransactionsReportRow] =
    DeriveJsonDecoder.gen[MerchantPaymentTransactionsReportRow]

  implicit val encoder: JsonEncoder[MerchantPaymentTransactionsReportRow] =
    DeriveJsonEncoder.gen[MerchantPaymentTransactionsReportRow]

  implicit val formatter: Indexable[MerchantPaymentTransactionsReportRow] = (t: MerchantPaymentTransactionsReportRow) => {
    t.toJson
  }

  val dateFormat = new SimpleDateFormat("dd/MM/yy HH:mm")

  def fromESDocRaw(esDoc: ESDoc): MerchantPaymentTransactionsReportRow = {
    val doc = esDoc.doc
    MerchantPaymentTransactionsReportRow(
        doc(MerchantPaymentTransactionsReportField.operator),
        doc(MerchantPaymentTransactionsReportField.provider),
        doc(MerchantPaymentTransactionsReportField.txRef).toLong,
        Option(doc(MerchantPaymentTransactionsReportField.txId)).filter(_.nonEmpty).map(_.toLong),
        Option(doc(MerchantPaymentTransactionsReportField.providerRef)).filter(_.nonEmpty).map(_.toLong),
        dateFormat.parse(doc(MerchantPaymentTransactionsReportField.created)).toLocalDate,
        dateFormat.parse(doc(MerchantPaymentTransactionsReportField.booked)).toLocalDate,
        doc(MerchantPaymentTransactionsReportField.amount),
        doc(MerchantPaymentTransactionsReportField.amountBase),
        Option(doc(MerchantPaymentTransactionsReportField.txAmount)).filter(_.nonEmpty),
        Option(doc(MerchantPaymentTransactionsReportField.txAmountBase)).filter(_.nonEmpty),
        doc(MerchantPaymentTransactionsReportField.fee),
        doc(MerchantPaymentTransactionsReportField.feeBase),
        Option(doc(MerchantPaymentTransactionsReportField.method)).filter(_.nonEmpty),
        doc(MerchantPaymentTransactionsReportField.txType),
        doc(MerchantPaymentTransactionsReportField.userId).toLong,
        doc(MerchantPaymentTransactionsReportField.jurisdiction)
    )
  }
}
