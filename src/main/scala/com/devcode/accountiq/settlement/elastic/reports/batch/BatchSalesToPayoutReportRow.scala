package com.devcode.accountiq.settlement.elastic.reports.batch

import com.devcode.accountiq.settlement.elastic.ESDoc
import com.devcode.accountiq.settlement.elastic.reports.{AIQField, Version}
import com.devcode.accountiq.settlement.util.DateUtil.LocalDateConverter
import com.sksamuel.elastic4s.ElasticApi.{dateField, properties}
import com.sksamuel.elastic4s.ElasticDsl.keywordField
import com.sksamuel.elastic4s.Indexable
import zio.json.internal.{RetractReader, Write}
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, EncoderOps, JsonDecoder, JsonEncoder, JsonError}

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date

trait BatchSalesToPayoutReportRow

case class BatchSalesToPayoutReportSummaryRow(_id: Option[String] = None,
                                              version: Option[Version] = None,
                                              status: String,
                                              sales: Double,
                                              refunds: Double,
                                              salesRefund: Double,
                                              pending: Double,
                                              payoutDate: Option[LocalDateTime],
                                              paymentMethod: Option[String],
                                              paymentMethodDescription: Option[String],
                                              salesCount: Long,
                                              refundCount: Long,
                                              aiqFilename: String,
                                              aiqProvider: String,
                                              aiqMerchant: String
                                             ) extends BatchSalesToPayoutReportRow

object BatchSalesToPayoutReportSummaryRow {
  implicit val decoder: JsonDecoder[BatchSalesToPayoutReportSummaryRow] =
    DeriveJsonDecoder.gen[BatchSalesToPayoutReportSummaryRow]

  implicit val encoder: JsonEncoder[BatchSalesToPayoutReportSummaryRow] =
    DeriveJsonEncoder.gen[BatchSalesToPayoutReportSummaryRow]
}

case class BatchSalesToPayoutPaidOutReportRow(status: String,
                                              sales: Double,
                                              refunds: Double,
                                              salesRefund: Double,
                                              pending: Long,
                                              payoutDate: LocalDateTime,
                                              paymentMethod: String,
                                              paymentMethodDescription: String,
                                              salesCount: Long,
                                              refundCount: Long,
                                              aiqFilename: String,
                                              aiqProvider: String,
                                              aiqMerchant: String,
                                             ) extends BatchSalesToPayoutReportRow

object BatchSalesToPayoutPaidOutReportRow {
  implicit val decoder: JsonDecoder[BatchSalesToPayoutPaidOutReportRow] =
    DeriveJsonDecoder.gen[BatchSalesToPayoutPaidOutReportRow]
  implicit val encoder: JsonEncoder[BatchSalesToPayoutPaidOutReportRow] =
    DeriveJsonEncoder.gen[BatchSalesToPayoutPaidOutReportRow]
}


object BatchSalesToPayoutReportRow {

  val mapping = properties(
    keywordField("status"),
    keywordField("sales"),
    keywordField("refunds"),
    keywordField("salesRefund"),
    keywordField("pending"),
    dateField("payoutDate"),
    keywordField("paymentMethod"),
    keywordField("paymentMethodDescription"),
    keywordField("salesCount"),
    keywordField("refundCount")
  )

  implicit val formatter: Indexable[BatchSalesToPayoutReportRow] = {
    case v: BatchSalesToPayoutReportSummaryRow => v.toJson
    case v: BatchSalesToPayoutPaidOutReportRow => v.toJson
  }

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def fromESDocRaw(esDoc: ESDoc): BatchSalesToPayoutReportRow = {
    val doc = esDoc.doc
    doc(BatchSalesToPayoutReportField.status) match {
      case status if status == "summary" =>
        BatchSalesToPayoutReportSummaryRow(
          None,
          None,
          status,
          doc(BatchSalesToPayoutReportField.sales).toDouble,
          doc(BatchSalesToPayoutReportField.refunds).toDouble,
          doc(BatchSalesToPayoutReportField.salesRefund).toDouble,
          doc(BatchSalesToPayoutReportField.pending).toDouble,
          Option(doc(BatchSalesToPayoutReportField.payoutDate)).filter(_.nonEmpty).map(dateFormat.parse).map(_.toLocalDateTime),
          Option(doc(BatchSalesToPayoutReportField.paymentMethod)).filter(_.nonEmpty),
          Option(doc(BatchSalesToPayoutReportField.paymentMethodDescription)).filter(_.nonEmpty),
          doc(BatchSalesToPayoutReportField.salesCount).toLong,
          doc(BatchSalesToPayoutReportField.refundCount).toLong,
          doc(AIQField.filename),
          doc(AIQField.provider),
          doc(AIQField.merchant)
        )
      case status if status == "paidOut" =>
        BatchSalesToPayoutPaidOutReportRow(
          status,
          doc(BatchSalesToPayoutReportField.sales).toDouble,
          doc(BatchSalesToPayoutReportField.refunds).toDouble,
          doc(BatchSalesToPayoutReportField.salesRefund).toDouble,
          doc(BatchSalesToPayoutReportField.pending).toLong,
          dateFormat.parse(doc(BatchSalesToPayoutReportField.payoutDate)).toLocalDateTime,
          doc(BatchSalesToPayoutReportField.paymentMethod),
          doc(BatchSalesToPayoutReportField.paymentMethodDescription),
          doc(BatchSalesToPayoutReportField.salesCount).toLong,
          doc(BatchSalesToPayoutReportField.refundCount).toLong,
          doc(AIQField.filename),
          doc(AIQField.provider),
          doc(AIQField.merchant)
        )
    }
  }
}
