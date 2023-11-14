package com.devcode.accountiq.settlement.elastic.reports.batch

import com.devcode.accountiq.settlement.elastic.ESDoc
import com.sksamuel.elastic4s.Indexable
import zio.json.internal.{RetractReader, Write}
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, EncoderOps, JsonDecoder, JsonEncoder, JsonError}

import java.text.SimpleDateFormat
import java.util.Date

trait BatchSalesToPayoutReportRow

case class BatchSalesToPayoutReportSummaryRow(status: String,
                                              sales: Double,
                                              refunds: Double,
                                              salesRefund: Double,
                                              pending: Double,
                                              payoutDateTimestamp: Option[Long],
                                              paymentMethod: Option[String],
                                              paymentMethodDescription: Option[String],
                                              salesCount: Long,
                                              refundCount: Long) extends BatchSalesToPayoutReportRow

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
                                              payoutDateTimestamp: Long,
                                              paymentMethod: String,
                                              paymentMethodDescription: String,
                                              salesCount: Long,
                                              refundCount: Long) extends BatchSalesToPayoutReportRow

object BatchSalesToPayoutPaidOutReportRow {
  implicit val decoder: JsonDecoder[BatchSalesToPayoutPaidOutReportRow] =
    DeriveJsonDecoder.gen[BatchSalesToPayoutPaidOutReportRow]
  implicit val encoder: JsonEncoder[BatchSalesToPayoutPaidOutReportRow] =
    DeriveJsonEncoder.gen[BatchSalesToPayoutPaidOutReportRow]
}


object BatchSalesToPayoutReportRow {

  implicit val formatter: Indexable[BatchSalesToPayoutReportRow] = (t: BatchSalesToPayoutReportRow) => {
    t match {
      case v: BatchSalesToPayoutReportSummaryRow => v.toJson
      case v: BatchSalesToPayoutPaidOutReportRow => v.toJson
    }
  }

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def fromESDocRaw(esDoc: ESDoc): BatchSalesToPayoutReportRow = {
    val doc = esDoc.doc
    doc(BatchSalesToPayoutReportField.status) match {
      case status if status == "summary" =>
        BatchSalesToPayoutReportSummaryRow(
          status,
          doc(BatchSalesToPayoutReportField.sales).toDouble,
          doc(BatchSalesToPayoutReportField.refunds).toDouble,
          doc(BatchSalesToPayoutReportField.salesRefund).toDouble,
          doc(BatchSalesToPayoutReportField.pending).toDouble,
          Option(doc(BatchSalesToPayoutReportField.payoutDate)).filter(_.nonEmpty).map(dateFormat.parse).map(_.getTime),
          Option(doc(BatchSalesToPayoutReportField.paymentMethod)).filter(_.nonEmpty),
          Option(doc(BatchSalesToPayoutReportField.paymentMethodDescription)).filter(_.nonEmpty),
          doc(BatchSalesToPayoutReportField.salesCount).toLong,
          doc(BatchSalesToPayoutReportField.refundCount).toLong
        )
      case status if status == "paidOut" =>
        BatchSalesToPayoutPaidOutReportRow(
          status,
          doc(BatchSalesToPayoutReportField.sales).toDouble,
          doc(BatchSalesToPayoutReportField.refunds).toDouble,
          doc(BatchSalesToPayoutReportField.salesRefund).toDouble,
          doc(BatchSalesToPayoutReportField.pending).toLong,
          dateFormat.parse(doc(BatchSalesToPayoutReportField.payoutDate)).getTime,
          doc(BatchSalesToPayoutReportField.paymentMethod),
          doc(BatchSalesToPayoutReportField.paymentMethodDescription),
          doc(BatchSalesToPayoutReportField.salesCount).toLong,
          doc(BatchSalesToPayoutReportField.refundCount).toLong
        )
    }
  }
}
