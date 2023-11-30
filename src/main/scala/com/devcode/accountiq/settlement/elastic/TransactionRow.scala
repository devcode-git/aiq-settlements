package com.devcode.accountiq.settlement.elastic


import com.sksamuel.elastic4s.ElasticApi.{dateField, properties}
import com.sksamuel.elastic4s.ElasticDsl.keywordField
import com.sksamuel.elastic4s.Indexable
import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, EncoderOps, JsonDecoder, JsonEncoder}

import java.time.LocalDate

case class TransactionRow(transactionReference: String,
                          processingAmount: Double,
                          processingCurrency: String,
                          transactionType: String,
                          transactionDate: LocalDate,
                          paymentMethod: String,
                          grossSettledAmount: Double,
                          settledCurrency: String,
                          netSettledAmount: Double,
                          providerFees: Double,
                          forex: Double,
                          batchNumber: Double,
                          batchCreditAmount: Double,
                          batchCurrency: String,
                          reconciliedStatus: String,
                          reason: String)

object TransactionRow {

  val mapping = properties(
    keywordField("transactionReference"),
    keywordField("processingAmount"),
    keywordField("processingCurrency"),
    keywordField("transactionType"),
    dateField("transactionDate"),
    keywordField("paymentMethod"),
    keywordField("grossSettledAmount"),
    keywordField("settledCurrency"),
    keywordField("netSettledAmount"),
    keywordField("providerFees"),
    keywordField("forex"),
    keywordField("batchNumber"),
    keywordField("batchCreditAmount"),
    keywordField("batchCurrency"),
    keywordField("reconciliedStatus"),
    keywordField("reason")
  )

  implicit val decoder: JsonDecoder[TransactionRow] =
    DeriveJsonDecoder.gen[TransactionRow]

  implicit val encoder: JsonEncoder[TransactionRow] =
    DeriveJsonEncoder.gen[TransactionRow]

  implicit val formatter: Indexable[TransactionRow] = (t: TransactionRow) =>
    t.toJson

}