package com.devcode.accountiq.settlement.services

import com.devcode.accountiq.settlement.elastic.{ReconcileStatus, TransactionRow}
import com.devcode.accountiq.settlement.elastic.dao.ElasticSearchDAO
import com.devcode.accountiq.settlement.elastic.reports.batch.BatchSalesToPayoutReportRow
import com.devcode.accountiq.settlement.elastic.reports.merchant.MerchantPaymentTransactionsReportRow
import com.devcode.accountiq.settlement.elastic.reports.settlement.SettlementDetailReportRow
import com.devcode.accountiq.settlement.recoonciliation.{ReconTimeFrame, ReconcileCmd}
import zio._

import scala.util.{Failure, Success}

object ReconciliationService {

  def reconcile(reconcileCmd: ReconcileCmd) = {
    for {
      _ <- ZIO.logInfo(s"Starting reconcile, merchant is ${reconcileCmd.merchant}, provider is ${reconcileCmd.provider}, tf is ${reconcileCmd.timeFrame.toString}}")
      settlementReports <- findSettlementDetailReportRow(reconcileCmd)
      merchantReports <- findMerchantPaymentTransactionsReports(reconcileCmd)
      // TODO: use ZIO.partition inside and return 2 lists - success/failure validated
      transactions <- validateSettlementAndMerchantReports(settlementReports.toList, merchantReports.toList)
      (matchedTransactions, unmatchedTransactions) = transactions.partition(t => t.reason.isDefined)
      batchReports <- findBatchSalesToPayoutReports(reconcileCmd)
      transactions <- validateTransactionsAndBatchReports(matchedTransactions, batchReports.toList)
      _ <- ZIO.logInfo(transactions.mkString(","))
    } yield transactions
  }

  private def toTransactionRowWithReason(sr: SettlementDetailReportRow, reason: String): TransactionRow = {
    TransactionRow(
      sr.merchantReference,
      None, // processingAmount is taken from merchant report later
      None, // processingCurrency is taken from merchant report later
      None, // transactionType is taken from merchant report later
      None, // transactionDate is taken from merchant report later, can we take sr.creationDate ?
      sr.payoutDate,
      sr.paymentMethod,
      sr.grossCredit,
      sr.netCurrency,
      sr.netCredit,
      sr.commission,
      sr.exchangeRate,
      sr.batchNumber,
      None, // batchCreditAmount is taken from batch report later
      ReconcileStatus.UNMATCHED.toString,
      Some(reason)
    )
  }

  private def toTransactionRowWithMerchantReport(sr: SettlementDetailReportRow, merchantReport: MerchantPaymentTransactionsReportRow): TransactionRow = {
    TransactionRow(
      sr.merchantReference,
      Some(merchantReport.amount.value), // processingAmount is taken from merchant report later
      Some(merchantReport.amount.cy), // processingCurrency is taken from merchant report later
      Some(merchantReport.txType), // transactionType is taken from merchant report later
      Some(merchantReport.booked), // transactionDate is taken from merchant report later
      sr.payoutDate,
      sr.paymentMethod,
      sr.grossCredit,
      sr.netCurrency,
      sr.netCredit,
      sr.commission,
      sr.exchangeRate,
      sr.batchNumber,
      None, // batchCreditAmount is taken from batch report later
      ReconcileStatus.MATCHED.toString,
      None
    )
  }

  private def validateTransactionsAndBatchReports(transactions: List[TransactionRow],
                                                  batchReports: List[BatchSalesToPayoutReportRow]) = {
    ZIO.succeed(transactions.map { transaction =>
      batchReports.find { batchReport =>
        batchReport.payoutDate == transaction.payoutDate
      } match {
        case Some(batchReport) => transaction.copy(batchCreditAmount = Some(batchReport.sales))
        case None =>
          val reason = s"Batch report with payout date ${transaction.payoutDate} was not found"
          transaction.copy(reconciliedStatus = ReconcileStatus.UNMATCHED.toString, reason = Some(reason))
      }
    })
  }

  private def validateSettlementAndMerchantReports(settlementDetailReport: List[SettlementDetailReportRow],
                                           merchantReports: List[MerchantPaymentTransactionsReportRow]): ZIO[Any, Throwable, List[TransactionRow]] = {
    ZIO.foreach(settlementDetailReport) {
      settlementReport =>
        // validation step 1: Transaction reference (Merchant Report) = Transaction reference (Settlement Report)
        val merchantReportWithSameRefId = merchantReports.find {
          _.txRef == settlementReport.merchantReference
        }

        merchantReportWithSameRefId match {
          case Some(merchantReport) =>
            val (merchantAmount, merchantCurrency) = (merchantReport.amount.value, merchantReport.amount.cy)
            val settlementAmount = settlementReport.grossCredit * settlementReport.exchangeRate
            val settlementCurrency = settlementReport.netCurrency
            // validation step 3: Processing Amount (Merchant Report) == Gross Settled Amount * Forex (Settlement Report)
            if (merchantAmount == settlementAmount && merchantCurrency == settlementCurrency) {
              ZIO.succeed(toTransactionRowWithMerchantReport(settlementReport, merchantReport))
            } else {
              val reason = s"Merchant report with txRef ${settlementReport.merchantReference} has a different amount value. Merchant report: , Settlement report: ${}"
              ZIO.logWarning(reason) *>
                ZIO.succeed(toTransactionRowWithReason(settlementReport, reason))
            }
          case None =>
            val reason = s"Merchant report with txRef ${settlementReport.merchantReference} was not found"
            ZIO.logWarning(reason) *>
              ZIO.succeed(toTransactionRowWithReason(settlementReport, reason))
        }
    }
  }

  def reconcileSettlementDetailReportToMerchantReportAmount(merchantReports: List[MerchantPaymentTransactionsReportRow],
                                                            settlementDetailReport: List[SettlementDetailReportRow]) = {
    // TODO: move it, are there nay other providers ?
    val adyenProviderName = "Adyen payconiq Deposit"
//    val merchantAmount = merchantReports.filter(_.provider == adyenProviderName).reduce(_.amount.value + _.amount.value)
//    val settlementDetailAmount = settlementDetailReport.reduce((a, b) => a.grossCredit * a.exchangeRate + b.grossCredit * b.exchangeRate)
  }

  // Formula: (((Gross Settled Amount * Provider_Forex - Gross Settled Amount * AccountIQ_Forex) x100)/(Gross Settled Amount * Provider_Forex) < 5
  def reconcileProviderForex(settlementDetailReport: List[SettlementDetailReportRow], accountIQExchangeRate: Double): Boolean = {
    true
//    val grossCreditWithProviderForex = settlementDetailReport.reduce((a, b) => a.grossCredit * a.exchangeRate + b.grossCredit * b.exchangeRate)
//    val grossCreditWithAccountIQForex = settlementDetailReport.reduce((a, b) => a.grossCredit * accountIQExchangeRate + b.grossCredit * accountIQExchangeRate)
//    ((grossCreditWithProviderForex - grossCreditWithAccountIQForex) * 100)/(grossCreditWithProviderForex) < 5
  }

  def findBatchSalesToPayoutReports(reconcileCmd: ReconcileCmd): ZIO[ElasticSearchDAO[BatchSalesToPayoutReportRow], Throwable, IndexedSeq[BatchSalesToPayoutReportRow]] = {
    for {
      dao <- ZIO.service[ElasticSearchDAO[BatchSalesToPayoutReportRow]]
      reports <- dao.find(reconcileCmd.timeFrame.start, reconcileCmd.timeFrame.end, reconcileCmd.merchant, reconcileCmd.provider)
      results <- ZIO.attempt(reports.result.map {
        case Success(v) => v
      })
      _ <- ZIO.logDebug(reports.toString)
    } yield results
  }

  def findMerchantPaymentTransactionsReports(reconcileCmd: ReconcileCmd): ZIO[ElasticSearchDAO[MerchantPaymentTransactionsReportRow], Throwable, IndexedSeq[MerchantPaymentTransactionsReportRow]] = {
    for {
      dao <- ZIO.service[ElasticSearchDAO[MerchantPaymentTransactionsReportRow]]
      reports <- dao.find(reconcileCmd.timeFrame.start, reconcileCmd.timeFrame.end, reconcileCmd.merchant, reconcileCmd.provider)
      results <- ZIO.attempt(reports.result.map {
        case Success(v) => v
      })
      _ <- ZIO.logDebug(reports.toString)
    } yield results
  }

  def findSettlementDetailReportRow(reconcileCmd: ReconcileCmd): ZIO[ElasticSearchDAO[SettlementDetailReportRow], Throwable, IndexedSeq[SettlementDetailReportRow]] = {
    for {
      dao <- ZIO.service[ElasticSearchDAO[SettlementDetailReportRow]]
      reports <- dao.find(reconcileCmd.timeFrame.start, reconcileCmd.timeFrame.end, reconcileCmd.merchant, reconcileCmd.provider)
      results <- ZIO.attempt(reports.result.map {
        case Success(v) => v
      })
      _ <- ZIO.logDebug(reports.toString)
    } yield results
  }

}
