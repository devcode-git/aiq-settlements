package com.devcode.accountiq.settlement.services

import com.devcode.accountiq.settlement.elastic.dao.ElasticSearchDAO
import com.devcode.accountiq.settlement.elastic.reports.batch.BatchSalesToPayoutReportRow
import com.devcode.accountiq.settlement.elastic.reports.merchant.MerchantPaymentTransactionsReportRow
import com.devcode.accountiq.settlement.elastic.reports.settlement.SettlementDetailReportRow
import com.devcode.accountiq.settlement.recoonciliation.{ReconTimeFrame, ReconcileCmd}
import zio._

import scala.util.{Success, Failure}

object ReconciliationService {

  def reconcile(reconcileCmd: ReconcileCmd) = {

    ZIO.succeed(s"merchant is ${reconcileCmd.merchant}, provider is ${reconcileCmd.provider}, tf is ${reconcileCmd.timeFrame.toString}}")
  }

  def reconcileMerchantReports(merchantReports: List[MerchantPaymentTransactionsReportRow],
                               settlementDetailReport: List[SettlementDetailReportRow]) = {
    val (settlementDetailMatchedReports, settlementDetailUnmatchedReports) = settlementDetailReport.partition { merchantReport =>
      merchantReports.find {
        r => r.txRef == merchantReport.merchantReference
      }.exists { r =>
        //todo: check gross debit ?
        // merchantReport.amount is "10 EUR" or "23.75 RON"
        // settlement.grossCredit is 30 (usually is EURO) guess all adyen is eur
        // remove .toLong and use separate column
        r.amount.value == merchantReport.grossCredit
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
