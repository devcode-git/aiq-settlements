package com.devcode.accountiq.settlement.util

import com.sksamuel.elastic4s.ElasticDate

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId}
import java.util.Date

object DateUtil {
  implicit class LocalDateConverter(dateToConvert: Date) {
    def toLocalDateTime: LocalDateTime = LocalDateTime.ofInstant(dateToConvert.toInstant, ZoneId.systemDefault())
  }

  implicit class LocalDateTimeConverter(dateToConvert: LocalDateTime) {
    def toElasticDate: ElasticDate = ElasticDate.fromTimestamp(Timestamp.valueOf(dateToConvert).getTime)
  }
}
