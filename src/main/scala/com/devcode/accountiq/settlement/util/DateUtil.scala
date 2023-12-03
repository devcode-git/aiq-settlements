package com.devcode.accountiq.settlement.util

import java.time.{LocalDateTime, ZoneId}
import java.util.Date

object DateUtil {
  implicit class LocalDateConverter(dateToConvert: Date) {
    def toLocalDateTime: LocalDateTime = LocalDateTime.ofInstant(dateToConvert.toInstant, ZoneId.systemDefault())
  }
}
