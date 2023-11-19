package com.devcode.accountiq.settlement.util

import java.time.LocalDate
import java.util.Date

object DateUtil {
  implicit class LocalDateConverter(dateToConvert: Date) {
    def toLocalDate: LocalDate = new java.sql.Date(dateToConvert.getTime).toLocalDate
  }
}
