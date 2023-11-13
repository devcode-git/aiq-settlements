package com.devcode.accountiq.settlement.transformer

import java.text.{DecimalFormat, DecimalFormatSymbols, SimpleDateFormat}
import java.util.Locale

object AIQParserUtil {

  // this is the best way how to convert doubles to strings and avoid values like 1.07794593E8 and etc
  // for example: 12345678D.toString returns 1.2345678E7
  // the solution from https://stackoverflow.com/a/25307973
  val doubleFormat = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH))
  doubleFormat.setMaximumFractionDigits(340) // 340 = DecimalFormat.DOUBLE_FRACTION_DIGITS

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

}
