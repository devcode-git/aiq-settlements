package com.devcode.accountiq.settlement.transformer

import org.apache.poi.ss.usermodel.{CellType, DateUtil, WorkbookFactory}
import zio._

import java.io.FileInputStream
import java.nio.file.Path
import java.text.{DecimalFormat, DecimalFormatSymbols, SimpleDateFormat}
import java.util.Locale
import scala.jdk.CollectionConverters.asScalaIteratorConverter

object XLSXParser {

  // this is the best way how to convert doubles to strings and avoid values like 1.07794593E8 and etc
  // for example: 12345678D.toString returns 1.2345678E7
  // the solution from https://stackoverflow.com/a/25307973
  val doubleFormat = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH))
  doubleFormat.setMaximumFractionDigits(340) // 340 = DecimalFormat.DOUBLE_FRACTION_DIGITS

  def parse(path: Path, dateFormat: SimpleDateFormat): ZIO[Any, Nothing, List[List[String]]] = {
    val wb = WorkbookFactory.create(new FileInputStream(path.toFile))
    val sheet = wb.getSheetAt(0)
    println(s" total row= ${sheet.getLastRowNum}")
    val rows = (for (i <- 0 to sheet.getLastRowNum)
      yield try
        Some(sheet.getRow(i))
      catch {
        case ex: Throwable =>
          //          log.error(s"failed to parse row: $i ", ex);
          None
      }).flatten.map(row => row.cellIterator().asScala.toList.map { cell =>
      cell.getCellTypeEnum match {
        case CellType._NONE => ""
        case CellType.NUMERIC =>
          if (DateUtil.isCellDateFormatted(cell)) {
            dateFormat.format(cell.getDateCellValue)
          } else {
            doubleFormat.format(cell.getNumericCellValue)
          }
        case CellType.STRING => cell.getStringCellValue
        case CellType.FORMULA => cell.getCellFormula
        case CellType.BLANK => ""
        case CellType.BOOLEAN => cell.getBooleanCellValue.toString
        case CellType.ERROR => cell.getErrorCellValue.toString
      }
    })

    ZIO.succeed(rows.toList)
  }

}
