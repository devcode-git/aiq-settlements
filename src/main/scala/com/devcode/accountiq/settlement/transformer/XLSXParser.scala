package com.devcode.accountiq.settlement.transformer

import org.apache.poi.ss.usermodel.{CellType, WorkbookFactory}

import java.io.{File, FileInputStream}
import java.nio.file.Path
import scala.jdk.CollectionConverters.asScalaIteratorConverter
import zio._

object XLSXParser {

  def parse(path: Path): ZIO[Any, Nothing, List[List[String]]] = {
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
        case CellType.NUMERIC => cell.getNumericCellValue.toString
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
