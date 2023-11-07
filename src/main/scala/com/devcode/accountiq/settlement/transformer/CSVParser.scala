package com.devcode.accountiq.settlement.transformer

import com.devcode.accountiq.settlement.elastic.ESDoc

import java.nio.file.Path
import com.github.tototoshi.csv._
import zio.stream.ZStream

object CSVParser {

  def parseESDocs(path: Path, defaultDelimiter: Char = ',') = {
    parse(path, defaultDelimiter).map(createESDocs)
  }

  private def createESDocs(rows: Seq[Seq[String]]) = {
    val headerRow = rows.head
    val dataRows = rows.tail

    dataRows.map(row => headerRow.zip(row)).map(_.toMap).map(ESDoc(_)).toList
  }

  private def parse(path: Path, defaultDelimiter: Char = ',') = {
    implicit object csvFormat extends DefaultCSVFormat {
      override val delimiter: Char = defaultDelimiter
    }
    for {
      rows <- ZStream.fromIterator(CSVReader.open(path.toFile).iterator).runCollect
    } yield rows
  }

}
