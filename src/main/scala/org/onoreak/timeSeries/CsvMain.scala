package org.onoreak.timeSeries

import java.io.{File, _}
import java.time.temporal.ChronoUnit
import java.util.Locale

import be.cetic.tsimulus.config.Configuration
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat
import scopt.OptionParser
import spray.json._

import scala.io.Source;


/**
  * Created by Chris on 5/1/17.
  */
object CsvMain
{
  case class TsCsvConfig(configFile: String = "", nullValue: String = "", rowsPerFile: Int = 100000,
                         outputFilePath: String = "", outputFileBaseName: String = "output")

  implicit object DurationOrdering extends Ordering[org.joda.time.Duration]{
    def compare(x: org.joda.time.Duration, y: org.joda.time.Duration): Int = x.getMillis.compare(y.getMillis)
  }

  val parser = new OptionParser[TsCsvConfig]("CSV TS Generator") {
    head("TimeSeries Simulator", "0.1")

    arg[String]("configFile").required().action( (x, c) =>
      c.copy(configFile = x) ).text("The Path to the generator configuration JSON file")

    opt[String]("nullValue").valueName("\"\"").
      action( (x, c) => c.copy(nullValue = x) ).
      text("The character to use for a null value")

    opt[Int]("rowsPerFile").valueName("100000").action( (x, c) => c.copy(rowsPerFile = x) ).
      text("The number of rows to be written before a new file is created.")

    opt[String]("outputFilePath").valueName(new java.io.File(".").getCanonicalPath).action( (x, c) => c.copy(outputFilePath = x) ).
      text("The path to the directory where the output files will be stored.")

    opt[String]("outputFileBaseName").valueName("output").action( (x, c) => c.copy(outputFilePath = x) ).
      text("The base file name to use. A -# will be appended as the file will be split amongst several CSV files.")
  }

  def main(args: Array[String]): Unit =
  {

    var fileName = "output"
    var rowsPerFile = 100000;
    var nullChar = ""
    var basePath = ""
    var configFileName = ""

    parser.parse(args, TsCsvConfig()) map { config =>
      rowsPerFile = config.rowsPerFile
      fileName = config.outputFileBaseName
      nullChar = config.nullValue
      basePath = config.outputFilePath
      configFileName = config.configFile
    } getOrElse System.exit(1)

    println(basePath)

    val content = Source .fromFile(new File(configFileName))
      .getLines()
      .mkString("\n")




    val startDate = "01-JAN-1800 12.00.00 AM"
    val pattern = "dd-MMM-yyyy hh.mm.ss aa"
    val config = Configuration(content.parseJson)

    val formatString = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    val dtf = DateTimeFormat.forPattern(formatString)

    var rowCount = 0
    var reset = true
    var fileIter = 0
    var file = new File(basePath + fileName + "-" + fileIter +  ".csv")
    var bw = new BufferedWriter(new FileWriter(file))
    val formatter = java.text.NumberFormat.getNumberInstance(Locale.US);
    formatter.setMaximumFractionDigits(3);
    val queryFreq = config.series.minBy(_.frequency).frequency.getMillis

    var currentTs = config.from;

    // Setup a map to keep track of the individual frequencies of the tags
    val freq = scala.collection.mutable.Map[String,(Long, LocalDateTime)]()
    config.series foreach (e => freq.put(e.name, (e.frequency.getMillis, LocalDateTime.parse(startDate, DateTimeFormat.forPattern(pattern)))))

    while (currentTs.isBefore(config.to)) {

      // Get the points for the array of tags listed in exported
      val value = Utils.getTimeValue(config.timeSeries, currentTs)

      // Reset the for loop counter
      var j = 0

      for (d <- value) {

        // Init the csv row
        var dataRow = ""

        // Print out the title if not yet done
        if (reset) {
          if (j == 0) dataRow = dataRow + "Time, " + d._1
          else dataRow = dataRow + d._1
          if (j != value.size - 1) dataRow = dataRow + ","
          else dataRow = dataRow + util.Properties.lineSeparator
        }

        //assume we have printed the header and are now going to print out the data
        else {
          // If this is the first iteration, print out the time if not yet done
          if (j == 0) dataRow = dataRow + dtf.print(d._2) + ","

          // Get the frequency of the current tag
          val dtFreq = freq(d._1)._2

          // Get the current Generation Time
          val dtGen = java.time.LocalDateTime.parse(dtf.print(d._2), java.time.format.DateTimeFormatter.ofPattern(formatString))
          // Get the last time the data was generated
          val dtLast = java.time.LocalDateTime.parse(dtf.print(dtFreq), java.time.format.DateTimeFormatter.ofPattern(formatString))

          // Check if the data frequency has expired and we need to record another value or insert null
          if (ChronoUnit.MILLIS.between(dtLast, dtGen) < freq(d._1)._1) {
            // Assume we inserted null, therefore insert null
            dataRow = dataRow + nullChar
            // Test if this is the last tag in the map, if not close with , otherwise new line
            if (j != value.size - 1) dataRow = dataRow + ","
            else dataRow = dataRow + util.Properties.lineSeparator
          }
          else {
            // Assume the time expired and we need to enter a value
            dataRow = dataRow + formatter.format(d._3.asInstanceOf[Option[Double]].getOrElse(nullChar))
            // Update the last time the tag was generated
            freq.update(d._1, (freq(d._1)._1, d._2))
            // Test if this is the last tag in the map, if not close with , otherwise new line
            if (j != value.size - 1) dataRow = dataRow + ","
            else dataRow = dataRow + util.Properties.lineSeparator
          }
        }
        // Increase the tag count iterator
        j = j + 1

        // Write the data row
        bw.write(dataRow)
        rowCount += 1
        if (rowCount % 10 == 0) bw.flush()

      } // Close For

      // Only add if the reset is not true
      if (reset == false) currentTs = currentTs.plusMillis(queryFreq.asInstanceOf[Int])

      // reset if it was true
      else reset = false

      // Check if we have written 100000 rows
      if (rowCount == rowsPerFile) {
        fileIter += 1;
        file = new File(basePath + fileName + "-" + fileIter + ".csv")
        bw = new BufferedWriter(new FileWriter(file))
        reset = true
        rowCount = 0
      }

    } // Close While
    // Close the file
    bw.close()

    println("Done!")
  }
}