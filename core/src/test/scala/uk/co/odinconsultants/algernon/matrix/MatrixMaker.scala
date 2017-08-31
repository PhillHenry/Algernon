package uk.co.odinconsultants.algernon.matrix

import org.apache.spark.sql.Encoder
import uk.co.odinconsultants.algernon.matrix.SparkForTesting.sc

import scala.reflect.runtime.universe.TypeTag

object MatrixMaker {

  import SparkForTesting.session.implicits._
  import SparseSparkMatrix._

  val toNumeric: String => Int = _.toInt
  val toDouble: String => Double = _.toDouble

  def asString[T](xs: Seq[MatrixCell[T]]): String = {
    val width   = xs.map(_.j).max.toInt
    val height  = xs.map(_.i).max.toInt
    val padding = xs.map(_.x.toString.length).max + 1
    val rows    = xs.groupBy(_.i)
    val string  = new StringBuffer()
    for (i <- 0 to height) {
      val row = rows(i).map(x => x.j -> x.x).toMap
      for (j <- 0 to width) {
        val str = row.getOrElse(j, 0)
        string.append(s"%-${padding}s".format(str))
      }
      string.append("\n")
    }
    string.toString
  }

  def toMatrix[T: Encoder : TypeTag : Numeric](x: String, toNumeric: String => T): SparseSpark[T] = {

    val rdd = sc.parallelize(asCells(x, toNumeric))

    SparkForTesting.session.createDataset(rdd)
  }

  def asCells[T: Encoder : TypeTag : Numeric](x: String, toNumeric: (String) => T): Seq[MatrixCell[T]] = {
    def toCell(i: Int, j: Int, v: String): MatrixCell[T] = MatrixCell(i.toLong, j.toLong, toNumeric(v))

    def toCells(line: String, i: Int): Seq[MatrixCell[T]] = line.split(" ").filterNot(_ == "").zipWithIndex.map { case (v, j) => toCell(i, j, v) }

    x.split("\n").zipWithIndex.flatMap { case (line, i) => toCells(line, i) }
  }
}
