package uk.co.odinconsultants.algernon.matrix

import org.junit.runner.RunWith
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import SparkForTesting._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, IntegerType, StructField, StructType}

@RunWith(classOf[JUnitRunner])
class SparseSparkMatrixSpec extends WordSpec with Matchers with TypeCheckedTripleEquals {

  import SparseSparkMatrix._
  import SparkForTesting.session.implicits._

  "Multiplying matrices" should {
    "generate the matrix at https://www.mathsisfun.com/algebra/matrix-multiplying.html" in {

      val converter: String => Int = _.toInt

      val A = toMatrix(
        """1 2 3
          |4 5 6
        """.stripMargin)(converter)
      val B = toMatrix(
        """7  8
          |9  10
          |11 12
        """.stripMargin)(converter)

      // TODO do the multiplication and make assertions
    }
  }

  def toMatrix[T](x: String)(implicit toNumeric: String => Int): SparseSpark = {
    def toCell(i: Int, j: Int, v: String): MatrixCell  = MatrixCell(i.toLong, j.toLong, toNumeric(v))

    def toCells(line: String, i: Int): Seq[MatrixCell] = line.split(" ").filterNot(_ == "").zipWithIndex.map { case (v, j) =>  toCell(i, j, v) }

    val rdd     = sc.parallelize(x.split("\n").zipWithIndex.flatMap { case (line, i) => toCells(line, i) })
    val schema  = StructType(Seq(StructField("i", LongType, nullable = false), StructField("j", LongType, nullable = false), StructField("x", IntegerType, nullable = false)))

    val df = sqlContext.createDataFrame(rdd.map(x => Row(x.i, x.j, x.x)), schema)

    df.as[MatrixCell]
  }

}
