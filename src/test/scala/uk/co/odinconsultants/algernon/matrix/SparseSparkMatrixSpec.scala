package uk.co.odinconsultants.algernon.matrix

import org.apache.spark.sql.Encoder
import org.junit.runner.RunWith
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.algernon.matrix.SparkForTesting._

import scala.reflect.runtime.universe.TypeTag

@RunWith(classOf[JUnitRunner])
class SparseSparkMatrixSpec extends WordSpec with Matchers with TypeCheckedTripleEquals {

  import SparkForTesting.session.implicits._
  import SparseSparkMatrix._

  "Multiplying matrices" should {
    "generate the matrix at https://www.mathsisfun.com/algebra/matrix-multiplying.html" in {

      val toNumeric: String => Int = _.toInt

      val A = toMatrix[Int](
        """1 2 3
          |4 5 6
        """.stripMargin, toNumeric)
      val B = toMatrix[Int](
        """7  8
          |9  10
          |11 12
        """.stripMargin, toNumeric)

      // TODO do the multiplication and make assertions
    }
  }

  def toMatrix[T: Encoder : TypeTag](x: String, toNumeric: String => T): SparseSpark[T] = {

    def toCell(i: Int, j: Int, v: String): MatrixCell[T]  = MatrixCell(i.toLong, j.toLong, toNumeric(v))

    def toCells(line: String, i: Int): Seq[MatrixCell[T]] = line.split(" ").filterNot(_ == "").zipWithIndex.map { case (v, j) =>  toCell(i, j, v) }

    val rdd = sc.parallelize(x.split("\n").zipWithIndex.flatMap { case (line, i) => toCells(line, i) })

    session.createDataset(rdd)
  }

}
