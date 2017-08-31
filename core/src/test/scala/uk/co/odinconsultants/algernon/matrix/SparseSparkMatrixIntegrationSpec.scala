package uk.co.odinconsultants.algernon.matrix

import java.lang.Math.pow

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class SparseSparkMatrixIntegrationSpec extends WordSpec with Matchers with TypeCheckedTripleEquals {

  import MatrixMaker._
  import SparkForTesting.session.implicits._
  import SparseSparkMatrix._

  private val AString: String =
    """1 2 3
      |4 5 6""".stripMargin

  val A: SparseSpark[Int] = toMatrix[Int](
    AString, toNumeric)
  val B: SparseSpark[Int] = toMatrix[Int](
    """7  8
      |9  10
      |11 12
    """.stripMargin, toNumeric)
  val toRotate: SparseSpark[Double] = toMatrix[Double](
    """1  2  3
      |-6 1 -4
      |-7 1  9""".stripMargin, toDouble)

  private val toRotateCells: mutable.WrappedArray[MatrixCell[Double]] = toRotate.collect()

  private implicit val session: SparkSession = SparkForTesting.session

  "Rotating the matrix in a certain way" should {
    "zero out a given cell" in {
      val cells = toRotate.make0(1, 0).collect()
      withClue(s"Starting with:\n${asString(toRotateCells)}\n\nOutput Matrix:\n${asString(cells)}\n") {
        cells should have size 8
        cells filter(c => c.i == 1 && c.j == 0) shouldBe empty
      }
    }
  }

  "Getting a value" should {
    "be non-zero if it exists" in {
      getOr0(toRotateCells, 1, 0) should not be 0d
      getOr0(toRotateCells, 0, 1) should not be 0d
      getOr0(toRotateCells, 0, 0) should not be 0d
      getOr0(toRotateCells, 1, 1) should not be 0d
    }
    "be 0 if there is no entry" in {
      getOr0(toRotateCells, 3, 3) shouldBe 0d
    }
  }

  "Multiplying matrices" should {
    "generate the matrix at https://www.mathsisfun.com/algebra/matrix-multiplying.html" in {
      val cells = A.multiply(B).collect()
      cells should have size 4
      cells should contain (MatrixCell(0, 0, 58))
      cells should contain (MatrixCell(0, 1, 64))
      cells should contain (MatrixCell(1, 0, 139))
      cells should contain (MatrixCell(1, 1, 154))
    }
  }

  "Transposing matrix" should {
    "swap rows and columns" in {
      val cells = A.transpose.collect()
      cells should have size 6
      cells should contain (MatrixCell(0, 0, 1))
      cells should contain (MatrixCell(0, 1, 4))
      cells should contain (MatrixCell(1, 0, 2))
      cells should contain (MatrixCell(1, 1, 5))
      cells should contain (MatrixCell(2, 0, 3))
      cells should contain (MatrixCell(2, 1, 6))
    }
  }

  "Frobenius norm" should {
    "be the root of the sum of the squares of the cells" in {
      val expected = pow(asCells(AString, toNumeric).map(_.x).foldLeft(0d) { case (acc, x) => acc + pow(x, 2)}, 0.5)
      A.frobeniusNormSquared shouldBe 91 //pow(91, 0.5)
    }
  }

}
