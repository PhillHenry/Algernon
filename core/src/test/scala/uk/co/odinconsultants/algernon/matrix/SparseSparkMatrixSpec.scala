package uk.co.odinconsultants.algernon.matrix

import org.junit.runner.RunWith
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class SparseSparkMatrixSpec extends WordSpec with Matchers with TypeCheckedTripleEquals {

  import MatrixMaker._
  import SparkForTesting.session.implicits._
  import SparseSparkMatrix._

  val A: SparseSpark[Int] = toMatrix[Int](
    """1 2 3
      |4 5 6
    """.stripMargin, toNumeric)
  val B: SparseSpark[Int] = toMatrix[Int](
    """7  8
      |9  10
      |11 12
    """.stripMargin, toNumeric)

  private implicit val session = SparkForTesting.session

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

}
