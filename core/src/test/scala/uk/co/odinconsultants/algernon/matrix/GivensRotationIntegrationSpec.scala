package uk.co.odinconsultants.algernon.matrix

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class GivensRotationIntegrationSpec extends WordSpec with Matchers {

  import MatrixMaker._
  import SparkForTesting.session.implicits._
  import SparseSparkMatrix._

  private implicit val session: SparkSession = SparkForTesting.session

  "Givens factorization" should {
    val width   = 20
    val height  = 20
    val density = 0.1
    val factor  = 100d
    val cells   = quasiRandomSparseMatrix(width, height, density, factor)
    val matrix  = toSparseMatrix(cells)

    "first get the indices which are relevant" in {
      val actual = matrix.givensIndexes.collect()
      actual.foreach(x => println("x = " + x))
      actual.length shouldBe cells.count(lowerTriangular)
    }

    "produce an upper triangular matrix" ignore {

      val R = matrix.givensRotation().collect()

      R.filter(lowerTriangular) shouldBe empty
    }
  }

}
