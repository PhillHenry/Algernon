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
    val width   = 10
    val height  = 10
    val density = 0.2
    val factor  = 100d
    val cells   = quasiRandomSparseMatrix(width, height, density, factor)
    val matrix  = toSparseMatrix(cells)

    "produce an upper triangular matrix" ignore { // as it runs like a dog

      val R = matrix.givensRotation

      R.filter(lowerTriangular).collect() shouldBe empty
    }

    "first get the indices which are relevant" in {
      val actual = matrix.givensIndexes.collect()
      actual.foreach(x => println("x = " + x))
      actual.length shouldBe cells.count(lowerTriangular)
    }
  }

}
