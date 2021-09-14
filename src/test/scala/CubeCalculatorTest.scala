import Training.CubeCalculator
import org.scalatest.funsuite.AnyFunSuite

class CubeCalculatorTest extends AnyFunSuite {

    test("CubeCalculator.cube") {
      println(assert(CubeCalculator.cube(3) === 27))
      println(assert(CubeCalculator.cube(0) === 0))
    }

}
