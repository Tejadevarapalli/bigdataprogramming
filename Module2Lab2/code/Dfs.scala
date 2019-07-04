import org.apache.spark.{SparkConf, SparkContext}
object Dfs {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\winutils")
    val conf = new SparkConf().setAppName("dfs").setMaster("local[*]")
    val sc = new SparkContext(conf)
    type Vertex = Int
    type Graph = Map[Vertex, List[Vertex]]
    val g: Graph = Map(1 -> List(2,3,5,6,7), 2 -> List(1,3,4,6,7), 3 -> List(1,2), 4 -> List(2,5,7),5 -> List(1,6,7),6 -> List(1,2,5,7),7 -> List(1,2,4,5,6))

   def DFS(start: Vertex, g: Graph): List[Vertex] = {

      def DFS0(v: Vertex, visited: List[Vertex]): List[Vertex] = {
        if (visited.contains(v))
          visited
        else {
          val neighbours:List[Vertex] = g(v) filterNot visited.contains
          neighbours.foldLeft(v :: visited)((b,a) => DFS0(a,b))
        }
      }
      DFS0(start,List()).reverse
    }
    val dfsresult=DFS(4,g )
    println(dfsresult.mkString(","))

  }
}
