import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {
  def main(args: Array[ String ]) {

        val conf = new SparkConf().setAppName("Graph")
        val sc = new SparkContext(conf)
        val data = sc.textFile(args(0))

        def transformed_data(row:Array[String]): (Long,Long,List[Long]) =
        {
         (
           row(0).toLong,
           row(0).toLong,
           row.tail.map(_.toString.toLong).toList
         )
        }

        var graph = data.map(line => {transformed_data(line.split(","))})
        val mapped_graph = graph.map(tuple=>(tuple._1,tuple))
       for(i <- 1 to 5)
        {
          graph =
            graph
            .flatMap(map => map match{ case (x, y, tuple) => (x, y) :: tuple.map(p => (p, y))})
            .reduceByKey((x, y) => math.min(x,y))
            .join(mapped_graph)
            .map(x => (x._2._2._2, x._2._1, x._2._2._3))
        }

        val result =
          graph
          .map(node => (node._2, 1))
          .reduceByKey(_+_)
          .sortByKey()
          .map {case (node,count) => node+" "+count}


        result
          .collect()
          .foreach(println)

          sc.stop()
      }
}

// import org.apache.spark.SparkContext
// import org.apache.spark.SparkConf

// object Graph {
//   def main(args: Array[ String ]) {
//   }
// }
