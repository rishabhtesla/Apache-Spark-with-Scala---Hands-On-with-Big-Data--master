package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.graphx._

/** Some examples of GraphX in action with the Marvel superhero dataset! */
object GraphX {
  
  // Each valid line becomes (vertex ID, display name). None means the line
  // is not a usable character record; flatMap later discards it.
  def parseNames(line: String) : Option[(VertexId, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      val heroID:Long = fields(0).trim().toLong
      if (heroID < 6487) {  // ID's above 6486 aren't real characters
        return Some( fields(0).trim().toLong, fields(1))
      }
    } 
  
    None // flatmap will just discard None results, and extract data from Some results.
  }
  
  /** Turn one hero's adjacency-list row into directed edges to its co-appearing heroes. */
  def makeEdges(line: String) : List[Edge[Int]] = {
    import scala.collection.mutable.ListBuffer
    var edges = new ListBuffer[Edge[Int]]()
    val fields = line.split(" ")
    val origin = fields(0)
    for (x <- 1 until (fields.length - 1)) {
      // Our attribute field is unused, but in other graphs could
      // be used to deep track of physical distances etc.
      edges += Edge(origin.toLong, fields(x).toLong, 0)
    }
    
    edges.toList
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "GraphX")
    
     // Vertices are (hero ID, hero name) pairs. Edges connect heroes who
     // appeared together; the third Edge field is an integer edge attribute.
    val names = sc.textFile("data/marvel-names.txt")
    val verts = names.flatMap(parseNames)
    
    val lines = sc.textFile("data/marvel-graph.txt")
    val edges = lines.flatMap(makeEdges)    
    
    // The default value supplies an attribute for any edge endpoint lacking
    // an explicit name. Cache because the graph is queried more than once.
    val default = "Nobody"
    val graph = Graph(verts, edges, default).cache()
    
    // degree is the number of incident connections per vertex. Joining with
    // verts adds each hero's name; take(10) returns only ten rows to the driver.
    println("\nTop 10 most-connected superheroes:")
    // The join merges the hero names into the output; sorts by total connections on each node.
    graph.degrees.join(verts).sortBy(_._2._1, ascending=false).take(10).foreach(println)


    // Pregel iteratively sends messages over edges. The vertex attribute here
    // is the shortest distance found so far from the chosen root.
    println("\nComputing degrees of separation from SpiderMan...")
    
    // Start from SpiderMan
    val root: VertexId = 5306 // SpiderMan
    
    // The starting hero has distance zero; infinity means not reached yet.
    val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)

    // Pregel arguments: initial message, maximum iterations, vertex update,
    // message-send rule, and message-reduction rule.
    val bfs = initialGraph.pregel(Double.PositiveInfinity, 10)( 
        // Our "vertex program" preserves the shortest distance
        // between an inbound message and its current value.
        // It receives the vertex ID we are operating on,
        // the attribute already stored with the vertex, and
        // the inbound message from this iteration.
        (id, attr, msg) => math.min(attr, msg),
        
        // Our "send message" function propagates out to all neighbors
        // with the distance incremented by one.
        triplet => { 
          if (triplet.srcAttr != Double.PositiveInfinity) { 
            Iterator((triplet.dstId, triplet.srcAttr+1)) 
          } else { 
            Iterator.empty 
          } 
        }, 
        
        // The "reduce" operation preserves the minimum
        // of messages received by a vertex if multiple
        // messages are received by one vertex
        (a,b) => math.min(a,b) ).cache()
    
    // Print out the first 100 results:
    bfs.vertices.join(verts).take(100).foreach(println)
    
    // Recreate our "degrees of separation" result:
    println("\n\nDegrees from SpiderMan to ADAM 3,031")  // ADAM 3031 is hero ID 14
    bfs.vertices.filter(x => x._1 == 14).collect.foreach(println)
    
  }
}