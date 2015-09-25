package org.neo4j.cypher.docgen.cookbook

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.docgen.tooling.{DocBuilder, DocumentingTest, ResultAssertions}

class NewPrettyGraphsTest extends DocumentingTest with QueryStatisticsTestSupport {
  override def doc = new DocBuilder {
    doc("Pretty graphs", "cypher-cookbook-pretty-graphs")
    abstraCt("This section is showing how to create some of the http://en.wikipedia.org/wiki/Gallery_of_named_graphs[named pretty graphs on Wikipedia].")
    section("Star Graph") {
      p("The graph is created by first creating a center node, and then once per element in the range, creates a leaf node and connects it to the center.")
      query( """CREATE (center)
               |FOREACH (x IN range(1,6)| CREATE (leaf),(center)-[:X]->(leaf))
               |RETURN id(center) AS id""", assertAStarIsBorn) {
        p("The query returns the id of the center node.")
        graphVizAfter()
      }
    }
  }.build()

  private def assertAStarIsBorn = ResultAssertions { p =>
    assertStats(p, nodesCreated = 7, relationshipsCreated = 6)
    p.toList should equal(List(Map("id" -> 0)))
  }
}
