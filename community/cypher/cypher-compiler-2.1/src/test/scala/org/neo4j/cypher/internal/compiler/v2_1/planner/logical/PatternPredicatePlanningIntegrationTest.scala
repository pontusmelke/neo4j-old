/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v2_1.ast.True
import org.neo4j.cypher.internal.compiler.v2_1.ast.Property
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.SingleRow
import org.neo4j.cypher.internal.compiler.v2_1.ast.RelTypeName
import org.neo4j.cypher.internal.compiler.v2_1.ast.GreaterThan
import org.neo4j.cypher.internal.compiler.v2_1.planner.Predicate
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.ast.PropertyKeyName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Expand
import org.neo4j.cypher.internal.compiler.v2_1.ast.SignedIntegerLiteral
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.SemiApply
import org.neo4j.cypher.internal.compiler.v2_1.planner.Exists

class PatternPredicatePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should build plans containing semi apply for a single pattern predicate") {
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan => 2000000
      case _: Expand => 10
      case _: SingleRow => 1
      case _ => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)
    implicit val planContext = newMockedPlanContext

    when(planContext.getOptRelTypeId("X")).thenReturn(None)

    produceLogicalPlan("MATCH (a) WHERE (a)-[:X]->() RETURN a") should equal(
      SemiApply(
        AllNodesScan("a"),
        Expand(
          SingleRow(Set("a")),
          "a", Direction.OUTGOING, Seq(RelTypeName("X")()_), "  UNNAMED27", "  UNNAMED19", SimplePatternLength
        )( mockRel )
      )( Exists( Predicate( Set.empty, True()_ ), QueryGraph.empty ) )
    )
  }

  test("should build plans containing semi apply for two pattern predicates") {
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan => 2000000
      case _: Expand => 10
      case _: SingleRow => 1
      case _ => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)
    implicit val planContext = newMockedPlanContext

    when(planContext.getOptRelTypeId(any())).thenReturn(None)

    produceLogicalPlan("MATCH (a) WHERE (a)-[:X]->() AND (a)-[:Y]->() RETURN a") should equal(
      SemiApply(
        SemiApply(
          AllNodesScan("a"),
          Expand(
            SingleRow(Set("a")),
            "a", Direction.OUTGOING, Seq(RelTypeName("X")()_), "  UNNAMED27", "  UNNAMED19", SimplePatternLength
          )( mockRel )
        )( Exists( Predicate( Set.empty, True()_ ), QueryGraph.empty ) ),
        Expand(
          SingleRow(Set("a")),
          "a", Direction.OUTGOING, Seq(RelTypeName("Y")()_), "  UNNAMED44", "  UNNAMED36", SimplePatternLength
        )( mockRel )
      )( Exists( Predicate( Set.empty, True()_ ), QueryGraph.empty ) )
    )
  }

  test("should build plans containing select or semi apply for a pattern predicate and an expression") {
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan => 2000000
      case _: Expand => 10
      case _: SingleRow => 1
      case _ => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)
    implicit val planContext = newMockedPlanContext

    when(planContext.getOptPropertyKeyId(any())).thenReturn(None)
    when(planContext.getOptRelTypeId(any())).thenReturn(None)

    produceLogicalPlan("MATCH (a) WHERE (a)-[:X]->() OR a.prop > 4 RETURN a") should equal(
      SelectOrSemiApply(
        AllNodesScan("a"),
        Expand(
          SingleRow(Set("a")),
          "a", Direction.OUTGOING, Seq(RelTypeName("X")()_), "  UNNAMED27", "  UNNAMED19", SimplePatternLength
        )( mockRel ),
        GreaterThan(Property(Identifier("a")_, PropertyKeyName("prop")()_)_, SignedIntegerLiteral("4")_)_
      )( HoldsOrExists( Predicate( Set.empty, True()_ ), True()_, QueryGraph.empty ) )
    )
  }

  test("should build plans containing select or semi apply for a pattern predicate and multiple expressions") {
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan => 2000000
      case _: Expand => 10
      case _: SingleRow => 1
      case _ => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)
    implicit val planContext = newMockedPlanContext

    when(planContext.getOptPropertyKeyId(any())).thenReturn(None)
    when(planContext.getOptRelTypeId(any())).thenReturn(None)

    produceLogicalPlan("MATCH (a) WHERE a.prop2 = 9 OR (a)-[:X]->() OR a.prop > 4 RETURN a") should equal(
      SelectOrSemiApply(
        AllNodesScan("a"),
        Expand(
          SingleRow(Set("a")),
          "a", Direction.OUTGOING, Seq(RelTypeName("X")()_), "  UNNAMED42", "  UNNAMED34", SimplePatternLength
        )( mockRel ),
        Or(
          Equals(Property(Identifier("a")_, PropertyKeyName("prop2")()_)_, SignedIntegerLiteral("9")_)_,
          GreaterThan(Property(Identifier("a")_, PropertyKeyName("prop")()_)_, SignedIntegerLiteral("4")_)_
        )_
      )( HoldsOrExists( Predicate( Set.empty, True()_ ), True()_, QueryGraph.empty ) )
    )
  }

}
