/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.slotted.SlottedPipeBuilder.translateColumnOrder
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{PipelineInformation, RefSlot}
import org.neo4j.cypher.internal.compiler.v3_4.planner.CantCompileQueryException
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4.IdName
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyTypes
import org.neo4j.cypher.internal.runtime.vectorized.operators._
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.v3_4.logical.plans
import org.neo4j.cypher.internal.v3_4.logical.plans._

class PipelineBuilder(pipelines: Map[LogicalPlanId, PipelineInformation], converters: ExpressionConverters)
  extends TreeBuilder[Pipeline] {

  override def create(plan: LogicalPlan): Pipeline = {
    val pipeline: Pipeline = super.create(plan)
    pipeline.construct
  }

  override protected def build(plan: LogicalPlan): Pipeline = {
    val pipeline = pipelines(plan.assignedId)

    val thisOp = plan match {
      case plans.AllNodesScan(IdName(column), _) =>
        new AllNodeScanOperator(
          pipeline.numberOfLongs,
          pipeline.numberOfReferences,
          pipeline.getLongOffsetFor(column))

      case plans.SingleRow(_) =>
        new SingleRowOperator
    }

    Pipeline(thisOp, Seq.empty, pipeline, NoDependencies)()
  }

  override protected def build(plan: LogicalPlan, from: Pipeline): Pipeline = {
    var source = from
    val pipeline = pipelines(plan.assignedId)

      val thisOp = plan match {
        case plans.ProduceResult(_, columns) =>
          new ProduceResultOperator(pipeline, columns.toArray)

        case plans.Selection(predicates, _) =>
          val predicate = predicates.map(converters.toCommandPredicate).reduce(_ andWith _)
          new FilterOperator(pipeline, predicate)

        case plans.Expand(lhs, IdName(fromName), dir, types, IdName(to), IdName(relName), ExpandAll) =>
          val fromOffset = pipeline.getLongOffsetFor(fromName)
          val relOffset = pipeline.getLongOffsetFor(relName)
          val toOffset = pipeline.getLongOffsetFor(to)
          val fromPipe = pipelines(lhs.assignedId)
          val lazyTypes = LazyTypes(types.toArray)(SemanticTable())
          new ExpandAllOperator(pipeline, fromPipe, fromOffset, relOffset, toOffset, dir, lazyTypes)

        case plans.Projection(_, expressions) =>
          val projectionOps = expressions.map {
            case (key, e) => pipeline(key) -> converters.toCommandExpression(e)
          }
          new ProjectOperator(projectionOps, pipeline)

        case plans.Sort(_, sortItems) =>
          val ordering = sortItems.map(translateColumnOrder(pipeline, _))
          val preSorting = new PreSortOperator(ordering, pipeline)
          source = source.addOperator(preSorting)
          new MergeSortOperator(ordering, pipeline)

        case plans.UnwindCollection(src, variable, collection) =>
          val offset = pipeline.get(variable.name) match {
            case Some(RefSlot(idx, _, _)) => idx
            case _ =>
              throw new InternalException("Weird slot found for UNWIND")
          }
          val runtimeExpression = converters.toCommandExpression(collection)
          new UnwindOperator(runtimeExpression, offset, pipelines(src.assignedId), pipeline)

        case p => throw new CantCompileQueryException(s"$p not supported in morsel runtime")
      }

    thisOp match {
      case o: Operator =>
        Pipeline(o, Seq.empty, pipeline, o.addDependency(source))()
      case mo: MiddleOperator =>
        source.addOperator(mo)
    }
  }

  override protected def build(plan: LogicalPlan, lhs: Pipeline, rhs: Pipeline): Pipeline =
    throw new CantCompileQueryException(s"$plan is not supported in morsel runtime")
}

object IsPipelineBreaker {
  def apply(plan: LogicalPlan): Boolean = {
    plan match {
      case _ => true
    }
  }
}