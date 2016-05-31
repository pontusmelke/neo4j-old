/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.codegen.ir

import org.neo4j.cypher.internal.compiler.v3_1.codegen.{CodeGenContext, MethodStructure, Variable}
import org.neo4j.cypher.internal.frontend.v3_1.SemanticDirection

case class VarExpandInstruction(opName: String, fromVar: Variable, dir: SemanticDirection,
                                types: Map[String, String], toVar: Variable, relVar: Variable,
                                relArrayVar: Variable, minSize: Int, maxSize: Int, action: Instruction) extends Instruction {

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {

    generator.trace(opName) { body =>
      body.varExpand(fromVar.name, types, relVar.name, relArrayVar.name, dir, toVar.name, minSize, maxSize) { inner =>
        inner.incrementRows()
        action.body(inner)
      }

      //    val iterVar = context.namer.newVarName()
      //    val nodeStackVar = context.namer.newVarName()
      //    val depthCounter = context.namer.newVarName()
      //
      //    generator.trace(opName) { body =>
      //      body.setUpVarExpand(fromVar.name, relArrayVar.name, nodeStackVar, continueFlag, depthCounter)
      //      body.whileLoop(generator.loadVariable(continueFlag)) { loopBody =>
      //        produceIterator(generator.popFromStack(nodeStackVar), iterVar, loopBody)
      //        loopBody.continueVarExpand(iterVar, continueFlag)
      //        loopBody.whileLoop(hasNext(loopBody, iterVar), loopBody.loadVariable(continueFlag)) { innerLoop =>
      //          innerLoop.nextRelationshipAndNode(toVar.name, iterVar, dir, fromVar.name, relVar.name)
      //          innerLoop.ifStatement(innerLoop.addRelVarExpand(relVar.name, relArrayVar.name )) { continuation =>
      //            action.body(continuation)
      //          }
      //          innerLoop.updateVarExpand(nodeStackVar, toVar.name,  relArrayVar.name, continueFlag, depthCounter, maxSize)
      //        }
      //      }
      //    }
      //    val iterator = s"${variable.name}Iter"
      //    generator.trace(producer.opName) { body =>
      //      producer.produceIterator(iterator, body)
      //      body.whileLoop(producer.hasNext(body, iterator)) { loopBody =>
      //        loopBody.incrementDbHits()
      //        loopBody.incrementRows()
      //        producer.produceNext(variable, iterator, loopBody)
      //        action.body(loopBody)
      //      }
      //    }
      //  }
    }
  }

  private def produceIterator[E](node: E, iterVar: String, generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    if(types.isEmpty)
      generator.nodeGetAllRelationships(iterVar, node, dir)
    else
      generator.nodeGetRelationships(iterVar, node, dir, types.keys.toSeq)
    generator.incrementDbHits()
  }

  private def hasNext[E](generator: MethodStructure[E], iterVar: String): E = generator.hasNextRelationship(iterVar)


  override def operatorId: Set[String] = Set(opName)

  override def children = Seq(action)

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    super.init(generator)
    generator.createRelExtractor(relVar.name)
    types.foreach {
      case (typeVar,relType) => generator.lookupRelationshipTypeId(typeVar, relType)
    }
  }
}
