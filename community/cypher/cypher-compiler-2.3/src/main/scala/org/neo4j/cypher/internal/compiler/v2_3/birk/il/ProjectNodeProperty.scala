/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.birk.il

import org.neo4j.cypher.internal.compiler.v2_3.birk.CodeGenerator

case class ProjectNodeProperty(propValueVar: String, propName: String, propKeyVar: String, nodeIdVar: String) extends Instruction {

  def generateCode() = s"Object $propValueVar = ro.nodeGetProperty( $nodeIdVar, $propKeyVar ).value();"

  //TODO only generate this if property token not known at compile time
  def generateInit() =
      s"""if ( $propKeyVar == -1 )
         |{
         |$propKeyVar = ro.propertyKeyGetForName( "$propName" );
         |}
       """.stripMargin

  override def _importedClasses() =
    Set("org.neo4j.kernel.api.properties.Property")

  override def fields() = s"private int $propKeyVar = -1;"
}

case class ProjectNodeProperties(projections:Seq[ProjectNodeProperty], parent:Instruction) extends Instruction {
  override def generateCode()= generate(_.generateCode())

  override protected def children: Seq[Instruction] = projections :+ parent

  override def fields() = generate(_.fields())

  override def generateInit() = generate(_.generateInit())

  private def generate(f : Instruction => String) = projections.map(f(_)).mkString("", CodeGenerator.n, CodeGenerator.n) + f(parent)
}
