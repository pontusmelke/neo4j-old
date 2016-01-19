package org.neo4j.cypher.internal.spi.v3_0

import org.neo4j.cypher.internal.compiler.v3_0.spi.TokenContext
import org.neo4j.cypher.{ConstraintValidationException, CypherExecutionException}
import org.neo4j.graphdb.{ConstraintViolationException => KernelConstraintViolationException}
import org.neo4j.kernel.api.TokenNameLookup
import org.neo4j.kernel.api.exceptions.KernelException

trait ExceptionTranslationSupport {
  inner: TokenContext =>

  protected def translateException[A](f: => A) = try {
    f
  } catch {
    case e: KernelException => throw new CypherExecutionException(e.getUserMessage(new TokenNameLookup {
      def propertyKeyGetName(propertyKeyId: Int): String = inner.getPropertyKeyName(propertyKeyId)

      def labelGetName(labelId: Int): String = inner.getLabelName(labelId)

      def relationshipTypeGetName(relTypeId: Int): String = inner.getRelTypeName(relTypeId)
    }), e)
    case e : KernelConstraintViolationException => throw new ConstraintValidationException(e.getMessage, e)
  }
}
