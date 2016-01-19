package org.neo4j.cypher.internal.spi.v3_0

import org.neo4j.cypher.internal.compiler.v3_0.pipes.EntityProducer
import org.neo4j.cypher.internal.compiler.v3_0.pipes.matching.{TraversalMatcher, ExpanderStep}
import org.neo4j.cypher.internal.compiler.v3_0.spi.{GraphStatistics, ProcedureSignature, ProcedureName, PlanContext}
import org.neo4j.graphdb.Node
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.neo4j.kernel.api.index.IndexDescriptor

class ExceptionTranslatingPlanContext(inner: PlanContext) extends PlanContext with ExceptionTranslationSupport {

  override def getIndexRule(labelName: String, propertyKey: String): Option[IndexDescriptor] =
    translateException(inner.getIndexRule(labelName, propertyKey))

  override def getUniqueIndexRule(labelName: String, propertyKey: String): Option[IndexDescriptor] =
    translateException(inner.getUniqueIndexRule(labelName, propertyKey))

  override def statistics: GraphStatistics =
    translateException(inner.statistics)

  override def checkNodeIndex(idxName: String): Unit =
    translateException(inner.checkNodeIndex(idxName))

  override def bidirectionalTraversalMatcher(steps: ExpanderStep, start: EntityProducer[Node], end: EntityProducer[Node]): TraversalMatcher =
    translateException(inner.bidirectionalTraversalMatcher(steps, start, end))

  override def txIdProvider: () => Long = {
    val innerTxProvider = translateException(inner.txIdProvider)
    () => translateException(innerTxProvider())
  }

  override def procedureSignature(name: ProcedureName): ProcedureSignature =
    translateException(inner.procedureSignature(name))

  override def hasIndexRule(labelName: String): Boolean =
    translateException(inner.hasIndexRule(labelName))

  // Legacy traversal matchers (pre-Ronja) (These were moved out to remove the dependency on the kernel)
  override def monoDirectionalTraversalMatcher(steps: ExpanderStep, start: EntityProducer[Node]): TraversalMatcher =
    translateException(inner.monoDirectionalTraversalMatcher(steps, start))

  override def getUniquenessConstraint(labelName: String, propertyKey: String): Option[UniquenessConstraint] =
    translateException(inner.getUniquenessConstraint(labelName, propertyKey))

  override def checkRelIndex(idxName: String): Unit =
    translateException(inner.checkRelIndex(idxName))

  override def getOrCreateFromSchemaState[T](key: Any, f: => T): T =
    translateException(inner.getOrCreateFromSchemaState(key, f))

  override def getOptRelTypeId(relType: String): Option[Int] =
    translateException(inner.getOptRelTypeId(relType))

  override def getRelTypeName(id: Int): String =
    translateException(inner.getRelTypeName(id))

  override def getRelTypeId(relType: String): Int =
    translateException(inner.getRelTypeId(relType))

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] =
    translateException(inner.getOptPropertyKeyId(propertyKeyName))

  override def getLabelName(id: Int): String =
    translateException(inner.getLabelName(id))

  override def getOptLabelId(labelName: String): Option[Int] =
    translateException(inner.getOptLabelId(labelName))

  override def getPropertyKeyId(propertyKeyName: String): Int =
    translateException(inner.getPropertyKeyId(propertyKeyName))

  override def getPropertyKeyName(id: Int): String =
    translateException(inner.getPropertyKeyName(id))

  override def getLabelId(labelName: String): Int =
    translateException(inner.getLabelId(labelName))
}
