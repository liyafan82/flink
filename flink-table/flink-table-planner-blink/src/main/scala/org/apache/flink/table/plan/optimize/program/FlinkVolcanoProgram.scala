/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.optimize.program

import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.flink.util.Preconditions

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException
import org.apache.calcite.plan.RelTrait
import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.{Programs, RuleSet}

/**
  * A FlinkRuleSetProgram that runs with [[org.apache.calcite.plan.volcano.VolcanoPlanner]].
  *
  * @tparam OC OptimizeContext
  */
class FlinkVolcanoProgram[OC <: FlinkOptimizeContext] extends FlinkRuleSetProgram[OC] {

  /**
    * Required output traits, this must not be None when doing optimize.
    */
  protected var requiredOutputTraits: Option[Array[RelTrait]] = None

  override def optimize(root: RelNode, context: OC): RelNode = {
    if (rules.isEmpty) {
      return root
    }

    if (requiredOutputTraits.isEmpty) {
      throw new TableException("Required output traits should not be None in FlinkVolcanoProgram")
    }

    val targetTraits = root.getTraitSet.plusAll(requiredOutputTraits.get).simplify()
    // reuse VolcanoPlanner instance defined in context
    val planner = Preconditions.checkNotNull(context.getVolcanoPlanner)
    val optProgram = Programs.ofRules(rules)

    try {
      optProgram.run(
        planner,
        root,
        targetTraits,
        ImmutableList.of(),
        ImmutableList.of())
    } catch {
      case e: CannotPlanException =>
        throw new TableException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${FlinkRelOptUtil.toString(root)}\n" +
            s"This exception indicates that the query uses an unsupported SQL feature.\n" +
            s"Please check the documentation for the set of currently supported SQL features.", e)
      case t: TableException =>
        throw new TableException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${FlinkRelOptUtil.toString(root)}\n" +
            s"${t.getMessage}\n" +
            s"Please check the documentation for the set of currently supported SQL features.", t)
      case a: AssertionError =>
        throw new AssertionError(s"Sql optimization: Assertion error: ${a.getMessage}", a)
      case r: RuntimeException if r.getCause.isInstanceOf[TableException] =>
        throw new TableException(
          s"Sql optimization: Cannot generate a valid execution plan for the given query: \n\n" +
            s"${FlinkRelOptUtil.toString(root)}\n" +
            s"${r.getCause.getMessage}\n" +
            s"Please check the documentation for the set of currently supported SQL features.",
          r.getCause)
    }
  }

  /**
    * Sets required output traits.
    */
  def setRequiredOutputTraits(relTraits: Array[RelTrait]): Unit = {
    Preconditions.checkNotNull(relTraits)
    requiredOutputTraits = Some(relTraits)
  }

}

class FlinkVolcanoProgramBuilder[OC <: FlinkOptimizeContext] {
  private val volcanoProgram = new FlinkVolcanoProgram[OC]

  /**
    * Adds rules for this program.
    */
  def add(ruleSet: RuleSet): FlinkVolcanoProgramBuilder[OC] = {
    volcanoProgram.add(ruleSet)
    this
  }

  /**
    * Sets required output traits.
    */
  def setRequiredOutputTraits(relTraits: Array[RelTrait]): FlinkVolcanoProgramBuilder[OC] = {
    volcanoProgram.setRequiredOutputTraits(relTraits)
    this
  }

  def build(): FlinkVolcanoProgram[OC] = volcanoProgram

}

object FlinkVolcanoProgramBuilder {
  def newBuilder[OC <: FlinkOptimizeContext] = new FlinkVolcanoProgramBuilder[OC]
}
