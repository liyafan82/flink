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

package org.apache.flink.benchmark;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockStreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.generated.CompilationOption;
import org.apache.flink.table.type.DoubleType;
import org.apache.flink.table.type.IntType;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.StringType;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for Java compilers.
 */
@State(Scope.Thread)
public class CompilationBenchmark {

	public static final int ROW_COUNT = 1024 * 1024;

	private InternalType[] inputTypes;

	private GenericRow[] rows;

	private PerfTestUtils perfUtil;

	private StreamRecord<GenericRow> record;

	private OneInputStreamOperator<GenericRow, GenericRow> jcaOperator;

	private OneInputStreamOperator<GenericRow, GenericRow> janinoOperator;

	@Setup
	public void prepare() throws Exception {
		inputTypes = new InternalType[] {
			DoubleType.INSTANCE,
			DoubleType.INSTANCE,
			DoubleType.INSTANCE,
			DoubleType.INSTANCE,
			StringType.INSTANCE,
			StringType.INSTANCE,
			IntType.INSTANCE
		};

		perfUtil = new PerfTestUtils();
		rows = new GenericRow[ROW_COUNT];
		for (int i = 0; i < ROW_COUNT; i++) {
			rows[i] = perfUtil.generateRow(inputTypes);
			perfUtil.fillRow(inputTypes, rows[i]);
		}

		record = new StreamRecord<>(null);

		final String className = "BatchExecCalcRule$30";
		jcaOperator = constructOperator(compileClass(className, CompilationOption.JCA));
		janinoOperator = constructOperator(compileClass(className, CompilationOption.JANINO));
	}

	private Class<?> compileClass(String className, CompilationOption option) throws IOException {
		long start = System.currentTimeMillis();
		Class<?> clazz = perfUtil.compileClass(className, option);
		long end = System.currentTimeMillis();
		System.out.println("Compilation time for " + option + ": " + (end - start) + "ms");

		return clazz;
	}

	private OneInputStreamOperator<GenericRow, GenericRow> constructOperator(Class<?> clazz) throws Exception {
		Object operator = clazz.getConstructor(Object[].class).newInstance(new Object[]{null});
		OneInputStreamOperator<GenericRow, GenericRow> oneInputOperator = (OneInputStreamOperator<GenericRow, GenericRow>) operator;
		AbstractStreamOperator<GenericRow> abstractOperator = (AbstractStreamOperator<GenericRow>) operator;

		StreamConfig cfg = new StreamConfig(new Configuration());
		cfg.setStateBackend(new MemoryStateBackend());

		cfg.setTimeCharacteristic(TimeCharacteristic.EventTime);
		cfg.setOperatorID(new OperatorID());

		MockStreamTask mockTask = new MockStreamTaskBuilder(MockEnvironment.builder().build())
			.setConfig(cfg)
			.setExecutionConfig(new ExecutionConfig())
			.build();
		abstractOperator.setup(mockTask, cfg, new PerfTestUtils.DumbOutput());

		return oneInputOperator;
	}

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	@OutputTimeUnit(TimeUnit.MICROSECONDS)
	public void testJCA() throws Exception {
		for (int j = 0; j < rows.length; j++) {
			jcaOperator.processElement(record.replace(rows[j]));
		}
	}

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	@OutputTimeUnit(TimeUnit.MICROSECONDS)
	public void testJanino() throws Exception {
		for (int j = 0; j < rows.length; j++) {
			janinoOperator.processElement(record.replace(rows[j]));
		}
	}

	public static void main(String[] args) throws RunnerException {
		Options opt = new OptionsBuilder()
			.include(CompilationBenchmark.class.getSimpleName())
			.forks(1)
			.build();
		new Runner(opt).run();
	}
}
