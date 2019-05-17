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

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.generated.CompilationOption;
import org.apache.flink.table.generated.CompileUtils;
import org.apache.flink.table.type.DoubleType;
import org.apache.flink.table.type.IntType;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.StringType;
import org.apache.flink.table.util.JCACompilationUtil;
import org.apache.flink.util.OutputTag;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * Utilities for performance tests.
 */
public class PerfTestUtils {

	/**
	 * The minimum length of a random string (inclusive).
	 */
	public static final int VAR_LENGTH_DATA_MIN_SIZE = 5;

	/**
	 * The maximum length of a random string (exclusive).
	 */
	public static final int VAR_LENGTH_DATA_MAX_SIZE = 20;

	private Random random = new Random(0);

	/**
	 * Create an empty row.
	 * @param schema the schema of the row.
	 * @return the created row.
	 */
	public GenericRow generateRow(InternalType[] schema) {
		return new GenericRow(schema.length);
	}

	/**
	 * Fill a row with random data.
	 * @param schema the schema of the row.
	 * @param row the row to be filled.
	 */
	public void fillRow(InternalType[] schema, GenericRow row) {
		for (int i = 0; i < schema.length; i++) {
			if (schema[i] instanceof DoubleType) {
				row.setDouble(i, random.nextDouble());
			} else if (schema[i] instanceof IntType) {
				row.setInt(i, random.nextInt());
			} else if (schema[i] instanceof StringType) {
				byte[] bytes = new byte[getBytesLength()];
				random.nextBytes(bytes);
				row.setField(i, new BinaryString(new String(bytes)));
			} else {
				throw new IllegalArgumentException("Type " + schema[i].getClass() + " not supported yet");
			}
		}
	}

	/**
	 * Generate the length of a random byte array.
	 * @return the length for the byte array.
	 */
	private int getBytesLength() {
		int r = random.nextInt() & Integer.MAX_VALUE;
		return r % (VAR_LENGTH_DATA_MAX_SIZE - VAR_LENGTH_DATA_MIN_SIZE) + VAR_LENGTH_DATA_MIN_SIZE;
	}

	/**
	 * Load class source code, given its name.
	 * @param className name of the class.
	 * @return the class code.
	 */
	public String loadSourceFile(String className) throws IOException {
		String codeDir = getClass().getClassLoader().getResource("codegen").getFile();
		File sourceFile = new File(codeDir, className + ".code");
		return FileUtils.readFileToString(sourceFile);
	}

	/**
	 * Compile the source code and get the class object.
	 * @param name the class name.
	 * @param option the compilation option.
	 * @return the compiled class object.
	 */
	public Class<?> compileClass(String name, CompilationOption option) throws IOException {
		String code = loadSourceFile(name);
		switch (option) {
			case JCA:
				return JCACompilationUtil.getInstance().getCodeClass(name, code);
			case JANINO:
				return CompileUtils.compile(this.getClass().getClassLoader(), name, code);
			default:
				throw new IllegalArgumentException("Invalid compilation option: " + option);
		}
	}

	/**
	 * An output that does nothing.
	 */
	public static class DumbOutput implements Output<StreamRecord<GenericRow>> {

		@Override
		public void emitWatermark(Watermark mark) {

		}

		@Override
		public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {

		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {

		}

		@Override
		public void collect(StreamRecord<GenericRow> record) {

		}

		@Override
		public void close() {

		}
	}
}
