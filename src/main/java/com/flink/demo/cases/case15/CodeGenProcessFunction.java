package com.flink.demo.cases.case15;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by P0007 on 2019/9/24.
 */
public class CodeGenProcessFunction extends ProcessFunction<Row, Row> implements ResultTypeQueryable<Row> {

    private static final Logger logger = LoggerFactory.getLogger(CodeGenProcessFunction.class);

    private String name;

    private String code;

    private TypeInformation<Row> typeInfo;

    private ProcessFunction<Row, Row> function;

    private transient Counter counter;

    private transient Long processTime;

    public CodeGenProcessFunction(String name, String code, TypeInformation<Row> typeInfo) {
        this.name = name;
        this.code = code;
        this.typeInfo = typeInfo;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("Compileing ProcessFunction: {} \n\n Code:\n {}", name, code);
        Class<ProcessFunction<Row, Row>> clazz = compile(getRuntimeContext().getUserCodeClassLoader(), name, code);
        logger.info("Instantiating ProcessFunction.");
        function = clazz.newInstance();
        FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext());
        FunctionUtils.openFunction(function, parameters);

        this.counter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("custom_group")
                .counter("total read");

        getRuntimeContext()
                .getMetricGroup()
                .addGroup("custom_group")
                .gauge("processTime", (Gauge<Long>) () -> processTime);
    }

    @Override
    public void processElement(Row value, Context ctx, Collector<Row> out) throws Exception {
        long startTime = System.nanoTime();
        function.processElement(value, ctx, out);
        long endTime = System.nanoTime();
        this.processTime = endTime - startTime;
        this.counter.inc();
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return typeInfo;
    }

    @Override
    public void close() throws Exception {
        FunctionUtils.closeFunction(function);
    }

    private Class compile(ClassLoader classLoader, String name, String code) {
        Class<?> aClass;
        SimpleCompiler compiler = new SimpleCompiler();
        compiler.setParentClassLoader(classLoader);
        try {
            compiler.cook(code);
            aClass = compiler.getClassLoader().loadClass(name);
        } catch (CompileException e) {
            throw new InvalidProgramException("Table program cannot be compiled. " +
                    "This is a bug. Please file an issue.", e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class " + name + " Not Found", e);
        }
        return aClass;
    }
}
