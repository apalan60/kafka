package org.apache.kafka.jmh.util;

import org.apache.kafka.common.protocol.Errors;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(value = 3)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(2)
public class EnumMapVsHashMapBenchmark {
    private EnumMap<Errors, Integer> enumMap;
    private HashMap<Errors, Integer> hashMap;

    @Setup(Level.Iteration)
    public void setUp() {
        enumMap = new EnumMap<>(Errors.class);
        hashMap = new HashMap<>();

        enumMap.put(Errors.NONE, 1);
        enumMap.put(Errors.UNKNOWN_SERVER_ERROR, 2);

        hashMap.put(Errors.NONE, 1);
        hashMap.put(Errors.UNKNOWN_SERVER_ERROR, 2);
    }

    @Benchmark
    public int iterateEnumMap() {
        int sum = 0;
        for (Map.Entry<Errors, Integer> e : enumMap.entrySet()) {
            sum += e.getValue();
        }
        return sum;
    }

    @Benchmark
    public int iterateHashMap() {
        int sum = 0;
        for (Map.Entry<Errors, Integer> e : hashMap.entrySet()) {
            sum += e.getValue();
        }
        return sum;
    } 
}
