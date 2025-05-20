package org.apache.kafka.jmh.util;

import org.apache.kafka.common.protocol.Errors;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(value = 5)
@Threads(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class EnumMapVsHashMapBenchmark {
    @Param({"2", "8", "16", "32", "64", "128"})
    private int entries;
    private EnumMap<Errors, Integer> enumMap;
    private HashMap<Errors, Integer> hashMap;
    private final Errors[] allErrors = Errors.values();

    @Setup(Level.Trial)
    public void setUp() {
        enumMap = new EnumMap<>(Errors.class);
        hashMap = new HashMap<>(entries * 2, 0.75f);

        for (int i = 0; i < entries; i++) {
            Errors err = allErrors[i % allErrors.length];
            enumMap.put(err, i);
            hashMap.put(err, i);
        };
    }

    /* =============== iterate workload =============== */

    @Benchmark
    public void iterateEnumMap(Blackhole bh) {
        int sum = 0;
        for (Map.Entry<Errors, Integer> e : enumMap.entrySet())
            sum += e.getValue();
        bh.consume(sum);
    }

    @Benchmark
    public void iterateHashMap(Blackhole bh) {
        int sum = 0;
        for (Map.Entry<Errors, Integer> e : hashMap.entrySet())
            sum += e.getValue();
        bh.consume(sum);
    }

    /* =============== put workload =============== */

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void putEnumMap(Blackhole bh) {
        EnumMap<Errors, Integer> map = new EnumMap<>(Errors.class);
        for (int i = 0; i < entries; i++) {
            Errors err = allErrors[i % allErrors.length];
            map.put(err, i);
        }
        bh.consume(map);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void putHashMap(Blackhole bh) {
        HashMap<Errors, Integer> map = new HashMap<>(entries * 2, 0.75f);
        for (int i = 0; i < entries; i++) {
            Errors err = allErrors[i % allErrors.length];
            map.put(err, i);
        }
        bh.consume(map);
    }
}
