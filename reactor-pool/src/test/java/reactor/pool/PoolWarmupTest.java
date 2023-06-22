/*
 * Copyright (c) 2019-2023 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.pool;

import org.assertj.core.data.Offset;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.pool.TestUtils.ParameterizedTestWithName;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test attempts to reproduce a problem reported in
 * https://github.com/reactor/reactor-netty/issues/2781.
 *
 * <p>
 * More specifically, this test is reproducing the sample scenario described here:
 * https://github.com/r2dbc/r2dbc-pool/issues/190#issuecomment-1520166774":
 * during warmup, some or all DBConnections may use the same TcpResource EventLoop thread
 * (if minResources == maxResources).
 *
 * <p>
 * In a nutshell, the tested scenario is the following:
 *
 * <ul>
 * <li>we have a DBConnectionPool that is internally using an InstrumentedPool for reactive DBConnection pooling.</li>
 * <li>A DBConnection, once acquired, allows to simulate the send of an SQL request (findAll)</li>
 * <li>like in reactor-netty and r2dbc, when a DBConnection is created, it will either use
 *   a dedicate thread that will be used to send SQL requests on the DBConnection, unless the current thread is already
 *   a DBConnection thread. In this case, the current DBConnection thread will be used: this is similar to
 *   "colocated" TcpRespource EventLoops in reactor-netty.</li>
 * </ul>
 *
 * @author Pierre De Rop
 */
public class PoolWarmupTest {
	/**
	 * The test will be run twice: with InstrumentedPool warmups, and without it.
	 * If warmup is not done, then the warmup will be internally done
	 * lazily when the first acquire is taking place because the allocation strategy is
	 * configured with minResources == maxResources.
	 */
	static List<Boolean> warmupOptions() { return Arrays.asList(Boolean.FALSE, Boolean.TRUE); }

	static final Logger LOGGER = Loggers.getLogger(PoolWarmupTest.class);

	/**
	 * Each DBConnection will use one of the following DBConnection Executor
	 */
	final static class DBConnectionThread implements Executor {
		final static ThreadLocal<DBConnectionThread> current = ThreadLocal.withInitial(() -> null);

		final ExecutorService dbThread;

		DBConnectionThread(String name) {
			dbThread = Executors.newSingleThreadExecutor(r -> new Thread(() -> {
				current.set(DBConnectionThread.this);
				r.run();
			}, name));
		}

		void stop() {
			dbThread.shutdown();
			try {
				if (! dbThread.awaitTermination(30, TimeUnit.SECONDS)) {
					throw new IllegalStateException("Could not stop db thread timely.");
				}
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void execute(Runnable command) {
			dbThread.execute(command);
		}
	}

	/**
	 * A DBConnection simulates an SQL "findAll" request, which is executed
	 * through a DBConnectionThread executor.
	 */
	final static class DBConnection {
		final DBConnectionThread dbThread;

		public DBConnection(DBConnectionThread dbThread) {
			this.dbThread = dbThread;
		}

		Flux<String> findAll() {
			return Flux.range(0, 1000)
					.map(integer -> "table entry -" + integer)
					.delayElements(Duration.ofMillis(1L))
					.publishOn(Schedulers.fromExecutor(dbThread));
		}
	}

	/**
	 * A DBConnection Pool, based on Reactor-Pool "InstrumentedPool", configured with minResources == maxResources
	 */
	final static class DBConnectionPool {
		final int poolSize;
		final DBConnectionThread[] dbThreads;
		final InstrumentedPool<DBConnection> pool;
		final static AtomicInteger roundRobin = new AtomicInteger();

		DBConnectionPool(int poolSize) {
			this.poolSize = poolSize;
			this.dbThreads = new DBConnectionThread[poolSize];
			IntStream.range(0, poolSize).forEach(i -> dbThreads[i] = new DBConnectionThread("dbthread-" + i));

			pool = PoolBuilder
					.from(Mono.defer(() -> {
						// if the current thread is already one of our DB thread, then DBConnection.findAll will use
						// this current thread, else, let's select one in a round-robin way.
						DBConnectionThread dbThread = DBConnectionThread.current.get();
						dbThread = dbThread == null ?
								dbThreads[(roundRobin.incrementAndGet() & 0x7F_FF_FF_FF) % dbThreads.length] : dbThread;
						return Mono.just(new DBConnection(dbThread))
								.delayElement(Duration.ofMillis(10)) // simulate Database handshaking (authentication, etc ...)
								.publishOn(Schedulers.fromExecutor(dbThread));
					}))
					.sizeBetween(10, 10)
					.idleResourceReuseOrder(false)
					.buildPool();
		}

		InstrumentedPool<DBConnection> getPool() {
			return pool;
		}

		void stop() {
			pool.disposeLater().block(Duration.ofSeconds(30));
			Stream.of(dbThreads).forEach(DBConnectionThread::stop);
		}
	}

	@ParameterizedTestWithName
	@MethodSource("warmupOptions")
	void warmupTest(boolean doWarmup) {
		int poolSize = 10;
		DBConnectionPool dbConnectionPool = new DBConnectionPool(poolSize);

		try {
			InstrumentedPool<DBConnection> pool = dbConnectionPool.getPool();
			if (doWarmup) {
				pool.warmup().block();
			}

			long startTime = System.currentTimeMillis();

			List<Flux<String>> fluxes = IntStream.rangeClosed(1, poolSize)
					.mapToObj(i -> Flux.from(pool.withPoolable(DBConnection::findAll)
							.doOnComplete(() -> LOGGER.info(": " + i + "-findAll done"))))
					.collect(Collectors.toList());

			List<Mono<Long>> next = new ArrayList<>();
			for (Flux<String> flux : fluxes) {
				next.add(flux.count().doOnNext(number -> LOGGER.info("num:" + number)));
			}

			Flux.fromIterable(next)
					.flatMap(x -> x)
					.collectList()
					.block(Duration.ofSeconds(60));

			long elapsed = (System.currentTimeMillis() - startTime);
			LOGGER.info("Elapsed time: " + elapsed);

			// each "dbConnection.findAll()" should take around 1000 millis, we have
			// 10 subscriptions, but we expect subscriptions to be served concurrently using
			// our 10 DBConnections threads from the pool ... So the elapsed time should be around 1000 millis, not 10 000 millis !
			assertThat(elapsed).isCloseTo(1000, Offset.offset(3000L));
		}

		finally {
			dbConnectionPool.stop();
		}
	}
}

