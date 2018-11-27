package reactor.util.pool.impl;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Simon Baslé
 */
class QueuePoolTest {

    private static Logger LOG = Loggers.getLogger(QueuePoolTest.class);

    public static final class PoolableTest {

        private int usedUp;
        private final int id;

        public PoolableTest(int id) {
            this.id = id;
            this.usedUp = 0;
        }

        public void clean() {
            this.usedUp++;
        }

        public boolean isHealthy() {
            return usedUp < 2;
        }

        @Override
        public String toString() {
            return "PoolableTest{id=" + id + ", used=" + usedUp + "}";
        }
    }

    private static final class PoolableTestConfig extends DefaultPoolConfig<PoolableTest> {

        private PoolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator) {
            super(minSize, maxSize,
                    allocator,
                    PoolableTest::clean,
                    PoolableTest::isHealthy);
        }

        private PoolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator,
                                   Consumer<? super PoolableTest> additionalCleaner) {
            super(minSize, maxSize,
                    allocator,
                    poolableTest -> {
                        poolableTest.clean();
                        additionalCleaner.accept(poolableTest);
                    },
                    PoolableTest::isHealthy);
        }
    }

    @Test
    void smokeTest() throws InterruptedException {
        AtomicInteger newCount = new AtomicInteger();
        QueuePool<PoolableTest> pool = new QueuePool<>(new PoolableTestConfig(2, 3,
                Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet())))));

        List<PoolableTest> borrowed1 = new ArrayList<>();
        pool.borrow().subscribe(borrowed1::add);
        pool.borrow().subscribe(borrowed1::add);
        pool.borrow().subscribe(borrowed1::add);
        List<PoolableTest> borrowed2 = new ArrayList<>();
        pool.borrow().subscribe(borrowed2::add);
        pool.borrow().subscribe(borrowed2::add);
        pool.borrow().subscribe(borrowed2::add);
        List<PoolableTest> borrowed3 = new ArrayList<>();
        pool.borrow().subscribe(borrowed3::add);
        pool.borrow().subscribe(borrowed3::add);
        pool.borrow().subscribe(borrowed3::add);

        assertThat(borrowed1).hasSize(3);
        assertThat(borrowed2).isEmpty();
        assertThat(borrowed3).isEmpty();

        Thread.sleep(1000);
        for (PoolableTest p : borrowed1) {
            pool.release(p);
        }
        assertThat(borrowed2).hasSize(3);
        assertThat(borrowed3).isEmpty();

        Thread.sleep(1000);
        for (PoolableTest p : borrowed2) {
            pool.release(p);
        }
        assertThat(borrowed3).hasSize(3);

        assertThat(borrowed1)
                .as("borrowed1/2 all used up")
                .hasSameElementsAs(borrowed2)
                .allSatisfy(p -> assertThat(p.usedUp).isEqualTo(2));

        assertThat(borrowed3)
                .as("borrowed3 all new")
                .allSatisfy(p -> assertThat(p.usedUp).isZero());
    }

    @Test
    void smokeTestAsync() throws InterruptedException {
        AtomicInteger newCount = new AtomicInteger();
        QueuePool<PoolableTest> pool = new QueuePool<>(new PoolableTestConfig(2, 3,
                Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet())))
                        .subscribeOn(Schedulers.newParallel("poolable test allocator"))));

        List<PoolableTest> borrowed1 = new ArrayList<>();
        CountDownLatch latch1 = new CountDownLatch(3);
        pool.borrow().subscribe(borrowed1::add, Throwable::printStackTrace, latch1::countDown);
        pool.borrow().subscribe(borrowed1::add, Throwable::printStackTrace, latch1::countDown);
        pool.borrow().subscribe(borrowed1::add, Throwable::printStackTrace, latch1::countDown);

        List<PoolableTest> borrowed2 = new ArrayList<>();
        pool.borrow().subscribe(borrowed2::add);
        pool.borrow().subscribe(borrowed2::add);
        pool.borrow().subscribe(borrowed2::add);

        List<PoolableTest> borrowed3 = new ArrayList<>();
        CountDownLatch latch3 = new CountDownLatch(3);
        pool.borrow().subscribe(borrowed3::add, Throwable::printStackTrace, latch3::countDown);
        pool.borrow().subscribe(borrowed3::add, Throwable::printStackTrace, latch3::countDown);
        pool.borrow().subscribe(borrowed3::add, Throwable::printStackTrace, latch3::countDown);

        if (!latch1.await(1, TimeUnit.SECONDS)) { //wait for creation of max elements
            fail("not enough elements created initially, missing " + latch1.getCount());
        }
        assertThat(borrowed1).hasSize(3);
        assertThat(borrowed2).isEmpty();
        assertThat(borrowed3).isEmpty();

        Thread.sleep(1000);
        for (PoolableTest p : borrowed1) {
            pool.release(p);
        }
        assertThat(borrowed2).hasSize(3);
        assertThat(borrowed3).isEmpty();

        Thread.sleep(1000);
        for (PoolableTest p : borrowed2) {
            pool.release(p);
        }

        if (latch3.await(2, TimeUnit.SECONDS)) { //wait for the re-creation of max elements

            assertThat(borrowed3).hasSize(3);

            assertThat(borrowed1)
                    .as("borrowed1/2 all used up")
                    .hasSameElementsAs(borrowed2)
                    .allSatisfy(p -> assertThat(p.usedUp).isEqualTo(2));

            assertThat(borrowed3)
                    .as("borrowed3 all new")
                    .allSatisfy(p -> assertThat(p.usedUp).isZero());
        }
        else {
            fail("not enough new elements generated, missing " + latch3.getCount());
        }
    }

    @Test
    void returnedReleasedIfBorrowerCancelled() {
        AtomicInteger newCount = new AtomicInteger();
        AtomicInteger releasedCount = new AtomicInteger();

        PoolableTestConfig testConfig = new PoolableTestConfig(1, 1,
                Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet())))
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                pt -> releasedCount.incrementAndGet());
        QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

        //borrow the only element
        PoolableTest element = pool.borrow().block();

        pool.borrow().subscribe().dispose();

        assertThat(releasedCount).as("before returning").hasValue(0);

        //release the element, which should forward to the cancelled second borrow, itself also cleaning
        pool.release(element);

        assertThat(releasedCount).as("after returning").hasValue(2);
    }

    @Test
    void allocatedReleasedIfBorrowerCancelled() {
        Scheduler scheduler = Schedulers.newParallel("poolable test allocator");
        AtomicInteger newCount = new AtomicInteger();
        AtomicInteger releasedCount = new AtomicInteger();

        PoolableTestConfig testConfig = new PoolableTestConfig(0, 1,
                Mono.defer(() -> Mono.just(new PoolableTest(newCount.incrementAndGet())))
                        .subscribeOn(scheduler),
                pt -> releasedCount.incrementAndGet());
        QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

        //borrow the only element and immediately dispose
        pool.borrow().subscribe().dispose();

        assertThat(newCount).as("created").hasValue(1);
        assertThat(releasedCount).as("released").hasValue(1);
    }

}