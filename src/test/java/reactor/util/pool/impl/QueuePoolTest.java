package reactor.util.pool.impl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Simon Baslé
 */
class QueuePoolTest {

    private static Logger LOG = Loggers.getLogger(QueuePoolTest.class);

    public static final class PoolableTest implements Disposable {

        private static AtomicInteger defaultId = new AtomicInteger();

        private int usedUp;
        private int discarded;
        private final int id;

        public PoolableTest() {
            this(defaultId.incrementAndGet());
        }

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
        public void dispose() {
            discarded++;
        }

        @Override
        public boolean isDisposed() {
            return discarded > 0;
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
                    pt -> Mono.fromRunnable(pt::clean),
                    PoolableTest::isHealthy);
        }

        private PoolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator, Scheduler deliveryScheduler) {
            super(minSize, maxSize,
                    allocator,
                    pt -> Mono.fromRunnable(pt::clean),
                    PoolableTest::isHealthy,
                    deliveryScheduler);
        }

        private PoolableTestConfig(int minSize, int maxSize, Mono<PoolableTest> allocator,
                                   Consumer<? super PoolableTest> additionalCleaner) {
            super(minSize, maxSize,
                    allocator,
                    poolableTest -> Mono.fromRunnable(() -> {
                        poolableTest.clean();
                        additionalCleaner.accept(poolableTest);
                    }),
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
            pool.releaseSync(p);
        }
        assertThat(borrowed2).hasSize(3);
        assertThat(borrowed3).isEmpty();

        Thread.sleep(1000);
        for (PoolableTest p : borrowed2) {
            pool.releaseSync(p);
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
            pool.releaseSync(p);
        }
        assertThat(borrowed2).hasSize(3);
        assertThat(borrowed3).isEmpty();

        Thread.sleep(1000);
        for (PoolableTest p : borrowed2) {
            pool.releaseSync(p);
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
        AtomicInteger releasedCount = new AtomicInteger();

        PoolableTestConfig testConfig = new PoolableTestConfig(1, 1,
                Mono.fromCallable(PoolableTest::new),
                pt -> releasedCount.incrementAndGet());
        QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

        //borrow the only element
        PoolableTest element = pool.borrow().block();

        pool.borrow().subscribe().dispose();

        assertThat(releasedCount).as("before returning").hasValue(0);

        //release the element, which should forward to the cancelled second borrow, itself also cleaning
        pool.releaseSync(element);

        assertThat(releasedCount).as("after returning").hasValue(2);
    }

    @Test
    void allocatedReleasedIfBorrowerCancelled() throws InterruptedException {
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

        Thread.sleep(10);

        assertThat(newCount).as("created").hasValue(1);
        assertThat(releasedCount).as("released").hasValue(1);
    }


    @Test
    void defaultThreadDeliveringWhenHasElements() throws InterruptedException {
        AtomicReference<String> threadName = new AtomicReference<>();
        Scheduler borrowScheduler = Schedulers.newSingle("borrow");
        PoolableTestConfig testConfig = new PoolableTestConfig(1, 1,
                Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
        QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

        //the pool is started with one available element
        //we prepare to borrow it
        Mono<PoolableTest> borrower = pool.borrow();
        CountDownLatch latch = new CountDownLatch(1);

        //we actually request the borrow from a separate thread and see from which thread the element was delivered
        borrowScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()), e -> latch.countDown(), latch::countDown));
        latch.await(1, TimeUnit.SECONDS);

        assertThat(threadName.get())
                .startsWith("borrow-");
    }

    @Test
    void defaultThreadDeliveringWhenNoElementsButNotFull() throws InterruptedException {
        AtomicReference<String> threadName = new AtomicReference<>();
        Scheduler borrowScheduler = Schedulers.newSingle("borrow");
        PoolableTestConfig testConfig = new PoolableTestConfig(0, 1,
                Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
        QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

        //the pool is started with no elements, and has capacity for 1
        //we prepare to borrow, which would allocate the element
        Mono<PoolableTest> borrower = pool.borrow();
        CountDownLatch latch = new CountDownLatch(1);

        //we actually request the borrow from a separate thread, but the allocation also happens in a dedicated thread
        //we look at which thread the element was delivered from
        borrowScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()), e -> latch.countDown(), latch::countDown));
        latch.await(1, TimeUnit.SECONDS);

        assertThat(threadName.get())
                .startsWith("poolable test allocator-");
    }

    @Test
    void defaultThreadDeliveringWhenNoElementsAndFull() throws InterruptedException {
        AtomicReference<String> threadName = new AtomicReference<>();
        Scheduler borrowScheduler = Schedulers.newSingle("borrow");
        Scheduler releaseScheduler = Schedulers.fromExecutorService(
                Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"release"))));
        PoolableTestConfig testConfig = new PoolableTestConfig(1, 1,
                Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
        QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

        //the pool is started with one elements, and has capacity for 1.
        //we actually first borrow that element so that next borrow will wait for a release
        PoolableTest uniqueElement = pool.borrow().block();

        //we prepare next borrow
        Mono<PoolableTest> borrower = pool.borrow();
        CountDownLatch latch = new CountDownLatch(1);

        //we actually perform the borrow from its dedicated thread, capturing the thread on which the element will actually get delivered
        borrowScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()),
                e -> latch.countDown(), latch::countDown));
        //after a short while, we release the borrowed unique element from a third thread
        releaseScheduler.schedule(() -> pool.releaseSync(uniqueElement), 500, TimeUnit.MILLISECONDS);
        latch.await(1, TimeUnit.SECONDS);

        assertThat(threadName.get())
                .isEqualTo("release");
    }

    @Test
    @Tag("loops")
    void defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain_loop() throws InterruptedException {
        AtomicInteger releaserWins = new AtomicInteger();
        AtomicInteger borrowerWins = new AtomicInteger();

        for (int i = 0; i < 100; i++) {
            defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain(releaserWins, borrowerWins);
        }
        //look at the stats and show them in case of assertion error. We expect all deliveries to be on either of the racer threads.
        //we expect a subset of the deliveries to happen on the second borrower's thread
        String stats = "releaser won " + releaserWins.get() + ", borrower won " + borrowerWins.get();
        assertThat(borrowerWins.get()).as(stats).isPositive();
        assertThat(releaserWins.get() + borrowerWins.get()).as(stats).isEqualTo(100);
    }

    @Test
    void defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain() throws InterruptedException {
        AtomicInteger releaserWins = new AtomicInteger();
        AtomicInteger borrowerWins = new AtomicInteger();

        defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain(releaserWins, borrowerWins);

        assertThat(releaserWins.get() + borrowerWins.get()).isEqualTo(1);
    }

    void defaultThreadDeliveringWhenNoElementsAndFullAndRaceDrain(AtomicInteger releaserWins, AtomicInteger borrowerWins) throws InterruptedException {
        AtomicReference<String> threadName = new AtomicReference<>();
        Scheduler borrow1Scheduler = Schedulers.newSingle("borrow1");
        Scheduler racerReleaseScheduler = Schedulers.fromExecutorService(
                Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"racerRelease"))));
        Scheduler racerBorrowScheduler = Schedulers.newSingle("racerBorrow");

        PoolableTestConfig testConfig = new PoolableTestConfig(1, 1,
                Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")));
        QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

        //the pool is started with one elements, and has capacity for 1.
        //we actually first borrow that element so that next borrow will wait for a release
        PoolableTest uniqueElement = pool.borrow().block();

        //we prepare next borrow
        Mono<PoolableTest> borrower = pool.borrow();
        CountDownLatch latch = new CountDownLatch(1);

        //we actually perform the borrow from its dedicated thread, capturing the thread on which the element will actually get delivered
        borrow1Scheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName())
                , e -> latch.countDown(), latch::countDown));

        //in parallel, we'll both attempt a second borrow AND release the unique element (each on their dedicated threads
        Mono<PoolableTest> otherBorrower = pool.borrow();
        racerBorrowScheduler.schedule(() -> otherBorrower.subscribe().dispose(), 100, TimeUnit.MILLISECONDS);
        racerReleaseScheduler.schedule(() -> pool.releaseSync(uniqueElement), 100, TimeUnit.MILLISECONDS);
        latch.await(1, TimeUnit.SECONDS);

        //we expect that sometimes the race will let the second borrower thread drain, which would mean first borrower
        //will get the element delivered from racerBorrow thread. Yet the rest of the time it would get drained by racerRelease.
        if (threadName.get().startsWith("racerRelease")) releaserWins.incrementAndGet();
        else if (threadName.get().startsWith("racerBorrow")) borrowerWins.incrementAndGet();
        else System.out.println(threadName.get());
    }

    @Test
    void consistentThreadDeliveringWhenHasElements() throws InterruptedException {
        Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
        AtomicReference<String> threadName = new AtomicReference<>();
        Scheduler borrowScheduler = Schedulers.newSingle("borrow");
        PoolableTestConfig testConfig = new PoolableTestConfig(1, 1,
                Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                deliveryScheduler);
        QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

        //the pool is started with one available element
        //we prepare to borrow it
        Mono<PoolableTest> borrower = pool.borrow();
        CountDownLatch latch = new CountDownLatch(1);

        //we actually request the borrow from a separate thread and see from which thread the element was delivered
        borrowScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()), e -> latch.countDown(), latch::countDown));
        latch.await(1, TimeUnit.SECONDS);

        assertThat(threadName.get())
                .startsWith("delivery-");
    }

    @Test
    void consistentThreadDeliveringWhenNoElementsButNotFull() throws InterruptedException {
        Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
        AtomicReference<String> threadName = new AtomicReference<>();
        Scheduler borrowScheduler = Schedulers.newSingle("borrow");
        PoolableTestConfig testConfig = new PoolableTestConfig(0, 1,
                Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                deliveryScheduler);
        QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

        //the pool is started with no elements, and has capacity for 1
        //we prepare to borrow, which would allocate the element
        Mono<PoolableTest> borrower = pool.borrow();
        CountDownLatch latch = new CountDownLatch(1);

        //we actually request the borrow from a separate thread, but the allocation also happens in a dedicated thread
        //we look at which thread the element was delivered from
        borrowScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()), e -> latch.countDown(), latch::countDown));
        latch.await(1, TimeUnit.SECONDS);

        assertThat(threadName.get())
                .startsWith("delivery-");
    }

    @Test
    void consistentThreadDeliveringWhenNoElementsAndFull() throws InterruptedException {
        Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
        AtomicReference<String> threadName = new AtomicReference<>();
        Scheduler borrowScheduler = Schedulers.newSingle("borrow");
        Scheduler releaseScheduler = Schedulers.fromExecutorService(
                Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"release"))));
        PoolableTestConfig testConfig = new PoolableTestConfig(1, 1,
                Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                deliveryScheduler);
        QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

        //the pool is started with one elements, and has capacity for 1.
        //we actually first borrow that element so that next borrow will wait for a release
        PoolableTest uniqueElement = pool.borrow().block();

        //we prepare next borrow
        Mono<PoolableTest> borrower = pool.borrow();
        CountDownLatch latch = new CountDownLatch(1);

        //we actually perform the borrow from its dedicated thread, capturing the thread on which the element will actually get delivered
        borrowScheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName()),
                e -> latch.countDown(), latch::countDown));
        //after a short while, we release the borrowed unique element from a third thread
        releaseScheduler.schedule(() -> pool.releaseSync(uniqueElement), 500, TimeUnit.MILLISECONDS);
        latch.await(1, TimeUnit.SECONDS);

        assertThat(threadName.get())
                .startsWith("delivery-");
    }

    @Test
    @Tag("loops")
    void customThreadDeliveringWhenNoElementsAndFullAndRaceDrain_loop() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            customThreadDeliveringWhenNoElementsAndFullAndRaceDrain(i);
        }
    }

    @Test
    void customThreadDeliveringWhenNoElementsAndFullAndRaceDrain() throws InterruptedException {
        customThreadDeliveringWhenNoElementsAndFullAndRaceDrain(0);
    }

    void customThreadDeliveringWhenNoElementsAndFullAndRaceDrain(int i) throws InterruptedException {
        Scheduler deliveryScheduler = Schedulers.newSingle("delivery");
        AtomicReference<String> threadName = new AtomicReference<>();
        Scheduler borrow1Scheduler = Schedulers.newSingle("borrow1");
        Scheduler racerReleaseScheduler = Schedulers.fromExecutorService(
                Executors.newSingleThreadScheduledExecutor((r -> new Thread(r,"racerRelease"))));
        Scheduler racerBorrowScheduler = Schedulers.newSingle("racerBorrow");

        PoolableTestConfig testConfig = new PoolableTestConfig(1, 1,
                Mono.fromCallable(PoolableTest::new)
                        .subscribeOn(Schedulers.newParallel("poolable test allocator")),
                deliveryScheduler);
        QueuePool<PoolableTest> pool = new QueuePool<>(testConfig);

        //the pool is started with one elements, and has capacity for 1.
        //we actually first borrow that element so that next borrow will wait for a release
        PoolableTest uniqueElement = pool.borrow().block();

        //we prepare next borrow
        Mono<PoolableTest> borrower = pool.borrow();
        CountDownLatch latch = new CountDownLatch(1);

        //we actually perform the borrow from its dedicated thread, capturing the thread on which the element will actually get delivered
        borrow1Scheduler.schedule(() -> borrower.subscribe(v -> threadName.set(Thread.currentThread().getName())
                , e -> latch.countDown(), latch::countDown));

        //in parallel, we'll both attempt a second borrow AND release the unique element (each on their dedicated threads
        Mono<PoolableTest> otherBorrower = pool.borrow();
        racerBorrowScheduler.schedule(() -> otherBorrower.subscribe().dispose(), 100, TimeUnit.MILLISECONDS);
        racerReleaseScheduler.schedule(() -> pool.releaseSync(uniqueElement), 100, TimeUnit.MILLISECONDS);
        latch.await(1, TimeUnit.SECONDS);

        //we expect that, consistently, the poolable is delivered on a `delivery` thread
        assertThat(threadName.get()).as("round #" + i).startsWith("delivery-");
    }

}