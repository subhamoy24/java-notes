# REST API takes 30 seconds because it calls 3 downstream APIs. How would you optimize it?

**Level:** Mid → Senior → Lead

This is a latency / integration-design question. Interviewers are not looking for “use a faster JSON parser.” They want you to **measure**, **cut sequential wait**, **bound tail latency**, and **decide what the client actually needs**.

---

## 1. Direct Core Answer

First I would confirm *why* it is 30 seconds: three sequential HTTP calls with no timeout will add (`t1 + t2 + t3`). If the three calls are independent, I run them **in parallel** so wall-clock time becomes roughly `max(t1, t2, t3)` plus a small fan-in cost — often ~10s instead of 30s if each downstream is ~10s. Then I add **hard timeouts**, a dedicated thread/connection pool (bulkhead), **caching** for stable data, and **partial success / fail-open** so one slow dependency cannot pin every request to 30s.

If 10s is still too slow for a synchronous REST call, the real fix is architectural: don’t do that work on the request path (precompute, cache, 202+poll, or a coarser-grained downstream API).

---

## 2. Under-the-Hood Details

### 2.1 Latency math (say this out loud)

| Pattern | Wall-clock (if each call is 10s) |
|---------|-----------------------------------|
| Sequential `callA(); callB(); callC();` | ~30s |
| Parallel, wait for all | ~10s + fan-in |
| Parallel, timeout 800ms each | ≤ ~800ms (some data missing) |
| Cached A+B, live C | ~ time of C |

A REST handler’s latency is **not** CPU. It is almost always **wait time** on sockets: DNS, TLS handshake, connection acquisition, server processing, retries. `jstack` / JFR will show the request thread in `socketRead0` / `park`, not in your business logic.

### 2.2 Why sequential is the default trap

Typical Spring MVC / Jakarta code:

```text
A = restClient.get(urlA)   // request thread blocked
B = restClient.get(urlB)
C = restClient.get(urlC)
return dto(A, B, C)
```

The servlet/Tomcat (or Jetty) **worker thread** is blocked for the whole 30s. With a pool of 200 threads you have ~6–7 such requests/sec before the pool is exhausted — then *unrelated* endpoints start timing out. That is a **bulkhead** failure, not just a slow API.

### 2.3 Parallelization mechanics (Java)

- **`CompletableFuture` + `Executor`**: each downstream call is a task; `allOf` joins. You **must** use your own executor (not `ForkJoinPool.commonPool()`), sized for *blocking I/O* (roughly: expected concurrency × blocking time, or just virtual threads).
- **Virtual threads (Java 21)**: a blocking HTTP client on a virtual thread is the cleanest model. One virtual thread per downstream call; the carrier isn’t held during socket wait (watch **pinning** on `synchronized` around I/O on older JDKs).
- **Reactive (`WebClient`, Project Reactor)**: event-loop does not occupy a worker per wait. Wins when you already live in a reactive stack; mixing MVC + `block()` on `WebClient` often gives you the worst of both worlds.

Independent calls → parallelize. If B needs A’s id, you can still overlap A∥C, then B.

### 2.4 Timeouts, retries, and tail latency

Without timeouts, a hung downstream = hung API. You need **all three**:

1. **Connect timeout** — fail fast if the peer is down.
2. **Response / read timeout** (or idle timeout) — cap wait for the body.
3. **Overall request deadline** — budget for the incoming SLO (e.g. 800ms) so A+B+C cannot exceed it even in parallel.

Retries **multiply** latency: 3 calls × 3 retries × 10s = disaster. Retry only **idempotent** GETs, with jittered backoff, and a **retry budget**. Hedged requests (send a duplicate after p95) can cut tails but double downstream load — Lead-level trade-off.

### 2.5 Connection pools

Each downstream should have its **own** HTTP client / pool (bulkhead). One shared pool lets the slowest service starve the others (pool wait shows up as “our API is slow” when the time is spent in `leased / pending` connections). Size pools from concurrent in-flight calls, not from “200 because Tomcat is 200.”

TLS session reuse, HTTP/2 multiplexing, and keep-alive matter when you go from 30s sequential to hundreds of parallel short calls — otherwise you just move the bottleneck to handshake/pool.

### 2.6 Caching and request collapsing

If any of the three payloads are user-independent or change slowly (config, catalog, entitlements): **Caffeine** (in-process) or Redis (multi-instance). For **identical in-flight** keys, collapse with `computeIfAbsent` + `CompletableFuture` so 500 concurrent requests don’t stampede 3×500 downstream calls.

### 2.7 What if 30s is *one* of the three?

Parallelism does not help a 30s dependency. Then: timeout + stale cache, async completion, or fix/split that service. Always **measure per dependency** (histogram, not averages).

### 2.8 JVM / runtime notes interviewers like

- Blocking I/O: thread is parked in native `read`; heap is mostly idle; CPU looks low; **thread dump is the smoking gun**.
- `RestTemplate` is blocking; `WebClient.block()` is still blocking the caller.
- Platform-thread pools for blocking work: if `maxThreads` ≈ downstream P99 × QPS, you OOME or hit native thread limits. Virtual threads change that calculus.
- Serialization of large payloads can add CPU, but it does not explain 30s unless payloads are pathological.

---

## 3. Code Example

### 3.1 Sequential (the 30s bug)

```java
public record Page(User user, Orders orders, Loyalty loyalty) {}

public Page loadSequential(String userId) {
    User user = userClient.get(userId);         // 10s
    Orders orders = orderClient.get(userId);   // 10s
    Loyalty loyalty = loyaltyClient.get(userId); // 10s
    return new Page(user, orders, loyalty);       // ~30s
}
```

### 3.2 Parallel fan-out with a deadline (Java 21 virtual threads)

```java
public final class PageAssembler {
    private final UserClient users;
    private final OrderClient orders;
    private final LoyaltyClient loyalty;
    private final Executor virtual = Executors.newVirtualThreadPerTaskExecutor();
    private static final Duration BUDGET = Duration.ofMillis(800);

    public PageAssembler(UserClient users, OrderClient orders, LoyaltyClient loyalty) {
        this.users = users;
        this.orders = orders;
        this.loyalty = loyalty;
    }

    public Page load(String userId) {
        long deadlineNanos = System.nanoTime() + BUDGET.toNanos();

        CompletableFuture<User> userF = supply(virtual, () -> users.get(userId));
        CompletableFuture<Orders> ordersF = supply(virtual, () -> orders.get(userId));
        CompletableFuture<Loyalty> loyaltyF = supply(virtual, () -> loyalty.get(userId));

        try {
            CompletableFuture.allOf(userF, ordersF, loyaltyF)
                    .orTimeout(remaining(deadlineNanos), TimeUnit.NANOSECONDS)
                    .join();
            return new Page(userF.join(), ordersF.join(), loyaltyF.join());
        } catch (CompletionException e) {
            throw new DownstreamTimeoutException("page assemble exceeded " + BUDGET, e);
        }
    }

    private static long remaining(long deadlineNanos) {
        return Math.max(0, deadlineNanos - System.nanoTime());
    }

    private static <T> CompletableFuture<T> supply(Executor ex, Callable<T> c) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return c.call();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, ex);
    }
}
```

Each HTTP client still needs **connect + read timeouts shorter than `BUDGET`**, otherwise you leak tasks after `orTimeout`.

### 3.3 Partial success (Lead answer: degrade, don’t fail the page)

```java
public Page loadDegraded(String userId) {
    CompletableFuture<User> userF = supply(virtual, () -> users.get(userId));
    CompletableFuture<Orders> ordersF = supply(virtual, () -> orders.get(userId));
    CompletableFuture<Loyalty> loyaltyF = supply(virtual, () -> loyalty.get(userId));

    User user = userF.orTimeout(300, TimeUnit.MILLISECONDS)
            .exceptionally(ex -> { throw new IllegalStateException("user is required", ex); })
            .join();

    Orders orders = ordersF.orTimeout(400, TimeUnit.MILLISECONDS)
            .exceptionally(ex -> Orders.empty())
            .join();

    Loyalty loyalty = loyaltyF.orTimeout(200, TimeUnit.MILLISECONDS)
            .exceptionally(ex -> Loyalty.unknown())
            .join();

    return new Page(user, orders, loyalty);
}
```

### 3.4 Don’t use the common pool for blocking HTTP

```java
// WRONG — starves ForkJoinPool.commonPool() (parallel streams, other CF work)
CompletableFuture.supplyAsync(() -> userClient.get(id));

// RIGHT — blocking I/O isolation
Executor downstream = Executors.newVirtualThreadPerTaskExecutor();
// or for platform threads: ThreadPoolExecutor with a *bounded* queue + CallerRuns/abort
```

### 3.5 HTTP client timeouts (Java 21 `HttpClient` sketch)

```java
HttpClient http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(100))
        .executor(Executors.newVirtualThreadPerTaskExecutor())
        .build();

HttpRequest req = HttpRequest.newBuilder(uri)
        .timeout(Duration.ofMillis(400))   // response timeout
        .GET()
        .build();
```

### 3.6 Cache + single-flight (stampede control)

```java
public final class CachedUserClient {
    private final UserClient origin;
    private final Cache<String, User> cache = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofSeconds(30))
            .maximumSize(100_000)
            .build();
    private final ConcurrentHashMap<String, CompletableFuture<User>> inFlight = new ConcurrentHashMap<>();

    public CachedUserClient(UserClient origin) { this.origin = origin; }

    public CompletableFuture<User> get(String userId, Executor ex) {
        User hit = cache.getIfPresent(userId);
        if (hit != null) {
            return CompletableFuture.completedFuture(hit);
        }
        return inFlight.computeIfAbsent(userId, id ->
                CompletableFuture.supplyAsync(() -> origin.get(id), ex)
                        .whenComplete((user, err) -> {
                            if (user != null) cache.put(id, user);
                            inFlight.remove(id);
                        }));
    }
}
```

---

## 4. Interview Pro-Tips & Traps

### Diagnose before you “optimize”

A strong Mid/Senior answer starts with: *“I would look at traces.”* One span per downstream, + queue wait, + pool acquire. Possible 30s stories:

| Observation | Likely cause | Fix |
|-------------|--------------|-----|
| 10s + 10s + 10s on one trace | Sequential composition | Parallel fan-out |
| One span is 30s, others 50ms | One bad dependency | Timeout, cache, isolate, fix that service |
| 30s exactly, round numbers | Default timeout (often 30s) | You are waiting for the *timeout*, not the work |
| Fast in isolation, slow under load | Thread/connection pool exhaustion | Bulkheads, virtual threads, bound queues |
| Retries in logs | Retry amplifying failure | Retry budget, circuit breaker |

### Common traps (they will poke these)

- **“I’ll use `@Async`.”** Without an executor and timeouts you just hide the same blocking on another pool.
- **`CompletableFuture.supplyAsync` with no executor.** Blocking HTTP on `commonPool()` is a production incident.
- **Parallel + no timeout.** You still have a 30s P99 if one peer hangs.
- **`synchronizedMap` / one giant lock around three calls.** You serialized them again.
- **Circuit breaker without fallback.** You fail faster (good) but still error (maybe not good).
- **Caching user-specific sensitive data** in a shared key. Key design and TTL are part of the answer.
- **Reactive-over-MVC `.block()`.** Looks modern; still occupies the Tomcat thread for 30s.
- **Unbounded `newCachedThreadPool`** for fan-out: native thread OOME under retry storms.

### Typical follow-up questions (have answers ready)

**Q: The three calls are not independent — B needs A’s response.**  
Pipeline: start C immediately; `thenCompose` A → B; wall clock ≈ `max(C, A+B)`.

**Q: How do you size the pool?**  
Platform threads: think *in-flight = QPS × P99 latency*. Virtual threads: still limit **downstream concurrency** (semaphores) so you don’t crush the other service.

**Q: What SLO do you pick?**  
Work backwards from the product: user-facing p99 300ms cannot include three 10s calls. Either cache/precompute or change the UX (spinner with 202, or omit the slow panel).

**Q: Circuit breaker vs timeout vs bulkhead?**  
Timeout = cap one call. Bulkhead = cap how many calls you run (protect *us*). Circuit breaker = stop calling a *sick* peer (protect *them* and our thread pool). You want all three.

**Q: Would you use Kafka instead?**  
Only if the client does not need the data on that request. Messaging is not an optimization for a synchronous page assemble; it is a different contract.

**Q: How do you prove the fix?**  
Load test with a downstream mock that sleeps 10s. Show p99: sequential ~30s → parallel ~10s → timeout 400ms. Add a chaos case: one service dead.

### Lead-level close (say this last)

> “Parallelism is the first 80% if the calls are independent. I would not ship a 10s synchronous REST endpoint even after fan-out. I would ask whether the page needs all three datasets live, put a deadline on the incoming request, isolate each dependency, and move anything slow off the request path. The code change is `CompletableFuture`/`virtual threads`; the engineering change is a latency budget.”

---

## Quick interview checklist (memorize)

1. **Measure** — traces, not guesses; sequential vs one outlier vs default 30s timeout.  
2. **Parallelize** independent I/O; pipeline the rest.  
3. **Timeout + deadline** — never inherit 30s client defaults.  
4. **Isolate** — per-dependency pool/semaphore (bulkhead).  
5. **Cache / single-flight** — stop stampeding.  
6. **Degrade** — required vs optional downstreams.  
7. **If still too slow** — it does not belong on the GET request path.
