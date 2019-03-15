# Reactor-Pool

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

The `reactor-pool` project aims at providing a generic object pool to reactive application that:
 - exposes a reactive API (`Publisher` input types, `Mono` return types)
 - is non-blocking (never blocking a user that makes an attempt to `acquire()` a resource)
 - has lazy acquire behavior

For use-cases where granular control of the `release()` is needed, the classic path of `acquire()` is offered, which exposes a `PooledRef` wrapper to the resource. This also allows access to statistics about the resource lifecycle in the pool.

```java
// given Pool<T> pool
Mono<PooledRef<T>> grabResource = pool.acquire();
//no resource is actually requested yet at this point

grabResouce.subscribe();
//now one resource is requested from the pool asynchronously

//Another example, this time synchronously acquiring, immediately followed up by a release:
PooledRef<T> ref = grabResource.block(); //second subscription requests a second resource
ref.release().block(); //release() is also asynchronous and lazy
```

For use-cases where the resource itself can be consumed reactively (exposes a reactive API), a scoped mode of acquisition is offered. `acquireInScope`:
 - let the consumer declaratively use the resource
 - provides a scope / closure in which the resource is acquired, used as instructed and released automatically
 - avoids dealing with an indirection (the resource is directly exposed)

```java
//given at DbConnection type and a Pool<DbConnection> pool
pool.acquireInScope(resourceMono -> resourceMono
    //we declare using the connection to create a Statement...
    .flatMap(DbConnection::createStatement)
    //...then performing a SELECT query...
    .flatMapMany(st -> st.query("SELECT * FROM foo"))
    //...then marshalling the rows to JSON
    .map(row -> rowToJson(row))
    //(all of which need the live resource)
)
//at this point the rest of the steps are outside the scope
//so the resource can be released
.map(json -> sanitize(json));
```

_Licensed under [Apache Software License 2.0](www.apache.org/licenses/LICENSE-2.0)_