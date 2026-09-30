# proxy plugin

Holds a list of upstream proxies, measures them against real websites, and hands
back one that is currently working. Other plugins drive it over the host's
cross-plugin callback interface; there is also a small HTTP surface on port
`8080` for inspection by hand.

Registered with the host as `Start(Spawn)`, so `on_start` runs on a detached
thread and is expected to stay alive for the life of the process.

## Callbacks

All three are reached with `client::external_plugin_call`. The host dispatches
callbacks through `tokio::task::spawn_blocking` (see `src/ipc.rs`), so they run
off the async executor and use `reqwest::blocking` rather than nesting a runtime
inside one.

The host looks a callback up by an exact match on
`CallbackInfo { func, vers, data_name, data }`, so the `data_name` list **and** the
order and types of the values must match the registration in `get_plugin_info`.

### `proxy_get_proxy`

Returns a proxy that is currently working, without making a request.

| | |
| --- | --- |
| in | `site: String` |
| out | `proxy_url: String`, `proxy_type: String`, `rating: u64` |

`proxy_url` is empty and `rating` is `0` when nothing in the list has passed a
check yet.

```rust
let out = client::external_plugin_call(
    "proxy_get_proxy".to_string(),
    CallbackInfoInput {
        vers: 0,
        data_name: vec!["site".into()],
        data: vec![CallbackCustomDataReturning::String("example.com".into())],
    },
)?;
```

### `proxy_request`

Makes the request through a chosen proxy and records the outcome.

| | |
| --- | --- |
| in | `url: String`, `site: String`, `timeout_ms: U64`, `retry_after_secs: U64` |
| out | `success: U64`, `status: U64`, `body: VU8`, `proxy_url: String`, `proxy_type: String`, `rating: U64` |

`timeout_ms` and `retry_after_secs` are optional and fall back to `10000` and
`3600`. `status` is `0` and `body` empty when the proxy could not be reached.

### `proxy_report`

Records an outcome the caller observed itself, for callers that already have
their own HTTP stack and only want the ranking updated.

| | |
| --- | --- |
| in | `proxy_url: String`, `site: String`, `success: U64` |
| out | `found: U64`, `rating: U64` |

`proxy_url` matches an entry with or without an `http://` prefix, so bare
`1.2.3.4:8080` and `http://1.2.3.4:8080` are the same entry. `found` is `0` when
the URL is not in the list, which is deliberately distinct from `rating = 0`:
an unknown URL is not evidence about any listed proxy, so nothing is scored.

## Scoring

Binary, as requested: a check either produced an HTTP response (`rating = 1`) or
it did not (`rating = 0`). Any HTTP status counts as a success, including 4xx and
5xx — the question is whether the proxy carried the request, not whether the
target liked it.

Ranking among working proxies uses the success rate in `rating_history`, so a
proxy that worked nine times out of ten outranks one that got lucky once.
`rating_history` is capped at 32 entries, keeping the rate about recent
behaviour rather than all-time behaviour.

An entry whose `proxy_type` cannot be turned into a reqwest proxy is never used,
even if it claims `rating = 1`.

## The per-site retry window

`retry_after_secs` is the point of the "retry in an hour" behaviour. Each `site`
gets its own window, stored in the `PLUGIN_proxy_sites` setting:

- **Inside the window** — the best known-working proxy is reused, and the rest of
  the list is left alone.
- **Once the window has elapsed** — the list is walked round-robin instead. This
  is what refreshes the ranking, and it is why a stale proxy eventually gets
  re-tested rather than trusted forever.
- **With nothing known to work** — a caller making a request still gets an entry
  so the list can be measured; a read-only caller gets nothing rather than a
  proxy already marked bad.

A `retry_after_secs` of `0` means "use the default" rather than "probe
constantly", so the stored window is never zero.

## Settings

| key | contents |
| --- | --- |
| `PLUGIN_proxy_list` | the proxies and their ratings |
| `PLUGIN_proxy_sites` | per-site probe schedule |

A proxy entry:

```json
{
  "name": "good",
  "proxy_type": "http",
  "proxy_url": "127.0.0.1:3128",
  "rating": 1,
  "rating_history": [1, 1, 1]
}
```

`proxy_url` may be bare `host:port`; `proxy_type` supplies the scheme in that
case. Accepted types are `http`, `https`, `socks4`, `socks4a`, `socks5`, and
`socks5h`.

The registry is read from settings once per process and then held in memory, so
edits made outside the plugin are not seen until the host restarts. Writes go
back to the settings on every mutation.

## HTTP surface

```sh
curl http://127.0.0.1:8080/health          # 200 "Health check"
curl http://127.0.0.1:8080/proxy           # 200 + one working proxy url, or 503
curl 'http://127.0.0.1:8080/proxy?site=x'  # same, scoped to a site
curl http://127.0.0.1:8080/proxies         # 200 + the whole ranking as json
```

`/proxy` is the quickest way to check the plugin by hand.

## Working against the published vetis beta line

The plugin builds against stock crates.io releases with **no** `[patch.crates-io]`
and no `rev` in any `Cargo.toml`:

```toml
vetis = "0.1.7-beta.3"
vetis-tokio = { version = "0.1.2-beta.4", default-features = false, features = ["rust-tls"] }
```

An earlier version of this plugin needed a fork because `/health` returned
`400 Bad Request: Host not found in request` for every ordinary client. That was
`vetis` resolving the target virtual host from the request URI authority alone
(`vetis/src/server.rs`):

```rust
let Some(authority) = req.uri().authority() else {
    return /* 400 Bad Request */;
};
```

Under **hyper 1.x**, an HTTP/1.1 request line in origin-form
(`GET /health HTTP/1.1`) parses into a `Uri` holding only the path and query. The
host stays in the `Host` header and is never copied into the URI, so
`uri().authority()` is `None`. The authority is only populated for HTTP/2 (via
`:authority`) and for HTTP/1.1 sent in absolute-form — the shape a forward proxy
emits, not what curl or a browser sends.

The beta line already carries the `Host`-header fallback, so the fork is not
needed. Three API changes had to be absorbed to get there:

1. `HostImpl` became `Host` (`vetis_tokio::host`).
2. `Vetis::new(ServerConfig...)` plus `server.add_host(host).await` became the
   builder: `Vetis::builder().add_listeners(build_listeners(config)?)`, then
   `builder = builder.add_host(host)?` and `let mut server = builder.build()`.
   `add_listeners` takes built `Listener`s, not a `ListenerConfig`.
3. The `http1` feature is gone; HTTP/1 is implicit. `rust-tls` is all that is
   needed.

## Three ways this bites on the beta line

**Virtual host names are an exact `hostname:port` match.** `vetis-tokio` keys
hosts as `format!("{}:{}", host.hostname(), port)`
(`listener/tcp.rs`), so stripping the port before lookup is wrong here and turns
a working request into a `502`. The plugin registers every name a local probe can
legitimately arrive under:

```rust
for hostname in ["localhost", "127.0.0.1", "[::1]"] { /* ... */ }
```

An unregistered host still gets `502`, so this does not weaken host matching.

**A host with no `bind_addresses` is silently never registered.** The beta line
attaches a host to each listener whose interface *and* port match an entry in the
host's bind addresses. Omit it and the listener accepts connections then resets
them. `HostConfig` must mirror the listener config:

```rust
.bind_addresses(vec![("0.0.0.0".parse()?, port)])
```

**Plaintext needs an explicit opt-in.** Without
`.allow_unsafe_connections(true)` the TCP worker accepts a connection, peeks to
decide TLS vs plaintext, and on a plaintext request returns `Ok(())` without
writing a response — the client sees "connection reset by peer" and curl reports
HTTP `000`. `0.1.6` allowed plaintext implicitly, so this opt-in did not exist
before.

## Shutdown

Calling `server.run()` blocks on `tokio::signal::ctrl_c()`. That both swallows the
host's shutdown signal and makes the `should_exit` poller unreachable dead code,
so the listener was never stopped through the host's lifecycle. The plugin calls
`server.start()` and drives shutdown from the host's exit flag:

```rust
server.start().await?;
loop {
    let exit = client::should_exit_async().await;
    if exit.is_err() || exit.is_ok_and(|f| f) { break; }
    Timer::after(Duration::from_secs(1)).await;
}
server.stop().await?;
```

## Notes

- The listener is configured for `HTTP/1.1` only and bound to `0.0.0.0`, so an
  HTTP/2 prior-knowledge client is refused and `[::1]` is not reachable even
  though it is registered as a hostname.
- Plugins are built with `panic = "abort"` and every entry point here is
  `extern "C"`, so a panic would take the whole host process down. Nothing on
  these paths unwraps; a poisoned registry lock is recovered rather than
  propagated.
- The plugin previously declared `GlobalCallbacks::Download` without exporting an
  `on_download` symbol. That declaration is gone.
