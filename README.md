# dply/laravel

Laravel queues on Cloudflare Queues for apps deployed as **dply Edge containers**.

```bash
composer require dply/laravel
```

Then set `QUEUE_CONNECTION=dply` in the site's Environment. Nothing else to run —
there is no `queue:work` process:

- `dispatch(new SendInvoice($order))` sends the job to the site's Cloudflare
  Queue through its Worker (`DPLY_APP_URL` + `DPLY_QUEUE_TOKEN`, both injected
  by dply).
- The site Worker consumes the queue and POSTs each batch to `/_dply/queue`,
  which runs the jobs in the container. Failed jobs are retried by Cloudflare
  with Laravel's usual `tries` / `failed()` handling.

The queue binding defaults to `JOBS` (Edge → Jobs); pick another with
`DPLY_QUEUE=MY_BINDING` or `->onQueue('MY_BINDING')`.

## Key-value store

Attach a key-value store under Resources. The next deploy injects
`DPLY_KV_HOST` and, unless Redis is attached, `CACHE_STORE`. Then:

```php
Cache::put('session', 'hello');
Cache::get('session');
Cache::forget('session');
```

The store name is the resource name, lowercased: `Cache::store('testing')`.
Values expire only when the TTL is at least 60 seconds. `Cache::many()` reads
up to 100 keys per request, and `Cache::flush()` removes every key.

`increment()` and `decrement()` throw: a key-value store is eventually
consistent and cannot count atomically. Counters, locks (`Cache::lock()`) and
rate limiting need Valkey (Redis) or State.

## Changelog

### 1.6.0

- In a dply queue worker (`DPLY_ROLE=worker`), every loop of `queue:work`
  and of Horizon's workers touches `/tmp/dply-worker-heartbeat` (at most every
  10 s). A job about to start dates the file to its deadline (its own
  `timeout`, else the worker's `--timeout`), so a long job is not mistaken for a
  hang. The container reports it to dply, which restarts a worker whose loops
  stopped while its container stayed up.

### 1.5.0

- On dply, a queue worker whose Redis connection died (a gateway restart, the
  store slept) reconnects and retries the pop once instead of logging
  `read error on connection`: BLPOP/BRPOP and the queue's pop and
  expired-jobs scripts only. Writes are never retried.
- The reconnecting Redis connector applies with `REDIS_PERSISTENT=false` too
  (dply's default since 2026-11-17). Before, it only applied to kept connections.
- A Redis connect waits 3 s for TCP and TLS (`DPLY_REDIS_CONNECT_TIMEOUT`;
  an app's own `timeout` wins), not PHP's 60 s, and a stalled handshake
  ("Failed to enable crypto") is retried twice with jitter.

### 1.4.0

- Removed the `dply` mailer (`MAIL_MAILER=dply`) and inbound mail
  (`/_dply/mail`, `InboundMailReceived`): dply no longer sends or receives mail
  for apps. Connect a mail provider instead. Drops the `symfony/mailer` requirement.

### 1.1.0

- The key-value store implements `touch()`. Laravel 13 added it to the cache
  store contract, and without it the store failed to load.
- `Cache::flush()` removes every key. It used to stop after the first 100.
- `Cache::many()` reads in one request per 100 keys; `putMany()` writes in parallel.
- `increment()` and `decrement()` on the key-value store now throw. They read
  and wrote the value back, so concurrent calls lost updates. Move counters and
  rate limiting to Valkey (Redis) or State.

## Object storage

Attach a bucket under Resources. The next deploy injects `DPLY_STORAGE_HOST`
and, unless you already set it, `FILESYSTEM_DISK`. Then the same calls you
use for S3 talk to that bucket:

```php
Storage::put('uploads/photo.jpg', $bytes);
Storage::get('uploads/photo.jpg');
Storage::disk('s3')->delete('uploads/photo.jpg');
```

A saved `AWS_ACCESS_KEY_ID` keeps your own `s3` disk. The bucket is still
`Storage::disk('uploads')` (the name you gave the resource, lowercased).

## Scheduler

Turn on **Container → Run the Laravel scheduler every minute** and dply calls
`schedule:run` on POST `/_dply/schedule` from a Cloudflare Cron Trigger. Crons
added under **Crons** run their handler as an artisan command.

Limits: 128 KB per job payload, 12 h max delay (Cloudflare Queues).

## Mail

Attach a mail provider under Resources → Mail. The next deploy sets
`MAIL_MAILER` and its keys. Laravel's default config has no `cloudflare` or
`mailgun` mailer, so this package adds them (your own entries win).
Previews get `MAIL_MAILER=log`: they never email real people.
