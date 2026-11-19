<?php

declare(strict_types=1);

namespace Dply\Laravel;

use Illuminate\Cache\CacheManager;
use Illuminate\Console\Events\CommandStarting;
use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Filesystem\FilesystemAdapter;
use Illuminate\Http\Request;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\Events\Looping;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Route;
use Illuminate\Support\Facades\Storage;
use Illuminate\Support\ServiceProvider;
use League\Flysystem\AwsS3V3\AwsS3V3Adapter;
use League\Flysystem\Filesystem;
use Pdo\Pgsql;

class DplyServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        if (! $this->app['config']->has('queue.connections.dply')) {
            $this->app['config']->set('queue.connections.dply', [
                'driver' => 'dply',
                'queue' => env('DPLY_QUEUE', 'JOBS'),
                'app_url' => env('DPLY_APP_URL'),
                'token' => env('DPLY_QUEUE_TOKEN'),
            ]);
        }

        // Laravel's default config has no cloudflare or mailgun mailer, so
        // MAIL_MAILER=cloudflare from a connected provider would throw "Mailer
        // [cloudflare] is not defined". An app's own entries win.
        foreach (['cloudflare', 'mailgun'] as $mailer) {
            if (! $this->app['config']->has("mail.mailers.{$mailer}")) {
                $this->app['config']->set("mail.mailers.{$mailer}", ['transport' => $mailer]);
            }
        }
        if (! $this->app['config']->has('services.cloudflare')) {
            $this->app['config']->set('services.cloudflare', ['account_id' => env('CLOUDFLARE_ACCOUNT_ID'), 'key' => env('CLOUDFLARE_KEY')]);
        }
        if (! $this->app['config']->has('services.mailgun')) {
            $this->app['config']->set('services.mailgun', ['domain' => env('MAILGUN_DOMAIN'), 'secret' => env('MAILGUN_SECRET'), 'endpoint' => env('MAILGUN_ENDPOINT', 'api.mailgun.net'), 'scheme' => 'https']);
        }

        $this->registerStorageDisks();
        $this->registerKvStores();
        $this->blockOnRedisQueue();
        $this->oneRoundTripPostgres();
        // An app's own Postgres connector wins.
        if (env('DPLY_QUEUE_TOKEN') && ! $this->app->bound('db.connector.pgsql')) {
            $this->app->bind('db.connector.pgsql', DplyPostgresConnector::class);
        }
        $this->oneRoundTripMysql();
        $this->persistentConnections();
    }

    /**
     * On dply, database and Redis connections stay open between requests.
     * The data sits behind a TLS gateway ~40 ms away, and a new connection
     * costs ~5 round trips (TCP, TLS, startup and auth) before the first
     * query: ~200 ms a request. The gateway pipes whole sessions (no
     * transaction pooling), so a kept connection is safe. PDO rolls back a
     * transaction a request left open, and pdo_pgsql reconnects a connection
     * the gateway closed while the database slept. The persistent id is the
     * connection name, so two names for one database never share a session.
     * Session state an app sets itself (SET, advisory locks) carries over to
     * the next request on that worker. Only when the app did not set it.
     *
     * Database connections only where the image says so (DPLY_PERSISTENT_PDO,
     * php-fpm): a FrankenPHP thread never exits, so each would hold one open
     * for as long as the container runs, and Postgres allows 50.
     */
    private function persistentConnections(): void
    {
        if ((string) env('DPLY_QUEUE_TOKEN', '') === '') {
            return;
        }
        $config = $this->app['config'];
        $pdo = filter_var(env('DPLY_PERSISTENT_PDO', false), FILTER_VALIDATE_BOOL);
        foreach ($pdo ? (array) $config->get('database.connections', []) : [] as $name => $connection) {
            if (! is_array($connection) || ! in_array($connection['driver'] ?? '', ['pgsql', 'mysql', 'mariadb'], true)) {
                continue;
            }
            $options = (array) ($connection['options'] ?? []);
            if (! array_key_exists(\PDO::ATTR_PERSISTENT, $options)) {
                $options[\PDO::ATTR_PERSISTENT] = 'dply-'.$name;
                $config->set("database.connections.{$name}.options", $options);
            }
        }
        if ($config->get('database.redis.client', 'phpredis') !== 'phpredis') {
            return;
        }
        // Kept or not (dply injects REDIS_PERSISTENT=false): a connect that
        // lost its socket, and a queue pop on a connection the gateway closed
        // (a gateway roll, the store slept), reconnect instead of throwing.
        $this->app->afterResolving('redis', fn ($redis) => $redis->extend('phpredis', fn () => new DplyRedisConnector));
        // Laravel's config ships options.persistent = env('REDIS_PERSISTENT', false),
        // so the env var unset is what "the app did not choose" looks like.
        if (env('REDIS_PERSISTENT') !== null) {
            return;
        }
        $config->set('database.redis.options.persistent', true);
        foreach ((array) $config->get('database.redis', []) as $name => $connection) {
            if (is_array($connection) && ! in_array($name, ['options', 'clusters'], true) && ! isset($connection['persistent_id'])) {
                $config->set("database.redis.{$name}.persistent_id", 'dply-'.$name);
            }
        }
    }

    /**
     * Opt-in (DPLY_MYSQL_ONE_ROUND_TRIP=true): MySQL queries in one round trip
     * instead of two. Laravel turns emulated prepares off, so pdo_mysql sends
     * PREPARE and EXECUTE separately. Emulated, PDO escapes the parameters
     * into the query itself (with the connection's charset) and sends it once.
     * Since PHP 8.1 numbers still come back as ints and floats. What changes:
     * parameters reach MySQL as quoted literals, so a column compared to a
     * string parameter is cast by MySQL, not bound by type. Only when the app
     * did not set it.
     */
    private function oneRoundTripMysql(): void
    {
        if ((string) env('DPLY_QUEUE_TOKEN', '') === '' || ! filter_var(env('DPLY_MYSQL_ONE_ROUND_TRIP', false), FILTER_VALIDATE_BOOL)) {
            return;
        }
        $config = $this->app['config'];
        foreach ((array) $config->get('database.connections', []) as $name => $connection) {
            if (! is_array($connection) || ! in_array($connection['driver'] ?? '', ['mysql', 'mariadb'], true)) {
                continue;
            }
            $options = (array) ($connection['options'] ?? []);
            if (! array_key_exists(\PDO::ATTR_EMULATE_PREPARES, $options)) {
                $options[\PDO::ATTR_EMULATE_PREPARES] = true;
                $config->set("database.connections.{$name}.options", $options);
            }
        }
    }

    /**
     * On dply, Postgres queries go in one round trip instead of three.
     * Laravel prepares a new statement for every query; pdo_pgsql then
     * sends PREPARE, EXECUTE and DEALLOCATE as separate round trips (265 ms
     * vs 88 ms per query measured through the dply gateway at ~88 ms RTT).
     * PGSQL_ATTR_DISABLE_PREPARES sends the query and its parameters together
     * (PQexecParams): the server still binds the parameters, so typing and
     * injection safety are unchanged. Only when the app did not set it.
     */
    private function oneRoundTripPostgres(): void
    {
        $config = $this->app['config'];
        if ((string) env('DPLY_QUEUE_TOKEN', '') === '') {
            return;
        }
        // PHP 8.4+ names it Pdo\Pgsql::ATTR_DISABLE_PREPARES (the PDO:: one is deprecated in 8.5).
        $attribute = class_exists(Pgsql::class) ? Pgsql::ATTR_DISABLE_PREPARES
            : (defined('PDO::PGSQL_ATTR_DISABLE_PREPARES') ? constant('PDO::PGSQL_ATTR_DISABLE_PREPARES') : null);
        if ($attribute === null) {
            return;
        }
        foreach ((array) $config->get('database.connections', []) as $name => $connection) {
            if (! is_array($connection) || ($connection['driver'] ?? '') !== 'pgsql') {
                continue;
            }
            $options = (array) ($connection['options'] ?? []);
            if (! array_key_exists($attribute, $options)) {
                $options[$attribute] = true;
                $config->set("database.connections.{$name}.options", $options);
            }
        }
    }

    /**
     * On dply, a Redis queue worker waits on the list for a job (BLPOP)
     * instead of asking every few seconds: a job starts the moment it is
     * pushed, and far fewer commands cross the network. Only when the app
     * left block_for unset.
     */
    private function blockOnRedisQueue(): void
    {
        $config = $this->app['config'];
        if ((string) env('DPLY_QUEUE_TOKEN', '') === '' || ! is_array($config->get('queue.connections.redis'))) {
            return;
        }
        if ($config->get('queue.connections.redis.block_for') === null) {
            $config->set('queue.connections.redis.block_for', max(1, (int) env('DPLY_REDIS_BLOCK_FOR', 5)));
        }
    }

    /** Writable by the app and tailed into the app log by the container (EdgeContainerDockerfile). */
    public const SCHEDULE_LOG = '/tmp/dply-app.log';

    /** Touched on every queue loop in a dply queue worker; the container reports it to dply (EdgeContainerDockerfile::WORKER_HEARTBEAT). */
    public const WORKER_HEARTBEAT = '/tmp/dply-worker-heartbeat';

    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->commands([ProbeCommand::class]);
        }

        // A dply queue worker: every loop of queue:work (and Horizon's workers) touches the heartbeat file, so a
        // worker wedged with its container up goes quiet and dply restarts it (observe, 2026-11-12). A job about
        // to start dates the file to when it must be done, so a long job is not taken for a wedge. Returns
        // nothing: a Looping listener returning false pauses the worker.
        if (getenv('DPLY_ROLE') === 'worker' && getenv('DPLY_WORKER_OBJECT') !== false) {
            Event::listen(Looping::class, static function (): void {
                self::beat();
            });
            Event::listen(JobProcessing::class, static function (JobProcessing $event): void {
                self::beat(time() + self::jobTimeout($event->job->timeout()));
            });
        }

        // Laravel runs every scheduled task with '> /dev/null 2>&1' unless it names an output, and the app logs to
        // stderr: whatever a scheduled command printed or logged, its errors included, reached no log (Beacon's
        // monitors:check, 2026-11-13). On dply, send those to SCHEDULE_LOG, which the app log includes.
        Event::listen(CommandStarting::class, function (CommandStarting $event): void {
            if (in_array($event->command, ['schedule:run', 'schedule:work'], true)) {
                self::logScheduledOutput($this->app->make(Schedule::class));
            }
        });

        // On dply the app's Worker is the only thing that reaches the
        // container, over plain HTTP. Trust it (as Laravel Cloud does) so
        // links and assets use the visitor's https host: the custom domain
        // or the dply hostname, whichever the request came in on. An app's
        // own TrustProxies config, when set, still wins.
        if (getenv('DPLY_QUEUE_TOKEN') !== false) {
            Request::setTrustedProxies(['REMOTE_ADDR'], Request::HEADER_X_FORWARDED_FOR | Request::HEADER_X_FORWARDED_HOST | Request::HEADER_X_FORWARDED_PORT | Request::HEADER_X_FORWARDED_PROTO);
            // Laravel 11+'s TrustProxies middleware resets the list above on
            // every request and rebuilds it from this config (or the app's
            // trustProxies(at:)), so without it every link came out http://.
            if (config('trustedproxy.proxies') === null) {
                config(['trustedproxy.proxies' => '*']);
            }
        }

        /** @var QueueManager $manager */
        $manager = $this->app['queue'];
        $manager->addConnector('dply', fn () => new DplyConnector);
        /** @var CacheManager $cache */
        $cache = $this->app['cache'];
        $cache->extend('dply', function ($app, array $config) {
            return $app['cache']->repository(new DplyKvStore((string) ($config['host'] ?? '')));
        });
        Storage::extend('dply', function ($app, array $config) {
            $adapter = new DplyAdapter((string) ($config['host'] ?? ''), isset($config['url']) ? (string) $config['url'] : null);

            return new FilesystemAdapter(new Filesystem($adapter), $adapter, $config);
        });

        // No web middleware: the Worker authenticates with DPLY_QUEUE_TOKEN.
        Route::post('/_dply/queue', QueueController::class)->name('dply.queue');
        Route::post('/_dply/schedule', ScheduleController::class)->name('dply.schedule');
        Route::post('/_dply/command', CommandController::class)->name('dply.command');
    }

    /**
     * One disk per attached bucket. The first is also the s3 disk when the
     * app has no S3 key of its own, so Storage::disk('s3') keeps working.
     */
    private function registerStorageDisks(): void
    {
        $disks = [];
        foreach (explode(',', (string) env('DPLY_STORAGE_DISKS', '')) as $pair) {
            [$name, $host] = array_pad(explode('=', trim($pair), 2), 2, '');
            if ($name !== '' && $host !== '') {
                $disks[$name] = $host;
            }
        }
        $host = (string) env('DPLY_STORAGE_HOST', '');
        $primary = (string) env('DPLY_STORAGE_DISK', '');
        if ($host !== '' && $primary !== '' && ! isset($disks[$primary])) {
            $disks[$primary] = $host;
        }
        // S3 keys from dply (AWS_* plus DPLY_STORAGE_BUCKETS=disk=bucket): each disk is a real s3 disk,
        // so temporaryUrl, uploads straight to the bucket and large files work. Needs Flysystem's S3 adapter.
        $buckets = [];
        foreach (explode(',', (string) env('DPLY_STORAGE_BUCKETS', '')) as $pair) {
            [$name, $bucket] = array_pad(explode('=', trim($pair), 2), 2, '');
            if ($name !== '' && $bucket !== '') {
                $buckets[$name] = $bucket;
            }
        }
        $s3 = $buckets !== [] && class_exists(AwsS3V3Adapter::class);
        foreach ($disks as $name => $diskHost) {
            // The app's own throw/report win: a backup command that set throw => true must see a failed upload.
            $own = array_intersect_key((array) $this->app['config']->get('filesystems.disks.'.$name, []), ['throw' => true, 'report' => true]);
            $this->app['config']->set('filesystems.disks.'.$name, $own + ($s3 && isset($buckets[$name]) ? [
                'driver' => 's3',
                'key' => env('AWS_ACCESS_KEY_ID'),
                'secret' => env('AWS_SECRET_ACCESS_KEY'),
                'region' => env('AWS_DEFAULT_REGION', 'auto'),
                'bucket' => $buckets[$name],
                'endpoint' => env('AWS_ENDPOINT'),
                'use_path_style_endpoint' => true,
                'url' => $name === $primary ? env('AWS_URL') : null,
                'throw' => false,
            ] : [
                'driver' => 'dply',
                'host' => $diskHost,
            ]));
        }
        if ($host !== '' && (string) env('AWS_ACCESS_KEY_ID', '') === '') {
            $this->app['config']->set('filesystems.disks.s3', [
                'driver' => 'dply',
                'host' => $host,
            ]);
        }
        if ($primary !== '' && env('FILESYSTEM_DISK') === null) {
            $this->app['config']->set('filesystems.default', $primary);
        }
    }

    /**
     * One cache store per attached key-value store. CACHE_STORE names the
     * default when Redis is not attached.
     */
    private function registerKvStores(): void
    {
        $stores = [];
        foreach (explode(',', (string) env('DPLY_KV_STORES', '')) as $pair) {
            [$name, $host] = array_pad(explode('=', trim($pair), 2), 2, '');
            if ($name !== '' && $host !== '') {
                $stores[$name] = $host;
            }
        }
        $host = (string) env('DPLY_KV_HOST', '');
        $primary = (string) env('DPLY_KV_STORE', '');
        if ($host !== '' && $primary !== '' && ! isset($stores[$primary])) {
            $stores[$primary] = $host;
        }
        foreach ($stores as $name => $storeHost) {
            $this->app['config']->set('cache.stores.'.$name, [
                'driver' => 'dply',
                'host' => $storeHost,
            ]);
        }
    }

    /**
     * The heartbeat file's mtime is when the worker next owes a sign of life.
     * A loop sets it to now (at most every 10 s), unless another process
     * running a long job already set it later; a job sets it to its deadline.
     */
    public static function beat(?int $until = null, string $file = self::WORKER_HEARTBEAT): void
    {
        static $last = 0;
        clearstatcache(true, $file);
        $mtime = @filemtime($file) ?: 0;
        if ($until !== null) {
            if ($until > $mtime) {
                @touch($file, $until);
            }

            return;
        }
        if (time() - $last >= 10 && $mtime <= time()) {
            $last = time();
            @touch($file);
        }
    }

    /** Seconds a job may run: its own timeout, else the worker's --timeout, else DPLY_WORKER_TIMEOUT; 0 (none) is a day. */
    public static function jobTimeout(?int $job, array $argv = []): int
    {
        $timeout = $job;
        foreach ($timeout === null ? ($argv ?: (array) ($_SERVER['argv'] ?? [])) : [] as $arg) {
            if (preg_match('/^--timeout=(\d+)$/', (string) $arg, $m) === 1) {
                $timeout = (int) $m[1];
            }
        }
        $timeout ??= (int) (getenv('DPLY_WORKER_TIMEOUT') ?: 60);

        return $timeout > 0 ? $timeout : 86400;
    }

    /** Point every scheduled task with no output of its own at SCHEDULE_LOG, when the container provides it. */
    public static function logScheduledOutput(Schedule $schedule, string $log = self::SCHEDULE_LOG): void
    {
        if (! is_file($log) || ! is_writable($log)) {
            return;
        }
        foreach ($schedule->events() as $event) {
            if ($event->output === $event->getDefaultOutput()) {
                $event->appendOutputTo($log);
            }
        }
    }
}
