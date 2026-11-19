<?php

declare(strict_types=1);

namespace Dply\Laravel;

use ErrorException;
use Illuminate\Redis\Connectors\PhpRedisConnector;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use RedisException;

/**
 * phpredis with kept connections (DplyServiceProvider::persistentConnections)
 * hands a request the socket the previous one left open. When the gateway
 * closed it meanwhile (the Valkey slept, or the gateway restarted), the
 * connect's AUTH fails with "read error on connection" and the request is a
 * 500: Laravel retries a lost connection on commands, not on connect. Here a
 * connect that lost its socket tries again, then once more without a kept one.
 * Its connections are DplyPhpRedisConnection: a queue pop that lost its
 * socket reconnects and tries again.
 */
class DplyRedisConnector extends PhpRedisConnector
{
    /** PhpRedisConnector::connect (the same in Laravel 11-13), with the retrying connection. */
    public function connect(array $config, array $options)
    {
        $formattedOptions = Arr::pull($config, 'options', []);
        if (isset($config['prefix'])) {
            $formattedOptions['prefix'] = $config['prefix'];
        }
        $connector = fn () => $this->createClient(array_merge($config, $options, $formattedOptions));

        return new DplyPhpRedisConnection($connector(), $connector, $config);
    }

    /**
     * Connect timeout (TCP + TLS handshake) when the app's config sets none.
     * Stock config/database.php has no `timeout` key, so phpredis waits
     * default_socket_timeout (60 s) on a handshake that stalled (observe,
     * 2026-11-17: the gateway got the ClientHello, then nothing). AUTH and a
     * store's wake are not inside it: they wait on the read timeout.
     */
    public const CONNECT_TIMEOUT = 3.0;

    protected function createClient(array $config)
    {
        // phpredis's max_retries (REDIS_MAX_RETRIES) only reconnects a kept
        // socket it found closed before a command, never a failed connect():
        // the retries here are the only ones a connect gets.
        if (! isset($config['timeout'])) {
            $config['timeout'] = (float) env('DPLY_REDIS_CONNECT_TIMEOUT', self::CONNECT_TIMEOUT);
        }
        foreach ([$config, $config, ['persistent' => false] + $config] as $attempt => $try) {
            try {
                return $this->connectOnce($try);
            } catch (RedisException|ErrorException $e) {
                if ($attempt === 2 || ! self::lostConnection($e)) {
                    throw $e;
                }
                // Jittered, so a fleet of workers cut at once does not reconnect in step.
                usleep(random_int(50_000, 250_000) * ($attempt + 1));
            }
        }
    }

    protected function connectOnce(array $config)
    {
        return parent::createClient($config);
    }

    public static function lostConnection(\Throwable $e): bool
    {
        return Str::contains($e->getMessage(), ['went away', 'socket', 'read error on connection', 'Connection lost', 'Connection reset by peer', 'Error while reading', 'timed out', 'Failed to enable crypto', 'Broken pipe', 'AUTH failed while reconnecting']);
    }
}
