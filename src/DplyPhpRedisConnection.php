<?php

declare(strict_types=1);

namespace Dply\Laravel;

use ErrorException;
use Illuminate\Queue\LuaScripts;
use Illuminate\Redis\Connections\PhpRedisConnection;
use RedisException;

/**
 * A queue worker spends its time in BLPOP on the queue's notify list. When the
 * gateway restarts or the connection dies under it, stock Laravel swaps in a
 * new client and rethrows "read error on connection" (Laravel 11, 12 and
 * early 13 retry nothing; later 13 only plain reads, never BLPOP or EVAL), so
 * a gateway roll logs an exception from every worker. Here the queue's own
 * reads are retried once on the new connection: BLPOP/BRPOP (they wait for a
 * signal; a lost one only means the next pop looks), the pop script and the
 * expired-jobs migration. Writes are never retried: one may already have run.
 */
class DplyPhpRedisConnection extends PhpRedisConnection
{
    public function command($method, array $parameters = [])
    {
        $client = $this->client;
        try {
            return parent::command($method, $parameters);
        } catch (RedisException|ErrorException $e) {
            if (! $this->retriesOnce((string) $method, $parameters) || ! DplyRedisConnector::lostConnection($e)) {
                throw $e;
            }
            // Laravel already reconnected when it knew the message; otherwise do it here.
            if ($this->client === $client && $this->connector) {
                try {
                    $this->client->close();
                } catch (\Throwable) {
                }
                $this->client = call_user_func($this->connector);
            }

            return parent::command($method, $parameters);
        }
    }

    // ponytail: if the first pop ran and only its reply was lost, that job waits
    // in :reserved until retry_after and runs then (attempts + 1): late, never twice at once.
    private function retriesOnce(string $method, array $parameters): bool
    {
        return match (strtolower($method)) {
            'blpop', 'brpop' => true,
            'eval' => in_array($parameters[0] ?? null, [LuaScripts::pop(), LuaScripts::migrateExpiredJobs()], true),
            default => false,
        };
    }
}
