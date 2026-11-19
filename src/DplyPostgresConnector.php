<?php

declare(strict_types=1);

namespace Dply\Laravel;

use Illuminate\Database\Connectors\PostgresConnector;

/**
 * Skips Laravel's `set search_path to "public"` on a dply Postgres: one
 * round trip on every connect, which through the Database pooler is the
 * whole cost of connecting (~1 ms without it, one round trip with it,
 * measured). The server's default search path ("$user", public) already
 * resolves to public there. The app's config keeps its value, so the schema
 * builder is unchanged.
 *
 * ponytail: assumes no schema named after the database user (`app`); if
 * one is ever created it would come first. Set the role's default
 * (ALTER ROLE … SET search_path = public) at provisioning if that matters.
 */
class DplyPostgresConnector extends PostgresConnector
{
    /** @param  array<string, mixed>  $config */
    public static function skipsSearchPath(array $config): bool
    {
        $host = $config['host'] ?? '';
        if (($config['search_path'] ?? $config['schema'] ?? null) !== 'public' || ! is_string($host)) {
            return false;
        }

        return str_ends_with($host, '.db.dply.io')
            || ($host === '127.0.0.1' && (string) ($config['port'] ?? '') === '6432' && getenv('DPLY_DB_POOLER') === '1');
    }

    protected function configureSearchPath($connection, $config)
    {
        if (! self::skipsSearchPath($config)) {
            parent::configureSearchPath($connection, $config);
        }
    }
}
