<?php

namespace Yonna\Database\Driver;

use Yonna\Database\DB;
use Yonna\Foundation\System;

class Cache
{

    const FOREVER = 'forever';
    const ONE_MINUTE = '60';
    const FIVE_MINUTE = '300';
    const TEN_MINUTE = '600';
    const HALF_ONE_HOUR = '1800';
    const ONE_HOUR = '3600';
    const ONE_DAY = '86400';
    const ONE_WEEK = '604800';

    const DEFAULT_MINIMUM_TIMEOUT = 10;

    /**
     * get timeout
     * 获取合理的过期时间
     * @param $timeout
     * @return array|bool|false|int|string|null
     */
    private static function timeout(int $timeout): int
    {
        if ($timeout <= 0) return 0;
        $min_timeout = System::env('DB_CACHE_MINIMUM_TIMEOUT') ?? self::DEFAULT_MINIMUM_TIMEOUT;
        if ($timeout < $min_timeout) $timeout = $min_timeout;
        return $timeout;
    }

    public static function get($key)
    {
        return DB::redis('cache')->get($key);
    }

    public static function set($key, $value, int $timeout = 0)
    {
        DB::redis('cache')->set($key, $value, self::timeout($timeout));
    }

    public static function uGet($uniqueCode, $key)
    {
        return DB::redis('cache')->hGet($uniqueCode, $key);
    }

    public static function uSet($uniqueCode, $key, $value)
    {
        DB::redis('cache')->hSet($uniqueCode, $key, $value);
    }

    public static function clear($uniqueCode)
    {
        DB::redis('cache')->delete($uniqueCode);
    }

}