<?php

namespace Yonna\Database\Driver;

use Yonna\Database\Config;
use Yonna\Throwable\Exception;

class Coupling
{

    private static $config = null;
    private static $coupler = [];
    private static $transTrace = [];

    /**
     * 连接数据库
     * @param string | array $conf
     * @return Mysql|Pgsql|Mssql|Sqlite|Mongo|Redis
     * @throws Exception\ParamsException
     */
    public static function connect($conf = 'default'): object
    {
        if (static::$config === null) {
            static::$config = Config::fetch();
            $dbKeys = array_keys(static::$config);
            array_walk($dbKeys, function ($key) {
                static::$transTrace[strtoupper($key)] = 0;
            });
        }
        if (is_string($conf)) {
            $conf = static::$config[$conf];
        }
        $link = [];
        if (is_array($conf)) {
            foreach ($conf as $ck => $cv) {
                $link[$ck] = $cv ?? null;
            }
        }

        if (empty($link['type'])) {
            Exception::params('Lack type of database');
        }
        if (empty($link['host']) || empty($link['port'])) {
            Exception::params('Lack of host/port address');
        }
        if (!in_array($link['type'], Type::array())) {
            Exception::params('Error type for database');
        }

        $u = crc32(serialize($link));
        if (!isset(static::$coupler[$u])) {
            $driver = "\\Yonna\\Database\\Driver\\{$link['type']}";
            static::$coupler[$u] = new $driver($link);
        }
        return static::$coupler[$u];
    }

}