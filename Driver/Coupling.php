<?php

namespace Yonna\Database\Driver;

use Yonna\Core;
use Yonna\Database\Config;
use Yonna\Throwable\Exception;

class Coupling
{

    private static $config = null;
    private static $db = array();
    private static $transTrace = array();

    /**
     * 连接数据库
     * @param string | array $conf
     * @param null $mustDbType db type check
     * @return Mysql|Pgsql|Mssql|Sqlite|Mongo|Redis
     */
    public static function connect($conf = 'default', $mustDbType = null): object
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
        if (empty($link['type'])) Exception::throw('Lack type of database');
        if ($mustDbType && $mustDbType !== $link['type']) Exception::throw('src type check no pass');
        if (empty($link['host']) || empty($link['port'])) Exception::throw('Lack of host/port address');
        $u = md5(var_export($link, true));
        if (empty(static::$db[$u])) {
            static::$db[$u] = Core::singleton("\\Yonna\\Database\\Driver\\{$link['type']}", $link);
        }
        return static::$db[$u];
    }

}