<?php


namespace Yonna\Database;

use Yonna\Database\Driver\Coupling;
use Yonna\Database\Driver\Type;

/**
 * Class DB
 */
class DB
{

    /**
     * @param string $conf
     * @return object|\Yonna\Database\Driver\Mongo|\Yonna\Database\Driver\Mssql|\Yonna\Database\Driver\Mysql|\Yonna\Database\Driver\Pgsql|\Yonna\Database\Driver\Redis|\Yonna\Database\Driver\Sqlite
     */
    public static function connect($conf = 'default')
    {
        return Coupling::connect($conf);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Mysql
     */
    public static function mysql($conf = 'mysql')
    {
        return Coupling::connect($conf, Type::MYSQL);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Pgsql
     */
    public static function pgsql($conf = 'pgsql')
    {
        return Coupling::connect($conf, Type::PGSQL);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Mssql
     */
    public static function mssql($conf = 'mssql')
    {
        return Coupling::connect($conf, Type::MSSQL);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Sqlite
     */
    public static function sqlite($conf = 'sqlite')
    {
        return Coupling::connect($conf, Type::SQLITE);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Mongo
     */
    public static function mongo($conf = 'mongo')
    {
        return Coupling::connect($conf, Type::MONGO);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Redis
     */
    public static function redis($conf = 'redis')
    {
        return Coupling::connect($conf, Type::REDIS);
    }

}
