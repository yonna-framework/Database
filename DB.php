<?php


namespace Yonna\Database;

use Yonna\Database\Src\Coupling;
use Yonna\Database\Src\Type;

/**
 * Class DB
 */
class DB
{

    /**
     * @param string $conf
     * @return object|\Yonna\Database\Src\Mongo|\Yonna\Database\Mssql|\Yonna\Database\Mysql|\Yonna\Database\Pgsql|\Yonna\Database\Redis|\Yonna\Database\Sqlite
     */
    public static function connect($conf = 'default')
    {
        return Coupling::connect($conf);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Src\Mysql
     */
    public static function mysql($conf = 'mysql')
    {
        return Coupling::connect($conf, Type::MYSQL);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Src\Pgsql
     */
    public static function pgsql($conf = 'pgsql')
    {
        return Coupling::connect($conf, Type::PGSQL);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Src\Mssql
     */
    public static function mssql($conf = 'mssql')
    {
        return Coupling::connect($conf, Type::MSSQL);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Src\Sqlite
     */
    public static function sqlite($conf = 'sqlite')
    {
        return Coupling::connect($conf, Type::SQLITE);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Src\Mongo
     */
    public static function mongo($conf = 'mongo')
    {
        return Coupling::connect($conf, Type::MONGO);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Src\Redis
     */
    public static function redis($conf = 'redis')
    {
        return Coupling::connect($conf, Type::REDIS);
    }

}
