<?php


namespace Yonna\Database;

use Yonna\Database\Driver\Coupling;
use Yonna\Database\Driver\Type;
use Yonna\Database\Record\Record;

/**
 * Class DB
 */
class DB
{

    /**
     * 启用记录
     */
    public static function enableRecord()
    {
        Record::enableRecord();
    }

    /**
     * 获取记录
     */
    public static function getRecord()
    {
        return Record::getRecord();
    }

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
        if (is_array($conf)) {
            $conf['type'] = Type::MYSQL;
        }
        return Coupling::connect($conf, Type::MYSQL);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Pgsql
     */
    public static function pgsql($conf = 'pgsql')
    {
        if (is_array($conf)) {
            $conf['type'] = Type::PGSQL;
        }
        return Coupling::connect($conf, Type::PGSQL);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Mssql
     */
    public static function mssql($conf = 'mssql')
    {
        if (is_array($conf)) {
            $conf['type'] = Type::MSSQL;
        }
        return Coupling::connect($conf, Type::MSSQL);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Sqlite
     */
    public static function sqlite($conf = 'sqlite')
    {
        if (is_array($conf)) {
            $conf['type'] = Type::SQLITE;
        }
        return Coupling::connect($conf, Type::SQLITE);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Mongo
     */
    public static function mongo($conf = 'mongo')
    {
        if (is_array($conf)) {
            $conf['type'] = Type::MONGO;
        }
        return Coupling::connect($conf, Type::MONGO);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Redis
     */
    public static function redis($conf = 'redis')
    {
        if (is_array($conf)) {
            $conf['type'] = Type::REDIS;
        }
        return Coupling::connect($conf, Type::REDIS);
    }

}
