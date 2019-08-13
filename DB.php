<?php


namespace Yonna\Database;

use Yonna\Database\Driver\Coupling;
use Yonna\Database\Driver\Type;
use Yonna\Database\Support\Record;
use Yonna\Database\Support\Transaction;

/**
 * Class DB
 */
class DB
{

    /**
     * enable record feature
     */
    public static function startRecord()
    {
        Record::clear();
        Record::setEnable(true);
    }

    /**
     * 获取记录
     * @param string|array $dbType
     * @return array
     * @see Type
     */
    public static function fetchRecord($dbType = null)
    {
        return Record::fetch($dbType);
    }

    // transaction

    /**
     * trans start
     */
    public static function beginTrans()
    {
        Transaction::begin();
    }

    /**
     * trans commit
     */
    public static function commitTrans()
    {
        Transaction::commit();
    }

    /**
     * trans rollback
     */
    public function rollBackTrans()
    {
        Transaction::rollback();
    }

    /**
     * 检测是否在一个事务内
     * @return bool
     */
    public function inTrans(): bool
    {
        return Transaction::in();
    }

    // connector

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
        return self::connect($conf);
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
        return self::connect($conf);
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
        return self::connect($conf);
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
        return self::connect($conf);
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
        return self::connect($conf);
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
        return self::connect($conf);
    }

}
