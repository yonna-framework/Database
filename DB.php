<?php


namespace Yonna\Database;

use Closure;
use Throwable;
use Yonna\Database\Driver\Coupling;
use Yonna\Database\Driver\Mongo;
use Yonna\Database\Driver\Mssql;
use Yonna\Database\Driver\Mysql;
use Yonna\Database\Driver\Pgsql;
use Yonna\Database\Driver\Redis;
use Yonna\Database\Driver\Sqlite;
use Yonna\Database\Driver\Type;
use Yonna\Database\Support\Record;
use Yonna\Database\Support\Transaction;
use Yonna\Throwable\Exception;

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
     * @param Closure $call
     * @throws Throwable
     */
    public static function transTrace(Closure $call)
    {
        try {
            Transaction::begin();
            $call();
            Transaction::commit();
        } catch (Throwable $e) {
            Transaction::rollback();
            Exception::origin($e);
        }
    }

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
    public static function rollBackTrans()
    {
        Transaction::rollback();
    }

    /**
     * 检测是否在一个事务内
     * @return bool
     */
    public static function inTrans(): bool
    {
        return Transaction::in();
    }

    /**
     * 当前时间（只能用于insert 和 update）
     * @param string $conf
     * @return array
     */
    public static function now($conf = 'default')
    {
        return self::connect($conf)->now();
    }

    // connector

    /**
     * @param string $conf
     * @return object|Mongo|Mssql|Mysql|Pgsql|Redis|Sqlite
     * @throws null
     */
    public static function connect($conf = 'default')
    {
        return Coupling::connect($conf);
    }

    /**
     * @param string $conf
     * @return Mysql
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
     * @return Pgsql
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
     * @return Mssql
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
     * @return Sqlite
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
     * @return Mongo
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
     * @return Redis
     */
    public static function redis($conf = 'redis')
    {
        if (is_array($conf)) {
            $conf['type'] = Type::REDIS;
        }
        return self::connect($conf);
    }

}
