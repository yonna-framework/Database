<?php


namespace Yonna\Database;

use PDOException;
use Yonna\Database\Driver\Coupling;
use Yonna\Database\Driver\Type;
use Yonna\Database\Support\Record;

/**
 * Class DB
 */
class DB
{

    /**
     * 启用记录
     * @param string|array $dbType
     * @see Type
     */
    public static function enableRecord($dbType = null)
    {
        Record::enableRecord($dbType);
    }

    /**
     * 获取记录
     */
    public static function getRecord()
    {
        return Record::getRecord();
    }

    /**
     * 开始事务
     */
    public function beginTrans()
    {
        if ($this->transTrace <= 0) {
            if ($this->pdo()->inTransaction()) {
                $this->pdo()->commit();
            }
            $this->transTrace = 1;
        } else {
            $this->transTrace++;
            return true;
        }
        try {
            return $this->pdo()->beginTransaction();
        } catch (PDOException $e) {
            // 服务端断开时重连一次
            if ($e->errorInfo[1] == 2006 || $e->errorInfo[1] == 2013) {
                $this->pdoClose();
                return $this->pdo()->beginTransaction();
            } else {
                throw $e;
            }
        }
    }

    /**
     * 提交事务
     */
    public function commitTrans()
    {
        $this->transTrace > 0 && $this->transTrace--;
        if ($this->transTrace > 0) {
            return true;
        }
        return $this->pdo()->commit();
    }

    /**
     * 事务回滚
     */
    public function rollBackTrans()
    {
        $this->transTrace > 0 && $this->transTrace--;
        if ($this->transTrace > 0) {
            return true;
        }
        if ($this->pdo()->inTransaction()) {
            return $this->pdo()->rollBack();
        }
        return false;
    }

    /**
     * 检测是否在一个事务内
     * @return bool
     */
    public function inTransaction()
    {
        return $this->pdo()->inTransaction();
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
