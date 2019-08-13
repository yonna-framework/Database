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
     * uuid
     * @var null
     */
    private $uuid = null;

    /**
     * transaction object
     * @var Transaction
     */
    private $transaction = null;

    /**
     * record object
     * @var Record
     */
    private $record = null;

    /**
     * new one
     * @return DB
     */
    public static function new()
    {
        return (new self());
    }

    /**
     * DB constructor.
     * the transaction is auto open,it will push driver into the stack
     */
    public function __construct()
    {
        $this->uuid = sha1(microtime() . $_SERVER['HTTP_USER_AGENT']);
        $this->transaction = (new Transaction());
        $this->record = (new Record());
    }

    // uuid

    /**
     * uuid
     */
    public static function uuid()
    {
        return $_ENV['UUID'] ?? $_SERVER['HTTP_USER_AGENT'] ?? 0;
    }

    // database record

    /**
     * enable record feature
     */
    public function startRecord()
    {
        $this->record->clear();
        $this->record->setEnable(true);
    }

    /**
     * 获取记录
     * @param string|array $dbType
     * @return array
     * @see Type
     */
    public function fetchRecord($dbType = null)
    {
        return $this->record->fetch($dbType);
    }

    // transaction

    /**
     * trans start
     */
    public function beginTrans()
    {
        $this->transaction->begin();
    }

    /**
     * trans commit
     */
    public function commitTrans()
    {
        $this->transaction->commit();
    }

    /**
     * trans rollback
     */
    public function rollBackTrans()
    {
        $this->transaction->rollback();
    }

    /**
     * 检测是否在一个事务内
     * @return bool
     */
    public function inTrans(): bool
    {
        return $this->transaction->in();
    }

    // connector

    /**
     * @param string $conf
     * @return object|\Yonna\Database\Driver\Mongo|\Yonna\Database\Driver\Mssql|\Yonna\Database\Driver\Mysql|\Yonna\Database\Driver\Pgsql|\Yonna\Database\Driver\Redis|\Yonna\Database\Driver\Sqlite
     */
    public function connect($conf = 'default')
    {
        return Coupling::connect(
            $conf,
            $this->uuid,
            $this->transaction,
            $this->record
        );
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Mysql
     */
    public function mysql($conf = 'mysql')
    {
        if (is_array($conf)) {
            $conf['type'] = Type::MYSQL;
        }
        return $this->connect($conf);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Pgsql
     */
    public function pgsql($conf = 'pgsql')
    {
        if (is_array($conf)) {
            $conf['type'] = Type::PGSQL;
        }
        return $this->connect($conf);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Mssql
     */
    public function mssql($conf = 'mssql')
    {
        if (is_array($conf)) {
            $conf['type'] = Type::MSSQL;
        }
        return $this->connect($conf);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Sqlite
     */
    public function sqlite($conf = 'sqlite')
    {
        if (is_array($conf)) {
            $conf['type'] = Type::SQLITE;
        }
        return $this->connect($conf);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Mongo
     */
    public function mongo($conf = 'mongo')
    {
        if (is_array($conf)) {
            $conf['type'] = Type::MONGO;
        }
        return $this->connect($conf);
    }

    /**
     * @param string $conf
     * @return \Yonna\Database\Driver\Redis
     */
    public function redis($conf = 'redis')
    {
        if (is_array($conf)) {
            $conf['type'] = Type::REDIS;
        }
        return $this->connect($conf);
    }

}
