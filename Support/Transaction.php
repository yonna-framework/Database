<?php

namespace Yonna\Database\Support;


use Closure;
use PDO;
use Yonna\Database\Driver\Type;

/**
 * 事务
 * Class Transaction
 * @package Yonna\Database\Support
 */
class Transaction extends Support
{

    /**
     * 多重嵌套事务处理堆栈
     */
    private static $transTrace = 0;


    /**
     * dbo 实例
     * [
     *      type => Mysql | Pgsql | Mssql | Sqlite | Mongo | Redis
     *      instance => object
     * ]
     * @var array
     */
    private static $instances = [];

    /**
     * 获取 instance
     * @param $item
     * @return PDO
     */
    private static function getInstances($item)
    {
        return $item['instance'];
    }


    /**
     * 事务
     * @param Closure $call
     * @return bool
     */
    public static function transTrace(Closure $call)
    {
        if (empty(self::$instances)) {
            return true;
        }
        if (self::$transTrace <= 0) {
            self::$transTrace = 1;
            foreach (self::$instances as $tran) {
                switch ($tran['db_type']) {
                    case Type::MONGO:
                        break;
                    case Type::REDIS:
                        break;
                    case Type::REDIS_CO:
                        break;
                    case Type::MYSQL:
                    case Type::PGSQL:
                    case Type::MSSQL:
                    case Type::SQLITE:
                    default:
                        $instance = self::getInstances($tran['instance']);
                        if ($instance->inTransaction()) {
                            $instance->commit();
                        }
                        $instance->beginTransaction();
                        break;
                }
            }
        } else {
            self::$transTrace += 1;
        }
        $call();
    }


    /**
     * 开始事务
     */
    public static function begin()
    {
        if (self::$transTrace <= 0) {
            if (self::$pdo()->inTransaction()) {
                self::$pdo()->commit();
            }
            self::$transTrace = 1;
        } else {
            self::$transTrace++;
            return true;
        }
        try {
            return self::$pdo()->beginTransaction();
        } catch (PDOException $e) {
            // 服务端断开时重连一次
            if ($e->errorInfo[1] == 2006 || $e->errorInfo[1] == 2013) {
                self::$pdoClose();
                return self::$pdo()->beginTransaction();
            } else {
                throw $e;
            }
        }
    }

    /**
     * 提交事务
     */
    public static function commit()
    {
        self::$transTrace > 0 && self::$transTrace--;
        if (self::$transTrace > 0) {
            return true;
        }
        return self::$pdo()->commit();
    }

    /**
     * 事务回滚
     */
    public static function rollback()
    {
        self::$transTrace > 0 && self::$transTrace--;
        if (self::$transTrace > 0) {
            return true;
        }
        if (self::$pdo()->inTransaction()) {
            return self::$pdo()->rollBack();
        }
        return false;
    }

    /**
     * 检测是否在一个事务内
     * @return bool
     */
    public static function in()
    {
        return self::$transTrace > 0;
    }

}
