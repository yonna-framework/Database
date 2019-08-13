<?php

namespace Yonna\Database\Support;


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
    protected static $transTrace = 0;


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
