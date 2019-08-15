<?php

namespace Yonna\Database\Support;


use PDO;
use PDOException;
use Redis;
use Swoole\Coroutine\Redis as SwRedis;
use MongoDB\Driver\Manager as MongoDBManager;
use MongoDB\Driver\Session as MongoDBSession;

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
     * @var array
     */
    private static $instances = [];

    /**
     * mongodb session
     * @var MongoDBSession
     */
    private static $mongodbSession = null;

    /**
     * 检测是否在一个事务内
     * @return bool
     */
    public static function in()
    {
        return self::$transTrace > 0;
    }

    /**
     * 注册实例
     * @param $instance
     */
    public static function register($instance)
    {
        if (!$instance instanceof PDO
            && !$instance instanceof Redis
            && !$instance instanceof SwRedis
            && !$instance instanceof MongoDBManager) {
            
        }
        self::$instances[] = $instance;
    }

    /**
     * 事务回滚
     */
    public static function rollback()
    {
        if (empty(self::$instances)) {
            return;
        }
        if (self::in()) {
            self::$transTrace -= 1;
        }
        if (!self::in()) {
            foreach (self::$instances as $instance) {
                if ($instance instanceof PDO) {
                    if ($instance->inTransaction()) {
                        $instance->rollBack();
                    }
                } elseif ($instance instanceof MongoDBManager) {
                    self::$mongodbSession->abortTransaction();
                    self::$mongodbSession = null;
                } elseif ($instance instanceof Redis) {
                    $instance->discard();
                } elseif ($instance instanceof SwRedis) {
                    $instance->discard();
                }
            }
        }
    }

    /**
     * 开始事务
     */
    public static function begin()
    {
        if (empty(self::$instances)) {
            return;
        }
        if (!self::in()) {
            self::$transTrace = 1;
            foreach (self::$instances as $instance) {
                if ($instance instanceof PDO) {
                    if ($instance->inTransaction()) {
                        $instance->commit();
                    }
                    try {
                        $instance->beginTransaction();
                    } catch (PDOException $e) {
                        // 服务端断开时重连 1 次
                        if ($e->errorInfo[1] == 2006 || $e->errorInfo[1] == 2013) {
                            $instance->beginTransaction();
                        } else {
                            throw $e;
                        }
                    }
                } elseif ($instance instanceof MongoDBManager) {
                    self::$mongodbSession = $instance->startSession();
                    self::$mongodbSession->startTransaction([]);
                } elseif ($instance instanceof Redis) {
                    $instance->multi();
                } elseif ($instance instanceof SwRedis) {
                    $instance->multi();
                }
            }
        } else {
            self::$transTrace += 1;
        }
    }

    /**
     * 提交事务
     */
    public static function commit()
    {
        if (empty(self::$instances)) {
            return;
        }
        if (self::in()) {
            self::$transTrace -= 1;
        }
        if (!self::in()) {
            foreach (self::$instances as $instance) {
                if ($instance instanceof PDO) {
                    $instance->commit();
                } elseif ($instance instanceof MongoDBManager) {
                    self::$mongodbSession->commitTransaction();
                    self::$mongodbSession = null;
                } elseif ($instance instanceof Redis) {
                    $instance->exec();
                } elseif ($instance instanceof SwRedis) {
                    $instance->exec();
                }
            }
        }
    }


}
