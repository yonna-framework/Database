<?php

namespace Yonna\Database\Support;


use PDO;
use PDOException;
use Redis;
use Swoole\Coroutine\Redis as SwRedis;
use Yonna\Database\Driver\Mdo\Client as MongoClient;
use Yonna\Throwable\Exception;

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
     * @param $instance
     */
    private static function start($instance)
    {
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
        } elseif ($instance instanceof MongoClient) {
            if ($instance->isReplica()) {
                $instance->setSession($instance->getManager()->startSession());
                $instance->getSession()->startTransaction([]);
            }
        } elseif ($instance instanceof Redis) {
            $instance->multi(Redis::MULTI);
        } elseif ($instance instanceof SwRedis) {
            $instance->multi();
        }
    }


    /**
     * 检测是否在一个事务内
     * @return bool
     */
    public static function in()
    {
        return self::$transTrace > 0;
    }


    /**
     * 开始事务
     */
    public static function begin()
    {
        if (!self::in()) {
            self::$transTrace = 1;
            foreach (self::$instances as $instance) {
                self::start($instance);
            }
        } else {
            self::$transTrace += 1;
        }
    }

    /**
     * 注册实例
     * @param $instance
     * @throws Exception\DatabaseException
     */
    public static function register($instance)
    {
        if (!$instance instanceof PDO
            && !$instance instanceof Redis
            && !$instance instanceof SwRedis
            && !$instance instanceof MongoClient) {
            Exception::database('instance error');
        }
        if (!in_array($instance, self::$instances)) {
            self::$instances[] = $instance;
            // if in transaction, auto start when register
            if (self::in()) {
                self::start($instance);
            }
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
                } elseif ($instance instanceof MongoClient) {
                    if ($instance->isReplica()) {
                        $instance->getSession()->commitTransaction();
                        $instance->getSession()->endSession();
                        $instance->setSession(null);
                    }
                } elseif ($instance instanceof Redis) {
                    $instance->exec();
                } elseif ($instance instanceof SwRedis) {
                    $instance->exec();
                }
            }
        }
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
                } elseif ($instance instanceof MongoClient) {
                    if ($instance->isReplica()) {
                        $instance->getSession()->abortTransaction();
                        $instance->getSession()->endSession();
                        $instance->setSession(null);
                    }
                } elseif ($instance instanceof Redis) {
                    $instance->discard();
                } elseif ($instance instanceof SwRedis) {
                    $instance->discard();
                }
            }
        }
    }

}
