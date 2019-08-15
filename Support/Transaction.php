<?php

namespace Yonna\Database\Support;


use Closure;
use PDO;
use PDOException;
use Redis;
use Swoole\Coroutine\Redis as SwRedis;
use MongoDB\Driver\Manager as MongoDBManager;
use MongoDB\Driver\Session as MongoDBSession;
use Throwable;
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
     * 事务
     * @param Closure $call
     * @return bool
     * @throws Throwable
     */
    public static function transTrace(Closure $call)
    {
        if (empty(self::$instances)) {
            return true;
        }
        try {
            if (self::$transTrace <= 0) {
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

            $call();

            if (self::$transTrace > 0) {
                self::$transTrace -= 1;
            }
            if (self::$transTrace <= 0) {
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

        } catch (Throwable $e) {
            if (self::$transTrace > 0) {
                self::$transTrace -= 1;
            }
            if (self::$transTrace <= 0) {
                foreach (self::$instances as $instance) {
                    if ($instance instanceof PDO) {
                        if ($instance->inTransaction()) {
                            $instance->rollBack();
                        }
                        return false;
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
            Exception::origin($e);
        }
        return true;
    }


    /**
     * 开始事务
     */
    public static function begin()
    {
        if (self::$transTrace <= 0) {
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
        if (self::$transTrace > 0) {
            self::$transTrace -= 1;
        }
        if (self::$transTrace <= 0) {
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

    /**
     * 事务回滚
     */
    public static function rollback()
    {
        if (self::$transTrace > 0) {
            self::$transTrace -= 1;
        }
        if (self::$transTrace <= 0) {
            foreach (self::$instances as $instance) {
                if ($instance instanceof PDO) {
                    if ($instance->inTransaction()) {
                        $instance->rollBack();
                    }
                    return false;
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


}
