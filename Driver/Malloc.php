<?php

namespace Yonna\Database\Driver;


use PDO;
use MongoDB\Driver\Manager as MongoManager;
use Redis;
use Swoole\Coroutine\Redis as SwRedis;
use Throwable;
use Yonna\Database\Driver\Mongo\Client as MongoClient;
use Yonna\Database\Support\Transaction;
use Yonna\Throwable\Exception;

class Malloc
{

    private static $malloc = [];

    /**
     * create a unique key for pool
     * @param string $dsn
     * @param string $dbType
     * @param array $params
     * @return string
     */
    private static function key(string $dsn, string $dbType, array $params = []): string
    {
        $key = $dsn . $dbType;
        if ($params) {
            ksort($params);
            foreach ($params as $k => $v) {
                $key .= $k . $v;
            }
        }
        return $key;
    }

    public static function tempAlloc(array $params = [])
    {

    }

    /**
     * malloc
     * @param array $params
     * @return mixed
     * @throws null
     */
    public static function allocation(array $params = [])
    {

        $dsn = $params['dsn'];
        $dbType = $params['db_type'];

        $key = self::key($dsn, $dbType, $params);
        $instance = null;

        if (!empty(static::$malloc[$key])) {
            $instance = static::$malloc[$key];
        } else {
            try {
                switch ($dbType) {
                    case Type::MYSQL:
                        $instance = new PDO($dsn, $params['account'], $params['password'],
                            array(
                                PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES ' . $params['charset'],
                                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                                PDO::ATTR_STRINGIFY_FETCHES => false,
                                PDO::ATTR_EMULATE_PREPARES => false,
                            )
                        );
                        break;
                    case Type::PGSQL:
                        $instance = new PDO($dsn, $params['account'], $params['password'],
                            array(
                                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                                PDO::ATTR_STRINGIFY_FETCHES => false,
                                PDO::ATTR_EMULATE_PREPARES => false,
                            )
                        );
                        break;
                    case Type::MSSQL:
                        $instance = new PDO($dsn, $params['account'], $params['password'],
                            array(
                                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                            )
                        );
                        break;
                    case Type::SQLITE:
                        $instance = new PDO($dsn, null, null,
                            array(
                                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                                PDO::ATTR_STRINGIFY_FETCHES => false,
                                PDO::ATTR_EMULATE_PREPARES => false,
                            )
                        );
                        break;
                    case Type::MONGO:
                        if (class_exists('\\MongoDB\Driver\Manager')) {
                            try {
                                $instance = new MongoClient();
                                $instance->setManager(new MongoManager($dsn));
                            } catch (Throwable $e) {
                                $instance = null;
                                Exception::database('MongoDB manager has some problem or uninstall,Stop it help you application');
                            }
                        }
                        break;
                    case Type::REDIS:
                        if (class_exists('\\Redis')) {
                            try {
                                $instance = new Redis();
                            } catch (Throwable $e) {
                                $instance = null;
                                Exception::database('Redis has some problem or uninstall,Stop it help you application.');
                            }
                            $instance->connect(
                                $params['host'],
                                $params['port']
                            );
                            if ($params['password']) {
                                $instance->auth($params['password']);
                            }
                        }
                        break;
                    case Type::REDIS_CO:
                        if (class_exists('SwRedis')) {
                            try {
                                $instance = new SwRedis();
                            } catch (Throwable $e) {
                                $instance = null;
                                Exception::database('Swoole Redis has some problem or uninstall,Stop it help you application.');
                            }
                            $instance->connect(
                                $params['host'],
                                $params['port']
                            );
                            if ($params['password']) {
                                $instance->auth($params['password']);
                            }
                        }
                        break;
                    default:
                        Exception::database("{$dbType} not support pooling yet");
                        break;
                }
                Transaction::register($instance);
            } catch (Throwable $e) {
                Exception::throw($e->getMessage());
            }
            static::$malloc[$key] = $instance;
        }
        return $instance;
    }

}