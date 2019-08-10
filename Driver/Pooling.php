<?php

namespace Yonna\Database\Driver;


use PDO;
use Redis;
use Swoole\Coroutine\Redis as SwRedis;
use Throwable;
use Yonna\Throwable\Exception;

class Pooling
{

    private const MAX = 10;
    private const SEP = '#####';

    private static $pool = [];

    /**
     * create a unique key for pool
     * @param string $dsn
     * @param string $dbType
     * @param array $params
     * @return string
     */
    private static function poolingKey(string $dsn, string $dbType, array $params = []): string
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

    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        self::destruct();
    }

    /**
     * destruct
     */
    public static function destruct()
    {
        if (!empty(self::$pool)) {
            foreach (self::$pool as $poolingKey => $obj) {
                foreach ($obj as $index => $item) {
                    var_dump($poolingKey);
                    var_dump($index);
                    self::pop($poolingKey, $index);
                }
            }
        }
    }


    /**
     * push into stack
     * @param string $poolingKey
     * @param int $index
     * @param $db_type
     * @param $instance
     * @return mixed
     */
    private static function push(string $poolingKey, int $index, $db_type, $instance)
    {
        static::$pool[$poolingKey][$index] = [
            'db_type' => $db_type,
            'instance' => $instance,
            'called' => 1,
        ];
        return $instance;
    }

    /**
     * pop from stack
     * @param string $poolingKey
     * @param int $index
     */
    private static function pop(string $poolingKey, int $index)
    {
        if (!isset(static::$pool[$poolingKey])) {
            return;
        }
        if (!isset(static::$pool[$poolingKey][$index])) {
            return;
        }
        $item = static::$pool[$poolingKey][$index];
        $instance = &$item['instance'];
        switch ($item['db_type']) {
            case Type::MYSQL:
            case Type::PGSQL:
            case Type::MSSQL:
            case Type::SQLITE:
                /**
                 * @var $instance PDO
                 */
                $instance = null;
                break;
            case Type::MONGO:
                break;
            case Type::REDIS:
                /**
                 * @var $instance Redis
                 */
                $instance->close();
                break;
            case Type::REDIS_CO:
                /**
                 * @var $instance SwRedis
                 */
                $instance->close();
                break;
        }
        array_splice(static::$pool[$poolingKey], $index, 1);
    }

    /**
     * pick a useless instance for database driver
     * @param string $poolingKey
     * @return PDO | Redis | SwRedis
     */
    private static function pick(string $poolingKey): object
    {
        if (empty(self::$pool)) {
            return null;
        }
        if (empty(self::$pool[$poolingKey])) {
            return null;
        }
        // start finding
        $i = 0;
        $poolingKeyCalled = 0;
        $instance = null;
        foreach (self::$pool[$poolingKey] as $k => $pool) {
            if ($instance === null || $pool['called'] < $poolingKeyCalled) {
                $instance = $pool['instance'];
                $poolingKeyCalled = $pool['called'];
                $i = $k;
            }
        }
        static::$pool[$poolingKey][$i]['called'] += 1;
    }

    /**
     * malloc
     * @param string $dsn
     * @param string $dbType
     * @param array $params
     * @return mixed
     * @throws null
     */
    public static function malloc(string $dsn, string $dbType, array $params = [])
    {
        if (!isset(static::$pool[$dsn])) {
            static::$pool[$dsn] = [];
        }

        // new instance return the newest one
        $poolingKey = self::poolingKey($dsn, $dbType, $params);
        $count = count(static::$pool[$poolingKey]);
        if ($count < self::MAX) {
            $instance = null;
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
                    case Type::REDIS:
                        if (class_exists('\\Redis')) {
                            try {
                                $instance = new Redis();
                            } catch (\Exception $e) {
                                $instance = null;
                                Exception::database('Redis has some problem or uninstall，Stop it help you application.');
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
                            } catch (\Exception $e) {
                                $instance = null;
                                Exception::database('Swoole Redis has some problem or uninstall，Stop it help you application.');
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
            } catch (Throwable $e) {
                Exception::throw($e->getMessage());
                exit;
            }
            self::push($poolingKey, $count, $dbType, $instance);
        } else {
            $instance = self::pick($poolingKey);
        }
        return $instance;
    }

}