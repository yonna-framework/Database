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

    private static $mallocIndex = [];
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
     * @param int $i
     * @return array
     */
    private static function pick(string $poolingKey, int $i = -9235): array
    {
        if (empty(self::$pool)) {
            return [null, -9235];
        }
        if (empty(self::$pool[$poolingKey])) {
            return [null, -9235];
        }
        // start finding
        if ($i === -9235) {
            $i = 0;
            $poolingKeyCalled = 0;
            $instance = null;
            foreach (self::$pool[$poolingKey] as $k => $pool) {
                if ($instance === null || $pool['called'] < $poolingKeyCalled) {
                    $poolingKeyCalled = $pool['called'];
                    $i = $k;
                }
            }
        }
        $instance = static::$pool[$poolingKey][$i]['instance'];
        static::$pool[$poolingKey][$i]['called'] += 1;
        return [$instance, $i];
    }

    /**
     * malloc
     * @param array $params
     * @return mixed
     * @throws null
     */
    public static function malloc(array $params = [])
    {
        $uuid = $params['uuid'];
        $dsn = $params['dsn'];
        $dbType = $params['db_type'];

        $poolingKey = self::poolingKey($dsn, $dbType, $params);

        if (isset(static::$mallocIndex[$uuid])) {
            $picker = self::pick($poolingKey, static::$mallocIndex[$uuid]);
            if (!empty($picker[0])) {
                return $picker[0];
            }
            unset(static::$mallocIndex[$uuid]);
        }

        if (!isset(static::$pool[$dsn])) {
            static::$pool[$dsn] = [];
        }

        // new instance return the newest one
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
            $index = $count;
        } else {
            $picker = self::pick($poolingKey);
            $instance = $picker[0];
            $index = $picker[1];
        }
        static::$mallocIndex[$uuid] = $index;
        return $instance;
    }

}