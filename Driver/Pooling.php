<?php

namespace Yonna\Database\Driver;


use PDO;
use PDOException;
use Yonna\Throwable\Exception;

class Pooling
{

    const MIN = 1;
    const MAX = 10;
    const SEP = '#####';

    private static $pool = [];

    /**
     * free
     * @param string $poolIndex
     */
    public static function free(string $poolIndex)
    {
        $poolIndex = explode(self::SEP, $poolIndex);
        $u = $poolIndex[0];
        $i = $poolIndex[1];
        if (isset(self::$pool[$u]) && isset(self::$pool[$u][$i])) {
            self::$pool[$u][$i]['used'] -= 1;
        }
    }

    /**
     * destroy
     * @param string $poolIndex
     */
    public static function destroy(string $poolIndex)
    {
        $poolIndex = explode(self::SEP, $poolIndex);
        $u = $poolIndex[0];
        $i = $poolIndex[1];
        if (isset(self::$pool[$u])) {
            array_splice(self::$pool[$u], $i, 1);
        }
    }

    /**
     * malloc
     * @param $dsn
     * @return mixed
     * @throws Exception\DatabaseException
     * @throws Exception\ThrowException
     */
    public static function malloc($dsn)
    {
        if (!isset(static::$pool[$dsn])) {
            static::$pool[$dsn] = [];
        }

        // new instance return the newest one
        $count = count(static::$pool[$dsn]);
        if ($count < self::MAX) {
            $link['udi'] = implode(self::SEP, [$dsn, $count]);
            try {
                switch ($this->db_type) {
                    case Type::MYSQL:
                        $this->pdo = new PDO($this->dsn(), $this->account, $this->password,
                            array(
                                PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES ' . $this->charset,
                                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                                PDO::ATTR_STRINGIFY_FETCHES => false,
                                PDO::ATTR_EMULATE_PREPARES => false,
                            )
                        );
                        break;
                    case Type::PGSQL:
                        $this->pdo = new PDO($this->dsn(), $this->account, $this->password,
                            array(
                                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                                PDO::ATTR_STRINGIFY_FETCHES => false,
                                PDO::ATTR_EMULATE_PREPARES => false,
                            )
                        );
                        break;
                    case Type::MSSQL:
                        $this->pdo = new PDO($this->dsn(), $this->account, $this->password,
                            array(
                                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                            )
                        );
                        break;
                    case Type::SQLITE:
                        $this->pdo = new PDO($this->dsn(), null, null,
                            array(
                                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                                PDO::ATTR_STRINGIFY_FETCHES => false,
                                PDO::ATTR_EMULATE_PREPARES => false,
                            )
                        );
                        break;
                    default:
                        Exception::database("{$this->db_type} not support PDO yet");
                        break;
                }
            } catch (PDOException $e) {
                Exception::throw($e->getMessage());
                exit;
            }
            static::$pool[$dsn][$count] = [
                'instance' => $instance,
                'used' => 1,
            ];
        } else {
            // get a instance from pool
            $i = 0;
            $dsnsed = 0;
            $instance = null;
            foreach (static::$pool[$dsn] as $k => $pool) {
                if ($instance === null || $pool['used'] < $dsnsed) {
                    $instance = $pool['instance'];
                    $dsnsed = $pool['used'];
                    $i = $k;
                }
            }
            static::$pool[$dsn][$i]['used'] += 1;
        }
        return $instance;
    }

}