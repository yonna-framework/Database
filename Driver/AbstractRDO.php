<?php

namespace Yonna\Database\Driver;

use Redis;
use Swoole\Coroutine\Redis as SwRedis;
use Yonna\Database\Support\Transaction;
use Yonna\Throwable\Exception;

abstract class AbstractRDO extends AbstractDB
{

    protected $db_type = Type::REDIS;

    const TYPE_OBJ = 'o';
    const TYPE_STR = 's';
    const TYPE_NUM = 'n';

    const READ_COMMAND = ['time', 'dbsize', 'info', 'exists', 'get', 'mget', 'hget'];

    /**
     * 架构函数 取得模板对象实例
     * @access public
     * @param array $setting
     */
    public function __construct(array $setting)
    {
        parent::__construct($setting);
    }

    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        parent::__destruct();
    }

    /**
     * @param $key
     */
    private function parseKey(&$key)
    {
        if (is_string($key)) {
            $key = addslashes($key);
        } else if (is_array($key)) {
            foreach ($key as &$k) {
                $this->parseKey($k);
            }
        }
    }

    /**
     * 获取 RDO
     * @return Redis | SwRedis
     */
    protected function rdo()
    {
        return $this->malloc();
    }


    /**
     * 设置执行命令
     * @param $command
     * @param mixed ...$options
     * @return mixed
     */
    protected function query($command, ...$options)
    {
        $queryResult = null;
        $commandStr = "un know command";
        switch ($command) {
            case 'select':
                $index = $options[0];
                $this->rdo()->select($index);
                $commandStr = "SELECT {$index}";
                break;
            case 'time':
                $queryResult = $this->rdo()->time();
                $commandStr = 'TIME';
                break;
            case 'dbsize':
                $queryResult = $this->rdo()->dbSize();
                $commandStr = 'DBSIZE';
                break;
            case 'bgrewriteaof':
                $this->rdo()->bgrewriteaof();
                $commandStr = 'BGREWRITEAOF';
                break;
            case 'save':
                $this->rdo()->save();
                $commandStr = 'SAVE';
                break;
            case 'bgsave':
                switch ($this->db_type) {
                    case Type::REDIS:
                        $this->rdo()->bgsave();
                        break;
                    case Type::REDIS_CO:
                        $this->rdo()->bgSave();
                        break;
                }
                $commandStr = 'BGSAVE';
                break;
            case 'lastsave':
                $this->rdo()->lastSave();
                $commandStr = 'LASTSAVE';
                break;
            case 'flushall':
                $this->rdo()->flushAll();
                $commandStr = 'FLUSHALL';
                break;
            case 'flushdb':
                $this->rdo()->flushDB();
                $commandStr = 'FLUSHDB';
                break;
            case 'info':
                $section = $options[0];
                switch ($this->db_type) {
                    case Type::REDIS:
                        $queryResult = $this->rdo()->info($section);
                        break;
                    case Type::REDIS_CO:
                        $queryResult = '';
                        break;
                }
                $commandStr = "INFO '{$section}'";
                break;
            case 'delete':
                $key = $options[0];
                $this->parseKey($key);
                $this->rdo()->delete($key);
                $commandStr = "DELETE '{$key}'";
                break;
            case 'ttl':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $this->rdo()->ttl($key);
                $commandStr = "TTL '{$key}'";
                break;
            case 'pttl':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $this->rdo()->pttl($key);
                $commandStr = "PTTL '{$key}'";
                break;
            case 'exists':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $this->rdo()->exists($key) == 1 ? true : false;
                $commandStr = "EXISTS '{$key}'";
                break;
            case 'expire':
                $key = $options[0];
                $this->parseKey($key);
                $this->rdo()->expire($key, $options[1]);
                $commandStr = "EXPIRE '{$key}' '{$options[1]}'";
                break;
            case 'set':
                $key = $options[0];
                $this->parseKey($key);
                $value = $options[1] . $options[2];
                $this->rdo()->set($key, $value);
                $commandStr = "SET '{$key}' '{$value}'";
                break;
            case 'setex':
                $key = $options[0];
                $this->parseKey($key);
                $value = $options[1] . $options[2];
                $ttl = $options[3];
                switch ($this->db_type) {
                    case Type::REDIS:
                        $this->rdo()->setex($key, $ttl, $value);
                        break;
                    case Type::REDIS_CO:
                        $this->rdo()->setEx($key, $ttl, $value);
                        break;
                }
                $commandStr = "SETEX '{$key}' {$ttl} '{$value}'";
                break;
            case 'psetex':
                $key = $options[0];
                $this->parseKey($key);
                $value = $options[1] . $options[2];
                $ttl = $options[3];
                switch ($this->db_type) {
                    case Type::REDIS:
                        $this->rdo()->psetex($key, $ttl, $value);
                        break;
                    case Type::REDIS_CO:
                        $this->rdo()->psetEx($key, $ttl, $value);
                        break;
                }
                $commandStr = "PSETEX '{$key}' {$ttl} '{$value}'";
                break;
            case 'get':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $this->rdo()->get($key);
                $commandStr = "GET '{$key}'";
                break;
            case 'mset':
                $key = $options[0];
                $this->parseKey($key);
                $ttl = $options[1];
                switch ($this->db_type) {
                    case Type::REDIS:
                        $queryResult = $this->rdo()->mset($key);
                        break;
                    case Type::REDIS_CO:
                        $queryResult = $this->rdo()->mSet($key);
                        break;
                }
                foreach ($key as $k => $v) {
                    if ($ttl > 0) {
                        $this->query('expire', $k, $ttl);
                    }
                    $key[$k] = "'{$k}' '{$v}'";
                }
                $commandStr = "MSET " . implode(' ', $key);
                break;
            case 'mget':
                $key = $options[0];
                $this->parseKey($key);
                switch ($this->db_type) {
                    case Type::REDIS:
                        $queryResult = $this->rdo()->mget($key);
                        break;
                    case Type::REDIS_CO:
                        $queryResult = $this->rdo()->mGet($key);
                        break;
                }
                $key = array_map(function ($k) {
                    return "'{$k}'";
                }, $key);
                $commandStr = "MGET " . implode(' ', $key);
                break;
            case 'hset':
                $key = $options[0];
                $this->parseKey($key);
                $hashKey = $options[1];
                $value = $options[2] . $options[3];
                $this->rdo()->hSet($key, $hashKey, $value);
                $commandStr = "HSET '{$key}' '$hashKey' '{$value}'";
                break;
            case 'hget':
                $key = $options[0];
                $this->parseKey($key);
                $hashKey = $options[1];
                $this->rdo()->hGet($key, $hashKey);
                $commandStr = "HGET '{$key}' '$hashKey'";
                break;
            case 'incr':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $this->rdo()->incr($key);
                $commandStr = "INCR '{$key}'";
                break;
            case 'decr':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $this->rdo()->decr($key);
                $commandStr = "DECR '{$key}'";
                break;
            case 'incrby':
                $key = $options[0];
                $this->parseKey($key);
                $value = $options[1];
                $queryResult = is_int($value) ? $this->rdo()->incrBy($key, $value) : $this->rdo()->incrByFloat($key, $value);
                $commandStr = "INCRBY '{$key}' {$value}";
                break;
            case 'decrby':
                $key = $options[0];
                $this->parseKey($key);
                $value = $options[1];
                $queryResult = $this->rdo()->decrBy($key, $value);
                $commandStr = "DECRBY '{$key}' {$value}";
                break;
            case 'hincrby':
                $key = $options[0];
                $this->parseKey($key);
                $hashKey = $options[1];
                $value = $options[2];
                $queryResult = is_int($value) ? $this->rdo()->hIncrBy($key, $hashKey, $value) : $this->rdo()->hIncrByFloat($key, $hashKey, $value);
                $commandStr = "HINCRBY '{$key}' {$value}";
                break;
        }
        parent::query($commandStr);
        return $queryResult;
    }


}
