<?php

namespace Yonna\Database\Driver;

use Yonna\Throwable\Exception;
use Redis;
use Swoole\Coroutine\Redis as SwRedis;

abstract class AbstractRDO extends AbstractDB
{

    protected $db_type = Type::REDIS;


    const TYPE_OBJ = 'o';
    const TYPE_STR = 's';
    const TYPE_NUM = 'n';

    /**
     * @var Redis | SwRedis | null
     *
     */
    protected $redis = null;

    /**
     * 架构函数 取得模板对象实例
     * @access public
     * @param array $setting
     * @throws Exception\DatabaseException
     */
    public function __construct(array $setting)
    {
        parent::__construct($setting);
        $this->redis = $this->malloc();
        return $this;
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
     * 设置执行命令
     * @param $command
     * @param mixed ...$options
     * @return mixed
     */
    protected function query($command, ...$options)
    {
        $result = null;
        $commandStr = "un know command";
        switch ($command) {
            case 'select':
                $index = $options[0];
                $this->redis->select($index);
                $commandStr = "SELECT {$index}";
                break;
            case 'time':
                $result = $this->redis->time();
                $commandStr = 'TIME';
                break;
            case 'dbsize':
                $result = $this->redis->dbSize();
                $commandStr = 'DBSIZE';
                break;
            case 'bgrewriteaof':
                $this->redis->bgrewriteaof();
                $commandStr = 'BGREWRITEAOF';
                break;
            case 'save':
                $this->redis->save();
                $commandStr = 'SAVE';
                break;
            case 'bgsave':
                switch ($this->db_type) {
                    case Type::REDIS:
                        $this->redis->bgsave();
                        break;
                    case Type::REDIS_CO:
                        $this->redis->bgSave();
                        break;
                }
                $commandStr = 'BGSAVE';
                break;
            case 'lastsave':
                $this->redis->lastSave();
                $commandStr = 'LASTSAVE';
                break;
            case 'flushall':
                $this->redis->flushAll();
                $commandStr = 'FLUSHALL';
                break;
            case 'flushdb':
                $this->redis->flushDB();
                $commandStr = 'FLUSHDB';
                break;
            case 'info':
                $section = $options[0];
                switch ($this->db_type) {
                    case Type::REDIS:
                        $result = $this->redis->info($section);
                        break;
                    case Type::REDIS_CO:
                        $result = '';
                        break;
                }
                $commandStr = "INFO '{$section}'";
                break;
            case 'delete':
                $key = $options[0];
                $this->redis->delete($key);
                $commandStr = "DELETE '{$key}'";
                break;
            case 'ttl':
                $key = $options[0];
                $result = $this->redis->ttl($key);
                $commandStr = "TTL '{$key}'";
                break;
            case 'pttl':
                $key = $options[0];
                $result = $this->redis->pttl($key);
                $commandStr = "PTTL '{$key}'";
                break;
            case 'exists':
                $key = $options[0];
                $result = $this->redis->exists($key) == 1 ? true : false;
                $commandStr = "EXISTS '{$key}'";
                break;
            case 'expire':
                $key = $options[0];
                $this->redis->expire($key, $options[1]);
                $commandStr = "EXPIRE '{$key}' '{$options[1]}'";
                break;
            case 'set':
                $key = $options[0];
                $value = $options[1] . $options[2];
                $this->redis->set($key, $value);
                $commandStr = "SET '{$key}' '{$value}'";
                break;
            case 'setex':
                $key = $options[0];
                $value = $options[1] . $options[2];
                $ttl = $options[3];
                switch ($this->db_type) {
                    case Type::REDIS:
                        $this->redis->setex($key, $ttl, $value);
                        break;
                    case Type::REDIS_CO:
                        $this->redis->setEx($key, $ttl, $value);
                        break;
                }
                $commandStr = "SETEX '{$key}' {$ttl} '{$value}'";
                break;
            case 'psetex':
                $key = $options[0];
                $value = $options[1] . $options[2];
                $ttl = $options[3];
                switch ($this->db_type) {
                    case Type::REDIS:
                        $this->redis->psetex($key, $ttl, $value);
                        break;
                    case Type::REDIS_CO:
                        $this->redis->psetEx($key, $ttl, $value);
                        break;
                }
                $commandStr = "PSETEX '{$key}' {$ttl} '{$value}'";
                break;
            case 'get':
                $key = $options[0];
                $result = $this->redis->get($key);
                $commandStr = "GET '{$key}'";
                break;
            case 'mset':
                $key = $options[0];
                $ttl = $options[1];
                switch ($this->db_type) {
                    case Type::REDIS:
                        $result = $this->redis->mset($key);
                        break;
                    case Type::REDIS_CO:
                        $result = $this->redis->mSet($key);
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
                switch ($this->db_type) {
                    case Type::REDIS:
                        $result = $this->redis->mget($key);
                        break;
                    case Type::REDIS_CO:
                        $result = $this->redis->mGet($key);
                        break;
                }
                $key = array_map(function ($k) {
                    return "'{$k}'";
                }, $key);
                $commandStr = "MGET " . implode(' ', $key);
                break;
            case 'hset':
                $key = $options[0];
                $hashKey = $options[1];
                $value = $options[2] . $options[3];
                $this->redis->hSet($key, $hashKey, $value);
                $commandStr = "HSET '{$key}' '$hashKey' '{$value}'";
                break;
            case 'hget':
                $key = $options[0];
                $hashKey = $options[1];
                $this->redis->hGet($key, $hashKey);
                $commandStr = "HGET '{$key}' '$hashKey'";
                break;
            case 'incr':
                $key = $options[0];
                $result = $this->redis->incr($key);
                $commandStr = "INCR '{$key}'";
                break;
            case 'decr':
                $key = $options[0];
                $result = $this->redis->decr($key);
                $commandStr = "DECR '{$key}'";
                break;
            case 'incrby':
                $key = $options[0];
                $value = $options[1];
                $result = is_int($value) ? $this->redis->incrBy($key, $value) : $this->redis->incrByFloat($key, $value);
                $commandStr = "INCRBY '{$key}' {$value}";
                break;
            case 'decrby':
                $key = $options[0];
                $value = $options[1];
                $result = $this->redis->decrBy($key, $value);
                $commandStr = "DECRBY '{$key}' {$value}";
                break;
            case 'hincrby':
                $key = $options[0];
                $hashKey = $options[1];
                $value = $options[2];
                $result = is_int($value) ? $this->redis->hIncrBy($key, $hashKey, $value) : $this->redis->hIncrByFloat($key, $hashKey, $value);
                $commandStr = "HINCRBY '{$key}' {$value}";
                break;
        }
        parent::query($commandStr);
        return $result;
    }


}
