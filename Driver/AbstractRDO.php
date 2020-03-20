<?php

namespace Yonna\Database\Driver;

use Redis;
use Swoole\Coroutine\Redis as SwRedis;
use Yonna\Database\Support\Transaction;
use Yonna\Throwable\Exception\DatabaseException;

abstract class AbstractRDO extends AbstractDB
{

    const TYPE_OBJ = 'o';
    const TYPE_STR = 's';
    const TYPE_NUM = 'n';

    const READ_COMMAND = ['time', 'dbsize', 'info', 'exists', 'get', 'mget', 'hget', 'lpop', 'rpop'];

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
            $key = $this->project_key . ':' . addslashes($key);
        } else if (is_array($key)) {
            foreach ($key as &$k) {
                $this->parseKey($k);
            }
        }
    }


    /**
     * 获取 RDO
     * @param bool $force_new
     * @return Redis | SwRedis
     */
    protected function rdo($force_new = false)
    {
        return $this->malloc($force_new);
    }

    /**
     * @param $command
     * @return bool
     */
    protected function isReadTransaction($command): bool
    {
        return in_array($command, self::READ_COMMAND) && Transaction::in();
    }

    /**
     * 设置执行命令
     * @param $command
     * @param mixed ...$options
     * @return mixed
     * @throws DatabaseException
     */
    protected function query($command, ...$options)
    {
        $queryResult = null;
        $commandStr = "un know command";
        if ($this->isReadTransaction($command)) {
            $rdo = $this->rdo(true);
        } else {
            $rdo = $this->rdo();
        }
        switch ($command) {
            case 'select':
                $index = $options[0];
                $rdo->select($index);
                $commandStr = "SELECT {$index}";
                break;
            case 'time':
                $queryResult = $rdo->time();
                $commandStr = 'TIME';
                break;
            case 'dbsize':
                $queryResult = $rdo->dbSize();
                $commandStr = 'DBSIZE';
                break;
            case 'bgrewriteaof':
                $rdo->bgrewriteaof();
                $commandStr = 'BGREWRITEAOF';
                break;
            case 'save':
                $rdo->save();
                $commandStr = 'SAVE';
                break;
            case 'bgsave':
                switch ($this->options['db_type']) {
                    case Type::REDIS:
                        $rdo->bgsave();
                        break;
                    case Type::REDIS_CO:
                        $rdo->bgSave();
                        break;
                }
                $commandStr = 'BGSAVE';
                break;
            case 'lastsave':
                $rdo->lastSave();
                $commandStr = 'LASTSAVE';
                break;
            case 'flushall':
                $rdo->flushAll();
                $commandStr = 'FLUSHALL';
                break;
            case 'flushdb':
                $rdo->flushDB();
                $commandStr = 'FLUSHDB';
                break;
            case 'info':
                $section = $options[0];
                switch ($this->options['db_type']) {
                    case Type::REDIS:
                        $queryResult = $rdo->info($section);
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
                $rdo->del($key);
                $commandStr = "DELETE '{$key}'";
                break;
            case 'ttl':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $rdo->ttl($key);
                $commandStr = "TTL '{$key}'";
                break;
            case 'pttl':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $rdo->pttl($key);
                $commandStr = "PTTL '{$key}'";
                break;
            case 'exists':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $rdo->exists($key) == 1 ? true : false;
                $commandStr = "EXISTS '{$key}'";
                break;
            case 'expire':
                $key = $options[0];
                $this->parseKey($key);
                $rdo->expire($key, $options[1]);
                $commandStr = "EXPIRE '{$key}' '{$options[1]}'";
                break;
            case 'set':
                $key = $options[0];
                $this->parseKey($key);
                $value = $options[1] . $options[2];
                $rdo->set($key, $value);
                $commandStr = "SET '{$key}' '{$value}'";
                break;
            case 'setex':
                $key = $options[0];
                $this->parseKey($key);
                $value = $options[1] . $options[2];
                $ttl = $options[3];
                switch ($this->options['db_type']) {
                    case Type::REDIS:
                        $rdo->setex($key, $ttl, $value);
                        break;
                    case Type::REDIS_CO:
                        $rdo->setEx($key, $ttl, $value);
                        break;
                }
                $commandStr = "SETEX '{$key}' {$ttl} '{$value}'";
                break;
            case 'psetex':
                $key = $options[0];
                $this->parseKey($key);
                $value = $options[1] . $options[2];
                $ttl = $options[3];
                switch ($this->options['db_type']) {
                    case Type::REDIS:
                        $rdo->psetex($key, $ttl, $value);
                        break;
                    case Type::REDIS_CO:
                        $rdo->psetEx($key, $ttl, $value);
                        break;
                }
                $commandStr = "PSETEX '{$key}' {$ttl} '{$value}'";
                break;
            case 'get':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $rdo->get($key);
                $commandStr = "GET '{$key}'";
                break;
            case 'mset':
                $key = $options[0];
                $this->parseKey($key);
                $ttl = $options[1];
                switch ($this->options['db_type']) {
                    case Type::REDIS:
                        $queryResult = $rdo->mset($key);
                        break;
                    case Type::REDIS_CO:
                        $queryResult = $rdo->mSet($key);
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
                switch ($this->options['db_type']) {
                    case Type::REDIS:
                        $queryResult = $rdo->mget($key);
                        break;
                    case Type::REDIS_CO:
                        $queryResult = $rdo->mGet($key);
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
                $rdo->hSet($key, $hashKey, $value);
                $commandStr = "HSET '{$key}' '$hashKey' '{$value}'";
                break;
            case 'hget':
                $key = $options[0];
                $this->parseKey($key);
                $hashKey = $options[1];
                $queryResult = $rdo->hGet($key, $hashKey);
                $commandStr = "HGET '{$key}' '$hashKey'";
                break;
            case 'incr':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $rdo->incr($key);
                $commandStr = "INCR '{$key}'";
                break;
            case 'decr':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $rdo->decr($key);
                $commandStr = "DECR '{$key}'";
                break;
            case 'incrby':
                $key = $options[0];
                $this->parseKey($key);
                $value = $options[1];
                $queryResult = is_int($value) ? $rdo->incrBy($key, $value) : $rdo->incrByFloat($key, $value);
                $commandStr = "INCRBY '{$key}' {$value}";
                break;
            case 'decrby':
                $key = $options[0];
                $this->parseKey($key);
                $value = $options[1];
                $queryResult = $rdo->decrBy($key, $value);
                $commandStr = "DECRBY '{$key}' {$value}";
                break;
            case 'hincrby':
                $key = $options[0];
                $this->parseKey($key);
                $hashKey = $options[1];
                $value = $options[2];
                $queryResult = is_int($value) ? $rdo->hIncrBy($key, $hashKey, $value) : $rdo->hIncrByFloat($key, $hashKey, $value);
                $commandStr = "HINCRBY '{$key}' {$value}";
                break;
            case 'llen':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $rdo->lLen($key);
                $commandStr = "LLEN '{$key}'";
                break;
            case 'lpush':
                $key = $options[0];
                $this->parseKey($key);
                $value = $options[1];
                $queryResult = $rdo->lpush($key, ...$value);
                if (is_array($value)) {
                    $commandStr = "LPUSH '{$key}' " . implode(',', $value);
                } else {
                    $commandStr = "LPUSH '{$key}' {$value}";
                }
                break;
            case 'rpush':
                $key = $options[0];
                $this->parseKey($key);
                $value = $options[1];
                $queryResult = $rdo->rPush($key, ...$value);
                if (is_array($value)) {
                    $commandStr = "RPUSH '{$key}' " . implode(',', $value);
                } else {
                    $commandStr = "RPUSH '{$key}' {$value}";
                }
                break;
            case 'lpop':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $rdo->lPop($key);
                $commandStr = "LPOP '{$key}'";
                break;
            case 'rpop':
                $key = $options[0];
                $this->parseKey($key);
                $queryResult = $rdo->rPop($key);
                $commandStr = "RPOP '{$key}'";
                break;
        }
        if ($this->isReadTransaction($command)) {
            $rdo->close();
        }
        parent::query($commandStr);
        return $queryResult;
    }


}
