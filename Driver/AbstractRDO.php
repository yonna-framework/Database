<?php

namespace Yonna\Database\Driver;

use Yonna\Foundation\Str;
use Yonna\Throwable\Exception;
use Redis as RedisDriver;
use Swoole\Coroutine\Redis as RedisSwoole;

abstract class AbstractRDO extends AbstractDB
{

    protected $db_type = Type::REDIS;


    const TYPE_OBJ = 'o';
    const TYPE_STR = 's';
    const TYPE_NUM = 'n';

    /**
     * @var RedisDriver | RedisSwoole | null
     *
     */
    protected $redis = null;

    /**
     * 架构函数 取得模板对象实例
     * @access public
     * @param array $setting
     * @param RedisSwoole | null $RedisDriver
     * @throws Exception\DatabaseException
     */
    public function __construct(array $setting, $RedisDriver = null)
    {
        parent::__construct($setting);
        if ($RedisDriver == null) {
            if (class_exists('\\Redis')) {
                try {
                    $RedisDriver = new RedisDriver();
                } catch (\Exception $e) {
                    $this->redis = null;
                    Exception::database('Redis遇到问题或未安装，请暂时停用Redis以减少阻塞卡顿');
                }
            }
        }
        $this->redis = $RedisDriver;
        $this->redis->connect(
            $this->host,
            $this->port
        );
        if ($this->password) {
            $this->redis->auth($this->password);
        }
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
            case 'dbsize':
                $result = $this->redis->dbSize();
                $commandStr = 'DBSIZE';
                break;
            case 'flushall':
                $this->redis->flushAll();
                $commandStr = 'FLUSHALL';
                break;
            case 'delete':
                $key = $options[0];
                $this->redis->delete($key);
                $commandStr = "DELETE '{$key}'";
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
        }
        parent::query($commandStr);
        return $result;
    }


}
