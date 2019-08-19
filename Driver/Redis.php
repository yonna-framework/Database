<?php

namespace Yonna\Database\Driver;


use Closure;

class Redis extends AbstractRDO
{

    /**
     * 格式化 值
     * @param string $type
     * @param string $value
     * @return float|mixed|string
     */
    private function factoryValue(string $type, string $value)
    {
        switch ($type) {
            case self::TYPE_OBJ:
                $value = json_decode($value, true);
                break;
            case self::TYPE_NUM:
                $value = round($value, 9, PHP_ROUND_HALF_UP);
                break;
            case self::TYPE_STR:
            default:
                break;
        }
        return $value;
    }

    /**
     * 选择一个数据库，索引从0开始,最大支持15
     * @param int $index
     * @param Closure $tempCall
     */
    public function select(int $index, Closure $tempCall = null)
    {
        if ($this->rdo() !== null && $index >= 0 && $index <= 15) {
            $index = (int)$index;
            if ($tempCall !== null) {
                $preIndex = $this->rdo()->getDBNum();
                $this->query('select', $index);
                $tempCall();
                $this->query('select', $preIndex);
            } else {
                $this->query('select', $index);
            }
        }
    }

    /**
     * time
     * @return array
     */
    public function time()
    {
        $time = -1;
        if ($this->rdo() !== null) {
            $time = $this->query('time');
        }
        return $time;
    }

    /**
     * DB size
     * @return int
     */
    public function dbSize()
    {
        $size = -1;
        if ($this->rdo() !== null) {
            $size = $this->query('dbsize');
        }
        return $size;
    }

    /**
     * 使用aof来进行数据库持久化
     * @param bool $sure
     */
    public function bgRewriteAof($sure = false)
    {
        if ($this->rdo() !== null && $sure === true) {
            $this->query('bgrewriteaof');
        }
    }

    /**
     * 将数据同步保存到磁盘
     * @param bool $sure
     */
    public function save($sure = false)
    {
        if ($this->rdo() !== null && $sure === true) {
            $this->query('save');
        }
    }

    /**
     * 将数据异步保存到磁盘
     * @param bool $sure
     */
    public function bgSave($sure = false)
    {
        if ($this->rdo() !== null && $sure === true) {
            $this->query('bgsave');
        }
    }

    /**
     * 返回上次成功将数据保存到磁盘的Unix时戳
     * @param bool $sure
     */
    public function lastSave($sure = false)
    {
        if ($this->rdo() !== null && $sure === true) {
            $this->query('lastsave');
        }
    }

    /**
     * 清空所有
     * @param bool $sure
     */
    public function flushAll($sure = false)
    {
        if ($this->rdo() !== null && $sure === true) {
            $this->query('flushall');
        }
    }

    /**
     * 清空DB
     * @param bool $sure
     */
    public function flushDB($sure = false)
    {
        if ($this->rdo() !== null && $sure === true) {
            $this->query('flushdb');
        }
    }

    /**
     * info [section]------返回关于 Redis 服务器的各种信息和统计数值
     * @param string $section
     * @return mixed|null
     */
    public function info($section = 'default')
    {
        $result = null;
        if ($this->rdo() !== null && $section) {
            $result = $this->query('info', $section);
        }
        return $result;
    }

    /**
     * 删除kEY
     * @param $key
     */
    public function delete($key)
    {
        if ($this->rdo() !== null && $key) {
            $this->query('delete', $key);
        }
    }

    /**
     * 以秒为单位,返回给定key的剩余生存时间(TTL, time to live)
     * @param $key
     * @return int
     */
    public function ttl($key): int
    {
        $ttl = -1;
        if ($this->rdo() !== null && $key) {
            $ttl = $this->query('ttl', $key);
        }
        return $ttl;
    }

    /**
     * 以毫秒为单位,返回给定key的剩余生存时间(TTL, time to live)
     * @param $key
     * @return int
     */
    public function pttl($key): int
    {
        $ttl = -1;
        if ($this->rdo() !== null && $key) {
            $ttl = $this->query('pttl', $key);
        }
        return $ttl;
    }

    /**
     * 检查给定key是否存在
     * @param $key
     * @return bool
     */
    public function exists(string $key): bool
    {
        $exist = false;
        if ($this->rdo() !== null && $key) {
            $exist = $this->query('exists', $key);
        }
        return $exist;
    }

    /**
     * 设定过期时长
     * @param $key
     * @param int $timeout <= 0 not expire
     * @return void
     */
    public function expire($key, int $timeout = 0)
    {
        if ($this->rdo() !== null && $key && $timeout > 0) {
            if ($timeout > 0) {
                $this->query('expire', $key, $timeout);
            }
        }
    }

    /**
     * 设置值，可设置毫秒级别的过期时长
     * @param $key
     * @param $value
     * @param int $ttl <= 0 forever unit:milliseconds
     * @return void
     */
    public function pSet($key, $value, int $ttl = 0)
    {
        if ($this->rdo() !== null && $key) {
            if ($ttl <= 0) {
                if (is_array($value)) {
                    $this->query('set', $key, self::TYPE_OBJ, json_encode($value));
                } elseif (is_string($value)) {
                    $this->query('set', $key, self::TYPE_STR, $value);
                } elseif (is_numeric($value)) {
                    $this->query('set', $key, self::TYPE_NUM, (string)$value);
                } else {
                    $this->query('set', $key, self::TYPE_STR, $value);
                }
            } else {
                if (is_array($value)) {
                    $this->query('psetex', $key, self::TYPE_OBJ, json_encode($value), $ttl);
                } elseif (is_string($value)) {
                    $this->query('psetex', $key, self::TYPE_STR, $value, $ttl);
                } elseif (is_numeric($value)) {
                    $this->query('psetex', $key, self::TYPE_NUM, (string)$value, $ttl);
                } else {
                    $this->query('psetex', $key, self::TYPE_STR, $value, $ttl);
                }
            }
        }
    }

    /**
     * 设置值，可设置过期时长
     * @param $key
     * @param $value
     * @param int $ttl <= 0 forever unit:second
     * @return void
     */
    public function set($key, $value, int $ttl = 0)
    {
        if ($this->rdo() !== null && $key) {
            if ($ttl <= 0) {
                if (is_array($value)) {
                    $this->query('set', $key, self::TYPE_OBJ, json_encode($value));
                } elseif (is_string($value)) {
                    $this->query('set', $key, self::TYPE_STR, $value);
                } elseif (is_numeric($value)) {
                    $this->query('set', $key, self::TYPE_NUM, (string)$value);
                } else {
                    $this->query('set', $key, self::TYPE_STR, $value);
                }
            } else {
                if (is_array($value)) {
                    $this->query('setex', $key, self::TYPE_OBJ, json_encode($value), $ttl);
                } elseif (is_string($value)) {
                    $this->query('setex', $key, self::TYPE_STR, $value, $ttl);
                } elseif (is_numeric($value)) {
                    $this->query('setex', $key, self::TYPE_NUM, (string)$value, $ttl);
                } else {
                    $this->query('setex', $key, self::TYPE_STR, $value, $ttl);
                }
            }
        }
    }

    /**
     * 获取值，key可以是string或一个string的数组，返回多个值
     * @param string|array[string] $key
     * @return bool|null|string|array
     */
    public function get($key)
    {
        $result = null;
        if ($this->rdo() === null || !$key) {
            return $result;
        } else {
            if (is_string($key)) {
                $res = $this->query('get', $key);
                $type = substr($res, 0, 1);
                $value = substr($res, 1);
                $result = $this->factoryValue($type, $value);
            } else if (is_array($key)) {
                $res = $this->query('mget', $key);
                $result = [];
                foreach ($res as $k => $v) {
                    $type = substr($v, 0, 1);
                    $value = substr($v, 1);
                    $result[$key[$k]] = $this->factoryValue($type, $value);
                }
            }
            return $result;
        }
    }

    /**
     * @param array $kv
     * @param int $ttl
     * @return void
     */
    public function mSet(array $kv, int $ttl = 0)
    {
        if ($this->rdo() !== null && $kv) {
            $keys = [];
            foreach ($kv as $k => $v) {
                if (is_array($v)) {
                    $keys[$k] = self::TYPE_OBJ . json_encode($v);
                } elseif (is_string($v)) {
                    $keys[$k] = self::TYPE_STR . $v;
                } elseif (is_numeric($v)) {
                    $keys[$k] = self::TYPE_NUM . (string)$v;
                } else {
                    $keys[$k] = self::TYPE_STR . $v;
                }
            }
            $this->query('mset', $keys, $ttl);
        }
    }

    /**
     * 获取值，key可以是string或一个string的数组，返回多个值
     * @param array[string] $key
     * @return array
     */
    public function mGet(array $key)
    {
        $result = [];
        if ($this->rdo() === null || !$key) {
            return $result;
        } else {
            $res = $this->query('mget', $key);
            $result = [];
            foreach ($res as $k => $v) {
                $type = substr($v, 0, 1);
                $value = substr($v, 1);
                $result[$key[$k]] = $this->factoryValue($type, $value);
            }
        }
        return $result;
    }

    /**
     * @param $hashKey
     * @param $key
     * @param $value
     * @return void
     */
    public function hSet($hashKey, $key, $value)
    {
        if ($this->rdo() !== null && $hashKey && $key) {
            if (is_array($value)) {
                $this->query('hset', $hashKey, $key, self::TYPE_OBJ, json_encode($value));
            } elseif (is_string($value)) {
                $this->query('hset', $hashKey, $key, self::TYPE_STR, $value);
            } elseif (is_numeric($value)) {
                $this->query('hset', $hashKey, $key, self::TYPE_NUM, (string)$value);
            } else {
                $this->query('hset', $hashKey, $key, self::TYPE_STR, $value);
            }
        }
    }

    /**
     * @param $hashKey
     * @param $key
     * @return bool|null|string|array
     */
    public function hGet($key, $hashKey)
    {
        $result = null;
        if ($this->rdo() === null || !$key || !$hashKey) {
            return $result;
        } else {
            $res = $this->query('hget', $key, $hashKey);
            $type = substr($res, 0, 1);
            $value = substr($res, 1);
            $result = $this->factoryValue($type, $value);
        }
        return $result;
    }

    /**
     * @param $key
     * @param int $value
     * @return int | float
     */
    public function incr($key, $value = 1)
    {
        $answer = -1;
        if ($this->rdo() === null || !$key) {
            return $answer;
        }
        if ($value === 1) {
            $answer = $this->query('incr', $key);
        } else {
            $answer = $this->query('incrby', $key, $value);
        }
        return $answer;
    }

    /**
     * @param $key
     * @param int $value
     * @return int
     */
    public function decr($key, int $value = 1)
    {
        $answer = -1;
        if ($this->rdo() === null || !$key) {
            return $answer;
        }
        if ($value === 1) {
            $answer = $this->query('decr', $key);
        } else {
            $answer = $this->query('decrby', $key, $value);
        }
        return $answer;
    }

    /**
     * @param $key
     * @param $hashKey
     * @param int | float $value
     * @return int
     */
    public function hIncr($key, $hashKey, $value = 1)
    {
        $answer = -1;
        if ($this->rdo() !== null && $key) {
            $answer = $this->query('hincrby', $key, $hashKey, $value);
        }
        return $answer;
    }

}