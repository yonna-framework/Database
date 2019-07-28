<?php

namespace Yonna\Database\Driver;


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
     * DB size
     * @return int
     */
    public function dbSize()
    {
        $size = -1;
        if ($this->redis !== null) {
            $size = $this->query('dbsize');
        }
        return $size;
    }

    /**
     * 清空所有
     * @param bool $sure
     */
    public function flushAll($sure = false)
    {
        if ($this->redis !== null && $sure === true) {
            $this->query('flushall');
        }
    }

    /**
     * 删除kEY
     * @param $key
     */
    public function delete($key)
    {
        if ($this->redis !== null && $key) {
            $this->query('delete', $key);
        }
    }

    /**
     * 设定过期时长
     * @param $key
     * @param int $timeout <= 0 not expire
     * @return void
     */
    public function expire($key, int $timeout = 0)
    {
        if ($this->redis !== null && $key && $timeout > 0) {
            if ($timeout > 0) {
                $this->query('expire', $key, $timeout);
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
        if ($this->redis !== null && $key) {
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
     * @param array $kv
     * @param int $ttl
     * @return void
     */
    public function mSet(array $kv, int $ttl = 0)
    {
        if ($this->redis !== null && $kv) {
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
     * 设置值，可设置毫秒级别的过期时长
     * @param $key
     * @param $value
     * @param int $ttl <= 0 forever unit:milliseconds
     * @return void
     */
    public function pSet($key, $value, int $ttl = 0)
    {
        if ($this->redis !== null && $key) {
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
     * @param $hashKey
     * @param $key
     * @param $value
     * @return void
     */
    public function hSet($hashKey, $key, $value)
    {
        if ($this->redis !== null && $hashKey && $key) {
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
     * 获取值，key可以是string或一个string的数组，返回多个值
     * @param string|array[string] $key
     * @return bool|null|string|array
     */
    public function get($key)
    {
        $result = null;
        if ($this->redis === null || !$key) {
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
     * 获取值，key可以是string或一个string的数组，返回多个值
     * @param array[string] $key
     * @return array
     */
    public function mGet(array $key)
    {
        $result = [];
        if ($this->redis === null || !$key) {
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
     * @return bool|null|string|array
     */
    public function hGet($key, $hashKey)
    {
        $result = null;
        if ($this->redis === null || !$key || !$hashKey) {
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
        if ($this->redis === null || !$key) {
            return $answer;
        }
        $key = $this->parse($key);
        if ($value === 1) {
            $answer = $this->redis->incr($key);
        } else {
            $answer = is_int($value) ? $this->redis->incrBy($key, $value) : $this->redis->incrByFloat($key, $value);
        }
        return $answer;
    }

    /**W
     * @param $key
     * @param int $value
     * @return int
     */
    public function decr($key, $value = 1)
    {
        $answer = -1;
        if ($this->redis === null || !$key) {
            return $answer;
        }
        $key = $this->parse($key);
        if ($value === 1) {
            $answer = $this->redis->decr($key);
        } else {
            $answer = $this->redis->decrBy($key, $value);
        }
        return $answer;
    }

    /**
     * @param $key
     * @param $hashKey
     * @param int $value
     * @return int
     */
    public function hIncr($key, $hashKey, int $value = 1)
    {
        $answer = -1;
        if ($this->redis !== null && $key) {
            $key = $this->parse($key);
            $answer = $this->redis->hIncrBy($key, $hashKey, $value);
        }
        return $answer;
    }

}