<?php

namespace Yonna\Database\Driver;


class Redis extends AbstractRDO
{


    /**
     * 清空所有
     * @param bool $sure
     */
    public function flushAll($sure = false)
    {
        if ($this->redis !== null && $sure === true) {
            $this->query('flushAll');
        }
    }

    /**
     * @return int
     */
    public function dbSize()
    {
        $size = -1;
        if ($this->redis !== null) {
            $size = $this->query('dbSize');
        }
        return $size;
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
     * @param $key
     * @param $value
     * @param int $timeout <= 0 not expire
     * @return void
     */
    public function set($key, $value, int $timeout = 0)
    {
        if ($this->redis !== null && $key) {
            if (is_array($value)) {
                $this->query('set', $key, self::TYPE_OBJ, json_encode($value));
            } elseif (is_string($value)) {
                $this->query('set', $key, self::TYPE_STR, $value);
            } elseif (is_numeric($value)) {
                $this->query('set', $key, self::TYPE_NUM, (string)$value);
            } else {
                $this->query('set', $key, self::TYPE_STR, $value);
            }
            if ($timeout > 0) {
                $this->query('expire', $key, $timeout);
            }
        }
    }

    /**
     * @param $key
     * @return bool|null|string|array
     */
    public function get($key)
    {
        if ($this->redis === null || !$key) {
            return null;
        } else {
            $result = $this->query('get', $key);
            $type = $result[0];
            $value = $result[1];
            switch ($type) {
                case self::TYPE_OBJ:
                    $value = json_decode($value, true);
                    break;
                case self::TYPE_NUM:
                    $value = round($value, 10);
                    break;
                case self::TYPE_STR:
                default:
                    break;
            }
            return $value;
        }
    }

    /**
     * @param $table
     * @param $key
     * @param $value
     * @return void
     */
    public function hSet($table, $key, $value)
    {
        if ($this->redis !== null && $table && $key) {
            $table = $this->parse($table);
            if (is_array($value)) {
                $this->redis->hSet($table, self::TYPE_OBJ . $key, json_encode($value));
            } elseif (is_string($value)) {
                $this->redis->hSet($table, self::TYPE_STR . $key, $value);
            } elseif (is_numeric($value)) {
                $this->redis->hSet($table, self::TYPE_NUM . $key, $value);
            } else {
                $this->redis->hSet($table, self::TYPE_STR . $key, $value);
            }
        }
    }

    /**
     * @param $table
     * @param $key
     * @return bool|null|string|array
     */
    public function hGet($table, $key)
    {
        if ($this->redis === null || !$table || !$key) {
            return null;
        } else {
            $table = $this->parse($table);
            $value = $this->redis->hGet($table, $key);
            $type = substr($value, 0, 1);
            $value = substr($value, 1);
            switch ($type) {
                case self::TYPE_OBJ:
                    $value = json_decode($value, true);
                    break;
                case self::TYPE_NUM:
                    $value = round($value, 10);
                    break;
                case self::TYPE_STR:
                default:
                    break;
            }
            return $value;
        }
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