<?php

namespace Yonna\Database\Support;

/**
 * 数据库记录
 * Class Record
 * @package Yonna\Database\Support
 */
class Record extends Support
{

    /**
     * @var bool
     */
    private static $enable = false;

    /**
     * 记录集
     * @var array
     */
    private static $records = [];

    /**
     * 时间基准
     * @var float|int
     */
    private static $record_time = 0;

    /**
     * get a new time
     * @return float|int
     */
    private static function time()
    {
        return 1000 * microtime(true);
    }

    /**
     * @return bool
     */
    private static function isEnable(): bool
    {
        return self::$enable;
    }

    /**
     * @param bool $enable
     */
    public static function setEnable(bool $enable): void
    {
        self::$enable = $enable;
        self::$record_time = self::time();
    }

    /**
     * 添加记录
     * @param string $dbType
     * @param string $connect
     * @param string $record
     */
    public static function add(string $dbType, string $connect, string $record)
    {
        if (self::isEnable() && $record) {
            $microNow = self::time();
            self::$records[] = [
                'type' => $dbType,
                'connect' => $connect,
                'query' => $record,
                'time' => round($microNow - self::$record_time, 4) . 'ms',
            ];
            self::$record_time = $microNow;
        }
    }

    /**
     * 清空记录
     */
    public static function clear()
    {
        self::$record_time = 0;
        self::$records = [];
    }

    /**
     * 获取记录，获取瞬间会disabled记录标识
     * @param $dbTypes
     * @return array
     */
    public static function fetch($dbTypes = null): array
    {
        self::setEnable(false);
        $record = [];
        if (is_string($dbTypes)) {
            $dbTypes = [$dbTypes];
        }
        if (!is_array($dbTypes)) {
            $dbTypes = null;
        }
        foreach (self::$records as $v) {
            if ($dbTypes === null || in_array($v['type'], $dbTypes)) {
                $record[] = $v;
            }
        }
        return $record;
    }

}
