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
     * @var array
     */
    private static $is_record = [];

    /**
     * 记录的数据库类型
     * @var array
     */
    private static $record_db_types = [];

    /**
     * 记录集
     * @var array
     */
    private static $records = [];

    /**
     * 时间基准
     * @var array
     */
    private static $record_time = [];


    /**
     * @param null $val
     * @return bool
     */
    private static function isRecord($val = null): bool
    {
        if (is_bool($val)) {
            static::$is_record[self::uuid()] = $val;
        }
        return static::$is_record[self::uuid()] ?? false;
    }

    /**
     * @param array $val
     * @return array
     */
    private static function recordType($val = null): array
    {
        if ($val !== null) {
            if (is_string($val)) {
                static::$record_db_types[self::uuid()][] = $val;
            } else if (is_array($val)) {
                static::$record_db_types[self::uuid()] = $val;
            }
        }
        return static::$record_db_types[self::uuid()] ?? [];
    }

    /**
     * @param float $val
     * @return float
     */
    private static function recordTime(float $val = null): float
    {
        if ($val !== null) {
            static::$record_time[self::uuid()] = $val;
        }
        return static::$record_time[self::uuid()] ?? 0;
    }

    /**
     * @param array $val
     * @return array
     */
    private static function records(array $val = null): array
    {
        if ($val !== null && is_array($val)) {
            if (empty($val)) {
                static::$records[self::uuid()] = [];
            } else {
                static::$records[self::uuid()][] = $val;
            }
        }
        return static::$records[self::uuid()] ?? [];
    }

    /**
     * 添加记录
     * @param string $dbType
     * @param string $connect
     * @param string $record
     */
    public static function addRecord(string $dbType, string $connect, string $record)
    {
        if (static::isRecord() !== true) {
            return;
        }
        if ($record) {
            $microNow = 1000 * microtime(true);
            if (!static::recordType() || in_array($dbType, static::recordType())) {
                static::records([
                    'type' => $dbType,
                    'connect' => $connect,
                    'query' => $record,
                    'time' => round($microNow - static::recordTime(), 4) . 'ms',
                ]);
            }
            static::recordTime($microNow);
        }
    }

    /**
     * 启用记录
     * @param array | string $dbType
     */
    public static function enableRecord($dbType = null)
    {
        static::isRecord(true);
        static::recordType([]);
        static::recordType($dbType);
        static::recordTime(1000 * microtime(true));
    }

    /**
     * 获取记录，获取后自动清空
     * @return array
     */
    public static function getRecord()
    {
        $record = static::records();
        static::isRecord(false);
        static::records([]);
        static::recordTime(0);
        return $record;
    }


}
