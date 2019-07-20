<?php


namespace Yonna\Database\Transaction;

/**
 * 事务
 * Class Transaction
 * @package Yonna\Database\Transaction
 */
class Transaction
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
    private static $record_time = 0;

    /**
     * 添加记录
     * @param string $dbType
     * @param string $connect
     * @param string $record
     */
    public static function addRecord(string $dbType, string $connect, string $record)
    {
        if (static::$is_record !== true) {
            return;
        }
        if ($record) {
            $microNow = 1000 * microtime(true);
            if (!static::$record_db_types || in_array($dbType, static::$record_db_types)) {
                static::$records[] = [
                    'type' => $dbType,
                    'connect' => $connect,
                    'query' => $record,
                    'time' => round($microNow - static::$record_time, 4) . 'ms',
                ];
            }
            static::$record_time = $microNow;
        }
    }

    /**
     * 启用记录
     * @param array | string $dbType
     */
    public static function enableRecord($dbType = null)
    {
        static::$is_record = true;
        static::$record_db_types = [];
        if (is_string($dbType)) {
            static::$record_db_types[] = $dbType;
        } else if (is_array($dbType)) {
            static::$record_db_types = $dbType;
        }
        static::$record_time = 1000 * microtime(true);
    }

    /**
     * 获取记录，获取后自动清空
     * @return array
     */
    public static function getRecord()
    {
        $record = [];
        if (isset(static::$records)) {
            $record = static::$records;
            static::$records = [];
            static::$record_time = 0;
        }
        return $record;
    }


}
