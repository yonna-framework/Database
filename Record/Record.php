<?php


namespace Yonna\Database\Record;

/**
 * 数据库记录
 * Class Record
 * @package Yonna\Database\Record
 */
class Record
{

    /**
     * @var array
     */
    private static $is_record = [];

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
            static::$records[] = [
                'connect' => "[$dbType]$connect",
                'query' => $record,
                'time' => round($microNow - static::$record_time,4) . 'ms',
            ];
            static::$record_time = $microNow;
        }
    }

    /**
     * 启用记录
     */
    public static function enableRecord()
    {
        static::$is_record = true;
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
