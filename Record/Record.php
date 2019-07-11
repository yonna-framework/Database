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
        if (!isset(static::$records[$dbType])) {
            static::$records[$dbType] = [];
        }
        if (!isset(static::$records[$dbType][$connect])) {
            static::$records[$dbType][$connect] = [];
        }
        if ($record) {
            static::$records[$dbType][$connect][] = $record;
        }
    }

    /**
     * 启用记录
     */
    public static function enableRecord()
    {
        static::$is_record = true;
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
        }
        return $record;
    }


}
