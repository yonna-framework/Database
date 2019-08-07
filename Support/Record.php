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
     * 记录的数据库类型
     * @var array
     */
    private $record_db_types = [];

    /**
     * 记录集
     * @var array
     */
    private $records = [];

    /**
     * 时间基准
     * @var array
     */
    private $record_time = null;

    /**
     * Record constructor.
     * @param string | array $dbType
     */
    public function __construct($dbType = null)
    {
        if ($dbType) {
            if (is_string($dbType)) {
                $this->record_db_types = [$dbType];
            } else if (is_array($dbType)) {
                $this->record_db_types = $dbType;
            }
        }
        $this->record_time = $this->time();
    }

    /**
     * get a new time
     * @return float|int
     */
    private function time()
    {
        return 1000 * microtime(true);
    }

    /**
     * 添加记录
     * @param string $dbType
     * @param string $connect
     * @param string $record
     */
    public function addRecord(string $dbType, string $connect, string $record)
    {
        if ($record) {
            $microNow = $this->time();
            if (!$this->record_db_types || in_array($dbType, $this->record_db_types)) {
                $this->records[] = [
                    'type' => $dbType,
                    'connect' => $connect,
                    'query' => $record,
                    'time' => round($microNow - $this->record_time, 4) . 'ms',
                ];
            }
            $this->record_time = $microNow;
        }
    }

    /**
     * 获取记录，获取后自动清空
     * @return array
     */
    public function fetchRecords()
    {
        $record = $this->records;
        $this->record_time = 0;
        $this->records = [];
        return $record;
    }


}
