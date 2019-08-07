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
    private $enable = false;

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
     */
    public function __construct()
    {
        $this->record_time = $this->time();
    }

    /**
     * @return bool
     */
    private function isEnable(): bool
    {
        return $this->enable;
    }

    /**
     * @param bool $enable
     */
    public function setEnable(bool $enable): void
    {
        $this->enable = $enable;
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
    public function add(string $dbType, string $connect, string $record)
    {
        if ($this->isEnable() && $record) {
            $microNow = $this->time();
            $this->records[] = [
                'type' => $dbType,
                'connect' => $connect,
                'query' => $record,
                'time' => round($microNow - $this->record_time, 4) . 'ms',
            ];
            $this->record_time = $microNow;
        }
    }

    /**
     * 清空记录
     */
    public function clear()
    {
        $this->record_time = 0;
        $this->records = [];
    }

    /**
     * 获取记录，获取瞬间会disabled记录标识
     * @param $dbTypes
     * @return array
     */
    public function fetch($dbTypes = null): array
    {
        $this->setEnable(false);
        $record = [];
        if (is_string($dbTypes)) {
            $dbTypes = [$dbTypes];
        }
        if (!is_array($dbTypes)) {
            $dbTypes = null;
        }
        foreach ($this->records as $v) {
            if ($dbTypes === null || in_array($v['type'], $dbTypes)) {
                $record[] = $v;
            }
        }
        return $record;
    }

}
