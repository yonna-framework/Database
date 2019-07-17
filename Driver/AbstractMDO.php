<?php

namespace Yonna\Database\Driver;

use MongoDB;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Exception\BulkWriteException;
use Yonna\Throwable\Exception;

abstract class AbstractMDO extends AbstractDB
{

    /**
     * @var MongoDB\Driver\Manager | null
     *
     */
    protected $mongoManager = null;

    /**
     * @var string
     */
    protected $collection = null;

    /**
     * 架构函数 取得模板对象实例
     * @access public
     * @param array $setting
     * @throws Exception\DatabaseException
     */
    public function __construct(array $setting)
    {
        parent::__construct($setting);
        $this->collection = $setting['collection'];
        if ($this->mongoManager == null) {
            if (class_exists('\\MongoDB\Driver\Manager')) {
                try {
                    $this->mongoManager = new MongoDB\Driver\Manager($this->dsn());
                } catch (\Exception $e) {
                    $this->mongoManager = null;
                    Exception::database('MongoDB遇到问题或未安装，请暂时停用MongoDB以减少阻塞卡顿');
                }
            }
        }
    }

    public function __destruct()
    {
        parent::__destruct();
    }

    /**
     * @return string
     */
    public function getCollection()
    {
        return $this->collection;
    }

    /**
     * 设置执行命令
     * @param $command
     * @param mixed ...$options
     * @return mixed
     * @throws Exception\DatabaseException
     */
    protected function query($command, ...$options)
    {
        $result = null;
        $commandStr = "un know command";
        switch ($command) {
            case 'insert':
                $commandStr = "db.{$this->collection}.insertOne(" . json_encode($options[0], JSON_UNESCAPED_UNICODE) . ')';
                $data = $options[0];
                $bulk = new BulkWrite();
                try {
                    $bulk->insert($data);
                } catch (BulkWriteException $e) {
                    Exception::database($e->getMessage());
                }
                $result = $this->mongoManager->executeBulkWrite($this->name . '.' . $this->collection, $bulk);
                $ids = $result->getUpsertedIds();
                $count = $result->getInsertedCount();
                $result = [$ids, $count];
                break;
            case 'insertAll':
                $commandStr = "db.{$this->collection}.insertMany(" . json_encode($options[0], JSON_UNESCAPED_UNICODE) . ')';
                $data = $options[0];
                $bulk = new BulkWrite();
                try {
                    foreach ($data as $d) {
                        $bulk->insert($d);
                    }
                } catch (BulkWriteException $e) {
                    Exception::database($e->getMessage());
                }
                $result = $this->mongoManager->executeBulkWrite($this->name . '.' . $this->collection, $bulk);
                $ids = $result->getUpsertedIds();
                $count = $result->getInsertedCount();
                $result = [$ids, $count];
                break;
        }
        parent::query($commandStr);
        return $result;
    }


    public function test()
    {
        $query = ["_id" => ['$gte' => 0]];
        $cmd = new MongoDB\Driver\Command([
            'distinct' => 'color',
            'key' => 'color',
            'query' => $query
        ]);
        print_r($cmd);
        $row = $this->mongoManager->executeCommand("olddream", $cmd);
    }

}
