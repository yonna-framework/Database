<?php

namespace Yonna\Database\Driver;

use MongoDB\Driver\Query;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Command;
use MongoDB\Driver\Exception\BulkWriteException;
use Yonna\Database\Driver\Mongo\Client;
use Yonna\Throwable\Exception;

abstract class AbstractMDO extends AbstractDB
{

    /**
     * @var string
     */
    protected $collection = null;

    /**
     * 架构函数 取得模板对象实例
     * @access public
     * @param array $setting
     */
    public function __construct(array $setting)
    {
        parent::__construct($setting);
        $this->collection = $setting['collection'];
    }

    /**
     * 析构函数
     */
    public function __destruct()
    {
        parent::__destruct();
    }

    /**
     * 获取 MDO
     * @return Client
     */
    protected function mdo()
    {
        return $this->malloc();
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
     * @param mixed ...$params
     * @return mixed
     * @throws Exception\DatabaseException
     */
    protected function query($command, ...$params)
    {
        $result = null;
        $commandStr = "un know command";

        $options = [];
        $session = $this->mdo()->getSession();
        if ($session) {
            $options['session'] = $session;
        }

        switch ($command) {
            case 'select':
                $filter = $params[0];
                $options = $params[1];
                /*
                $filter = ['x' => ['$gt' => 1]];
                $options = [
                    'projection' => ['_id' => 0],
                    'sort' => ['x' => -1],
                ];
                */
                $query = new Query($filter, $options);
                $cursor = $this->mdo()->getManager()->executeQuery($this->name . '.' . $this->collection, $query);
                $result = [];
                foreach ($cursor as $doc) {
                    var_dump($doc);
                    $result[] = $doc;
                }
                $filterStr = empty($filter) ? '{}' : json_encode($filter);
                $projectionStr = empty($options['projection']) ? '' : ',' . json_encode($options['projection']);
                $sortStr = empty($options['sort']) ? '' : '.sort(' . json_encode($options['sort']) . ')';
                $limitStr = empty($options['limit']) ? '' : '.limit(' . json_encode($options['limit']) . ')';
                $skipStr = empty($options['skip']) ? '' : '.skip(' . json_encode($options['skip']) . ')';
                $commandStr = "db.{$this->collection}find(";
                $commandStr .= $filterStr . $projectionStr;
                $commandStr .= ')';
                $commandStr .= $sortStr;
                $commandStr .= $limitStr;
                $commandStr .= $skipStr;
                break;
            case 'insert':
                $data = $params[0];
                $bulk = new BulkWrite();
                try {
                    $bulk->insert($data);
                } catch (BulkWriteException $e) {
                    Exception::database($e->getMessage());
                }
                $result = $this->mdo()->getManager()->executeBulkWrite($this->name . '.' . $this->collection, $bulk, $options);
                $result = [
                    'ids' => $result->getUpsertedIds(),
                    'insert_count' => $result->getInsertedCount(),
                    'bulk_count' => $bulk->count(),
                ];
                $commandStr = "db.{$this->collection}.insertOne(" . json_encode($params[0], JSON_UNESCAPED_UNICODE) . ')';
                break;
            case 'insertAll':
                $data = $params[0];
                $bulk = new BulkWrite();
                try {
                    foreach ($data as $d) {
                        $bulk->insert($d);
                    }
                } catch (BulkWriteException $e) {
                    Exception::database($e->getMessage());
                }
                $result = $this->mdo()->getManager()->executeBulkWrite($this->name . '.' . $this->collection, $bulk, $options);
                $result = [
                    'ids' => $result->getUpsertedIds(),
                    'insert_count' => $result->getInsertedCount(),
                    'bulk_count' => $bulk->count(),
                ];
                $commandStr = "db.{$this->collection}.insertMany(" . json_encode($params[0], JSON_UNESCAPED_UNICODE) . ')';
                break;
        }
        parent::query($commandStr);
        return $result;
    }


    public function test()
    {
        $query = ["_id" => ['$gte' => 0]];
        $cmd = new Command([
            'distinct' => 'color',
            'key' => 'color',
            'query' => $query
        ]);
        print_r($cmd);
        $row = $this->mdo()->getManager()->executeCommand("yonna", $cmd);
    }

}
