<?php

namespace Yonna\Database\Driver;

use MongoDB\Driver\Command;
use MongoDB\Driver\Query;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Exception\BulkWriteException;
use Yonna\Database\Driver\Mdo\Client;
use Yonna\Throwable\Exception;

/**
 * Class AbstractMDO
 * @package Yonna\Database\Driver
 * @see https://docs.mongodb.com/ecosystem/drivers/
 */
abstract class AbstractMDO extends AbstractDB
{

    /**
     * filter -> where
     * @var array
     */
    protected $filter = [];

    /**
     * @var array
     */
    protected $data = [];

    /**
     * 架构函数 取得模板对象实例
     * @access public
     * @param array $options
     */
    public function __construct(array $options)
    {
        parent::__construct($options);
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
        return $this->options['collection'];
    }

    /**
     * where分析
     * 这个where需要被继承的where覆盖才会有效
     * @return array
     */
    protected function parseWhere()
    {
        return [];
    }

    /**
     * value分析
     * @access protected
     * @param $filter
     * @return string
     */
    protected function getFilterStr($filter)
    {
        return '{}';
    }

    /**
     * @param $cursor
     * @return array
     */
    private function resultFormat($cursor)
    {
        $result = [];
        foreach ($cursor as $doc) {
            $temp = [];
            $doc = (array)$doc;
            foreach ($doc as $field => $d) {
                if ($field === "_id") {
                    $_id = $d->jsonSerialize();
                    $temp['_id'] = $_id['$oid'];
                } else {
                    $temp[$this->getCollection() . '_' . $field] = $d;
                }
            }
            $result[] = $temp;
        }
        return $result;
    }

    /**
     * 设置执行命令
     * @param $command
     * @return mixed
     * @throws Exception\DatabaseException
     */
    protected function query($command)
    {
        $result = null;
        $commandStr = "un know command";

        $mdoOps = [];
        $session = $this->mdo()->getSession();
        if ($session) {
            $mdoOps['session'] = $session;
        }

        try {
            switch ($command) {
                case 'drop':
                    $command = new Command([
                        'drop' => $this->options['collection'],
                    ]);
                    $res = $this->mdo()->getManager()->executeCommand($this->name, $command);
                    $res = current($res->toArray());
                    $result = $res->ok === 1;
                    $commandStr = "db.{$this->options['collection']}.drop()";
                    break;
                case 'count':
                    $filter = $this->parseWhere();
                    $command = new Command([
                        'count' => $this->options['collection'],
                        'query' => $filter ?: null,
                    ]);
                    $res = $this->mdo()->getManager()->executeCommand($this->name, $command);
                    $res = current($res->toArray());
                    $result = $res->n;
                    $commandStr = "db.{$this->options['collection']}.find(";
                    $commandStr .= $this->getFilterStr($filter);
                    $commandStr .= ').count()';
                    break;
                case 'select':
                    $filter = $this->parseWhere();
                    $query = new Query($filter, $this->options);
                    $cursor = $this->mdo()->getManager()->executeQuery($this->name . '.' . $this->options['collection'], $query);
                    $result = $this->resultFormat($cursor);
                    $projectionStr = empty($this->options['projection']) ? '' : ',' . json_encode($this->options['projection']);
                    $sortStr = empty($this->options['sort']) ? '' : '.sort(' . json_encode($this->options['sort']) . ')';
                    $limitStr = empty($this->options['limit']) ? '' : '.limit(' . json_encode($this->options['limit']) . ')';
                    $skipStr = empty($this->options['skip']) ? '' : '.skip(' . json_encode($this->options['skip']) . ')';
                    $commandStr = "db.{$this->options['collection']}.find(";
                    $commandStr .= $this->getFilterStr($filter) . $projectionStr;
                    $commandStr .= ')';
                    $commandStr .= $sortStr . $limitStr . $skipStr;
                    break;
                case 'insert':
                    if (empty($this->data)) {
                        return false;
                    }
                    $bulk = new BulkWrite();
                    $bulk->insert($this->data);
                    $result = $this->mdo()->getManager()->executeBulkWrite($this->name . '.' . $this->options['collection'], $bulk, $mdoOps);
                    $result = [
                        'ids' => $result->getUpsertedIds(),
                        'insert_count' => $result->getInsertedCount(),
                        'bulk_count' => $bulk->count(),
                    ];
                    $commandStr = "db.{$this->options['collection']}.insertOne(" . json_encode($this->data, JSON_UNESCAPED_UNICODE) . ')';
                    break;
                case 'insertAll':
                    if (empty($this->data)) {
                        return false;
                    }
                    $bulk = new BulkWrite();
                    foreach ($this->data as $d) {
                        $bulk->insert($d);
                    }
                    $result = $this->mdo()->getManager()->executeBulkWrite($this->name . '.' . $this->options['collection'], $bulk, $mdoOps);
                    $result = [
                        'insert_count' => $result->getInsertedCount(),
                        'bulk_count' => $bulk->count(),
                    ];
                    $commandStr = "db.{$this->options['collection']}.insertMany(" . json_encode($this->data, JSON_UNESCAPED_UNICODE) . ')';
                    break;
                case 'update':
                    if (empty($this->data)) {
                        return false;
                    }
                    $filter = $this->parseWhere();
                    $bulk = new BulkWrite();
                    $bulk->update($filter, ['$set' => $this->data], [
                        'multi' => true
                    ]);
                    $result = $this->mdo()->getManager()->executeBulkWrite($this->name . '.' . $this->options['collection'], $bulk, $mdoOps);
                    $result = [
                        'update_count' => $result->getUpsertedCount(),
                        'bulk_count' => $bulk->count(),
                    ];
                    $commandStr = "db.{$this->options['collection']}.update("
                        . $this->getFilterStr($filter) . ',{$set:'
                        . json_encode($this->data, JSON_UNESCAPED_UNICODE) . ',{multi:true})';
                    break;
                case 'delete':
                    $filter = $this->parseWhere();
                    $bulk = new BulkWrite();
                    $bulk->delete($filter, [
                        'limit' => false
                    ]);
                    $result = $this->mdo()->getManager()->executeBulkWrite($this->name . '.' . $this->options['collection'], $bulk, $mdoOps);
                    $result = [
                        'delete_count' => $result->getDeletedCount(),
                        'bulk_count' => $bulk->count(),
                    ];
                    $commandStr = "db.{$this->options['collection']}.delete("
                        . $this->getFilterStr($filter) . ',{limit:false})';
                    break;
            }
        } catch (BulkWriteException $e) {
            Exception::database($e->getMessage());
        } catch (\MongoDB\Driver\Exception\Exception $e) {
            Exception::database($e->getMessage());
        }
        parent::query($commandStr);
        return $result;
    }

}
