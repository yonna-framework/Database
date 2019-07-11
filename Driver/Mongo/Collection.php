<?php
/**
 * 数据库连接构建类，依赖 PDO_MYSQL 扩展
 * mysql version >= 5.7
 */

namespace Yonna\Database\Driver\Mongo;

use Yonna\Throwable\Exception;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB;
use Yonna\Database\Driver\AbstractDB;
use Yonna\Database\Driver\Type;

class Collection extends AbstractDB
{

    protected $db_type = Type::MONGO;

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
     * @param mixed ...$options
     */
    private function setCommand(...$options)
    {
        $backtrace = debug_backtrace();
        $func_name = $backtrace[1]['function'];
        $command = null;
        switch ($func_name) {
            case 'insert':
                $command = "db.{$this->collection}.insertOne(" . json_encode($options[0], JSON_UNESCAPED_UNICODE) . ')';
                break;
            case 'insertAll':
                $command = "db.{$this->collection}.insertMany(" . json_encode($options[0], JSON_UNESCAPED_UNICODE) . ')';
                break;
            default:
                break;
        }
        parent::query($command);
    }

    /**
     * @param array $data
     * @return int count
     * @throws Exception\DatabaseException
     */
    public function insert(array $data): int
    {
        $this->setCommand($data);
        $bulk = new BulkWrite();
        $bulk->insert($data);
        $count = 0;
        try {
            $result = $this->mongoManager->executeBulkWrite($this->name . '.' . $this->collection, $bulk);
            $ids = $result->getUpsertedIds();
            print_r($ids);
            $count = $result->getInsertedCount();
        } catch (BulkWriteException $e) {
            Exception::database($e->getMessage());
        }
        return $count;
    }

    /**
     * @param array $data
     * @return int count
     * @throws Exception\DatabaseException
     */
    public function insertAll(array $data): int
    {
        $this->setCommand($data);
        $bulk = new BulkWrite();
        foreach ($data as $d) {
            $bulk->insert($d);
        }
        $count = 0;
        try {
            $result = $this->mongoManager->executeBulkWrite($this->name . '.' . $this->collection, $bulk);
            // $ids = $result->getUpsertedIds();
            $count = $result->getInsertedCount();
        } catch (BulkWriteException $e) {
            Exception::database($e->getMessage());
        }
        return $count;
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
