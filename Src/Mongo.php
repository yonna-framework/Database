<?php

namespace Yonna\Database;

use Exception;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB;

class Mongo extends AbstractDB
{

    protected $db_type = Type::MONGO;

    /**
     * @var MongoDB\Driver\Manager | null
     *
     */
    private $mongoManager = null;

    /**
     * 架构函数 取得模板对象实例
     * @access public
     * @param array $setting
     * @throws Exception
     */
    public function __construct(array $setting)
    {
        parent::__construct($setting);
        if ($this->mongoManager == null) {
            if (class_exists('\\MongoDB\Driver\Manager')) {
                try {
                    $this->mongoManager = new MongoDB\Driver\Manager($this->dsn());
                } catch (Exception $e) {
                    $this->mongoManager = null;
                    throw new Exception('MongoDB遇到问题或未安装，请暂时停用MongoDB以减少阻塞卡顿');
                }
            }
        }
        return $this;
    }

    public function __destruct()
    {
        parent::__destruct();
    }


    /**
     * @param array $data
     * @return int
     * @throws Exception
     */
    public function insert(array $data): int
    {
        $bulk = new BulkWrite();
        $bulk->insert($data);
        $count = 0;
        try {
            $result = $this->mongoManager->executeBulkWrite('ppm.test', $bulk);
            $count = $result->getInsertedCount();
        } catch (BulkWriteException $e) {
            throw new Exception($e->getMessage());
        }
        return $count;
    }

}