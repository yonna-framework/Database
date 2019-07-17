<?php
/**
 * 数据库连接构建类，依赖 PDO_MYSQL 扩展
 * mysql version >= 5.7
 */

namespace Yonna\Database\Driver\Mongo;

use Yonna\Database\Driver\AbstractMDO;
use Yonna\Database\Driver\Type;
use Yonna\Throwable\Exception\DatabaseException;

class Collection extends AbstractMDO
{

    protected $db_type = Type::MONGO;

    /**
     * insert
     * @param $data
     * @return mixed
     * @throws DatabaseException
     */
    public function insert($data)
    {
        return $this->query('insert', $data);
    }

    /**
     * insert all
     * @param $data
     * @return mixed
     * @throws DatabaseException
     */
    public function insertAll($data)
    {
        return $this->query('insertAll', $data);
    }

}
