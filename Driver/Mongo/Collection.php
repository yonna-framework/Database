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


    public function groupBy(): self
    {
        return $this;
    }

    /**
     * @param $orderBy
     * @param string $sort
     * @return Collection
     */
    public function orderBy($orderBy, $sort = self::ASC): self
    {
        if (!$orderBy) {
            return $this;
        }
        if (is_string($orderBy)) {
            $sort = strtolower($sort);
            $this->options['sort'][$orderBy] = $sort === self::ASC ? 1 : -1;
        } elseif (is_array($orderBy)) {
            $orderBy = array_filter($orderBy);
            foreach ($orderBy as $v) {
                $orderInfo = explode(' ', $v);
                $orderInfo[1] = strtolower($orderInfo[1]);
                $this->options['sort'][$orderInfo[0]] = $orderInfo[1] === self::ASC ? 1 : -1;
                unset($orderInfo);
            }
        }
        return $this;
    }

    /**
     * order by string 支持 field asc,field desc 形式
     * @param $orderBy
     * @return self
     */
    public function orderByStr($orderBy): self
    {
        $orderBy = explode(',', $orderBy);
        foreach ($orderBy as $o) {
            $o = explode(' ', $o);
            $o[1] = strtolower($o[1]);
            $this->options['sort'][$o[0]] = $o[1] === self::ASC ? 1 : -1;
        }
        return $this;
    }

    /**
     * @param int $limit
     * @return Collection
     */
    public function limit(int $limit): self
    {
        $this->options['limit'] = $limit;
        return $this;
    }

    /**
     * @param int $skip
     * @return Collection
     */
    public function offset(int $skip): self
    {
        $this->options['skip'] = $skip;
        return $this;
    }

    /** final operation */

    /**
     * @return mixed
     * @throws DatabaseException
     */
    public function multi()
    {
        return $this->query('select');
    }

    /**
     * insert
     * @param $data
     * @return mixed
     * @throws DatabaseException
     */
    public function insert($data)
    {
        $this->data = $data;
        return $this->query('insert');
    }

    /**
     * insert all
     * @param $data
     * @return mixed
     * @throws DatabaseException
     */
    public function insertAll($data)
    {
        $this->data = $data;
        return $this->query('insertAll');
    }

    /**
     * insert all
     * @return mixed
     */
    public function tt()
    {
        return $this->test();
    }

}
