<?php

namespace Yonna\Database\Driver\Mdo;

use Yonna\Throwable\Exception;

/**
 * Trait TraitOperat
 * @package Yonna\Database\Driver\Mdo
 */
trait TraitOperat
{

    /**
     * @param int $skip
     * @return Collection
     */
    public function offset(int $skip): self
    {
        $this->options['skip'] = $skip;
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
     * @return mixed
     */
    public function multi()
    {
        return $this->query('select');
    }

    /**
     * 查找记录一条
     * @return mixed
     */
    public function one()
    {
        $this->limit(1);
        $result = $this->multi();
        return $result && is_array($result) ? reset($result) : $result;
    }

    /**
     * 分页查找
     * @param int $current
     * @param int $per
     * @return mixed
     */
    public function page($current = 1, $per = 10)
    {
        $count = $this->count();
        $limit = (int)$per;
        $offset = (int)($current - 1) * $limit;
        $this->offset($offset);
        $this->limit($limit);
        $data = $this->multi();
        $result = [];
        $per = !$per ? 10 : $per;
        $last = ceil($count / $per);
        $result['list'] = $data;
        $result['page']['total'] = $count;
        $result['page']['per'] = $per;
        $result['page']['current'] = (int)$current;
        $result['page']['last'] = (int)$last;
        return $result;
    }

    /**
     * 统计
     * @return int
     */
    public function count()
    {
        return $this->query('count');
    }


    /**
     * insert
     * @param $data
     * @return mixed
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
     */
    public function insertAll($data)
    {
        $this->data = $data;
        return $this->query('insertAll');
    }

    /**
     * update
     * @param $data
     * @param bool $sure
     * @return mixed
     * @throws Exception\DatabaseException
     */
    public function update($data, $sure = false)
    {
        $where = $this->parseWhere();
        if (!$where && !$sure) {
            Exception::database('Mongo update must be sure when without where');
        }
        $this->data = $data;
        return $this->query('update');
    }

    /**
     * delete
     * @param bool $sure
     * @return mixed
     * @throws Exception\DatabaseException
     */
    public function delete($sure = false)
    {
        $where = $this->parseWhere();
        if (!$where && !$sure) {
            Exception::database('Mongo delete must be sure when without where');
        }
        return $this->query('delete');
    }

}
