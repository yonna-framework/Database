<?php

namespace Yonna\Database\Driver\Mdo;

/**
 * Trait TraitOperat
 * @package Yonna\Database\Driver\Mdo
 */
trait TraitOperat
{

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

}
