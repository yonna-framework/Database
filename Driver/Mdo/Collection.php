<?php
/**
 * 数据库连接构建类，依赖 PDO_MYSQL 扩展
 * mysql version >= 5.7
 */

namespace Yonna\Database\Driver\Mdo;

use Yonna\Database\Driver\AbstractMDO;
use Yonna\Throwable\Exception\DatabaseException;

class Collection extends AbstractMDO
{
    use TraitOperat;
    use TraitWhere;

    /**
     * 构造方法
     *
     * @param array $options
     */
    public function __construct(array $options)
    {
        parent::__construct($options);
    }

    /**
     * @return Collection
     */
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
     * @param int $skip
     * @return Collection
     */
    public function offset(int $skip): self
    {
        $this->options['skip'] = $skip;
        return $this;
    }

    /**
     * 删除合集
     * @param bool $sure 确认执行，防止误操作
     * @return self
     * @throws DatabaseException
     */
    public function drop($sure = false)
    {
        if ($this->getCollection() && $sure === true) {
            return $this->query('drop');
        }
        return $this;
    }

}
