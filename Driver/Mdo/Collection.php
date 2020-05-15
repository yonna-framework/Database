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
     * 限定字段
     * @param $field
     * @param bool $except 排除模式
     * @return $this
     */
    public function field($field, $except = false): self
    {
        if (is_string($field)) {
            $field = explode(',', $field);
        }
        $this->options['projection'] = [];
        if (is_array($field)) {
            $field = array_filter($field);
            foreach ($field as $f) {
                $f = trim($f);
                $this->options['projection'][] = [$f => $except ? 0 : 1];
            }
        }
        return $this;
    }

    /**
     * @param $group
     * @return Collection
     */
    public function groupBy($group): self
    {
        $this->options['aggregate'] = $group;
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
            $orderBy = explode(',', $orderBy);
        }
        if (is_array($orderBy)) {
            $orderBy = array_filter($orderBy);
            foreach ($orderBy as $o) {
                $o = explode(' ', $o);
                if (count($o) > 1) {
                    $o[1] = strtolower($o[1]);
                    $orderBy[$o[0]] = $o[1];
                    $this->options['sort'][$o[0]] = $o[1] === self::ASC ? 1 : -1;
                } else {
                    $this->options['sort'][$o[0]] = $sort === self::ASC ? 1 : -1;
                }
            }
        }
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
