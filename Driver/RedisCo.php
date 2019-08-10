<?php

namespace Yonna\Database\Driver;

use Yonna\Throwable\Exception;

class RedisCo extends Redis
{

    protected $db_type = Type::REDIS_CO;

    /**
     * 架构函数 取得模板对象实例
     * @access public
     * @param array $setting
     * @throws Exception\DatabaseException
     */
    public function __construct(array $setting)
    {
        parent::__construct($setting);
        return $this;
    }

    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        parent::__destruct();
    }

}