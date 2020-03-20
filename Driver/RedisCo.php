<?php

namespace Yonna\Database\Driver;

use Yonna\Throwable\Exception;

class RedisCo extends Redis
{

    /**
     * 架构函数 取得模板对象实例
     * @param array $setting
     */
    public function __construct(array $setting)
    {
        $options['db_type'] = Type::REDIS_CO;
        parent::__construct($setting);
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