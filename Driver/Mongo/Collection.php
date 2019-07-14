<?php
/**
 * 数据库连接构建类，依赖 PDO_MYSQL 扩展
 * mysql version >= 5.7
 */

namespace Yonna\Database\Driver\Mongo;

use Yonna\Database\Driver\AbstractMDO;
use Yonna\Database\Driver\Type;

class Collection extends AbstractMDO
{

    protected $db_type = Type::MONGO;

}
