<?php
/**
 * src Driver Types
 */

namespace Yonna\Database\Driver;

use Yonna\Mapping\Mapping;

class Type extends Mapping
{

    const MYSQL = 'Mysql';
    const PGSQL = 'Pgsql';
    const MSSQL = 'Mssql';
    const SQLITE = 'Sqlite';
    const MONGO = 'Mongo';
    const REDIS = 'Redis';
    const REDIS_CO = 'RedisCo';

    public static function array()
    {
        return [
            self::MYSQL,
            self::PGSQL,
            self::MSSQL,
            self::SQLITE,
            self::MONGO,
            self::REDIS,
            self::REDIS_CO,
        ];
    }

}