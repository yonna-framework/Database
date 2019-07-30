<?php


namespace Yonna\Database\Support;

/**
 * 数据库记录
 * Class Support
 * @package Yonna\Database\Support
 */
abstract class Support
{

    /**
     * uuid
     */
    public static function uuid()
    {
        return $_ENV['UUID'] ?? $_SERVER['HTTP_USER_AGENT'] ?? 0;
    }

}
