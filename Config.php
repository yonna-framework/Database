<?php

namespace Yonna\Database;

use Exception;
use Yonna\Database\Driver\Type;
use Yonna\Foundation\System;

class Config
{

    protected static $config = array();

    /**
     * @return array
     */
    public static function fetch(): array
    {
        return static::$config;
    }

    /**
     * @param $key
     * @param null $default
     * @return array|bool|false|string|null
     */
    public static function env($key, $default = null)
    {
        return System::env($key, $default);
    }

    /**
     * @param string $tag
     * @param array $setting
     * @throws null
     */
    private static function set(string $tag, array $setting)
    {
        $type = $setting['type'] ?? null;
        $host = $setting['host'] ?? null;
        $port = $setting['port'] ?? null;
        $account = $setting['account'] ?? null;
        $password = $setting['password'] ?? null;
        $name = $setting['name'] ?? null;
        $replica = $setting['replica'] ?? null;
        $charset = $setting['charset'] ?? null;
        $schemas = $setting['schemas'] ?? null;
        $project_key = isset($setting['project_key']) ? strtolower($setting['project_key']) : null;
        $auto_cache = isset($setting['auto_cache']) ? strtolower($setting['auto_cache']) : false;
        $auto_crypto = isset($setting['auto_crypto']) ? strtolower($setting['auto_crypto']) : false;
        $crypto_type = $setting['crypto_type'] ?? null;
        $crypto_secret = $setting['crypto_secret'] ?? null;
        $crypto_iv = $setting['crypto_iv'] ?? null;
        // check
        if (empty($type)) {
            throw new Exception('no type');
        }
        if ($type === Type::MYSQL || $type === Type::PGSQL || $type === Type::MSSQL || $type === Type::MONGO || $type === Type::REDIS) {
            if (empty($host)) {
                throw new Exception('no host');
            }
            if (empty($port)) {
                throw new Exception('no port');
            }
        }
        if ($type === Type::MYSQL || $type === Type::PGSQL || $type === Type::MSSQL) {
            if (empty($account)) {
                throw new Exception('no account');
            }
            if (empty($password)) {
                throw new Exception('no password');
            }
        }
        if ($type === Type::SQLITE) {
            if (empty($host)) {
                throw new Exception('no host file');
            }
        }
        // auto_cache
        if ($auto_cache === 'true' || $auto_cache === 'false') {
            $auto_cache = $auto_cache === 'true';
        } elseif (is_numeric($auto_cache)) {
            $auto_cache = (int)$auto_cache;
        }
        // auto_crypto
        if ($auto_crypto === 'true' || $auto_crypto === 'false') {
            $auto_crypto = $auto_crypto === 'true';
            if ($auto_crypto === true) {
                if (empty($crypto_type)) {
                    throw new Exception('no crypto_type');
                }
                if (empty($crypto_secret)) {
                    throw new Exception('no crypto_secret');
                }
                if (empty($crypto_iv)) {
                    throw new Exception('no crypto_iv');
                }
                if (!in_array($crypto_type, System::getOpensslCipherMethods())) {
                    throw new Exception("OpensslCipherMethods not support this type: {$crypto_type}");
                }
            }
        }
        static::$config[$tag] = [
            'type' => $type,
            'host' => $host,
            'port' => $port,
            'account' => $account,
            'password' => $password,
            'name' => $name,
            'replica' => $replica,
            'charset' => $charset,
            'schemas' => $schemas,
            'project_key' => $project_key,
            'auto_cache' => $auto_cache,
            'auto_crypto' => $auto_crypto,
            'crypto_type' => $crypto_type,
            'crypto_secret' => $crypto_secret,
            'crypto_iv' => $crypto_iv,
        ];
    }

    public static function mysql(string $tag, array $setting)
    {
        $setting['type'] = Type::MYSQL;
        static::set($tag, $setting);
    }

    public static function pgsql(string $tag, array $setting)
    {
        $setting['type'] = Type::PGSQL;
        static::set($tag, $setting);
    }

    public static function mssql(string $tag, array $setting)
    {
        $setting['type'] = Type::MSSQL;
        static::set($tag, $setting);
    }

    public static function sqlite(string $tag, array $setting)
    {
        $setting['type'] = Type::SQLITE;
        static::set($tag, $setting);
    }

    public static function mongo(string $tag, array $setting)
    {
        $setting['type'] = Type::MONGO;
        static::set($tag, $setting);
    }

    public static function redis(string $tag, array $setting)
    {
        $setting['type'] = Type::REDIS;
        $setting['auto_cache'] = 'false';
        static::set($tag, $setting);
    }

}