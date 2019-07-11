<?php

namespace Yonna\Database\Driver;

use Exception;
use Yonna\Database\Driver\Mongo\Collection;

class Mongo
{

    private $setting = null;

    /**
     * 构造方法
     *
     * @param array $setting
     */
    public function __construct(array $setting)
    {
        $this->setting = $setting;
    }

    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        $this->setting = null;
    }

    /**
     * @param string $collection
     * @return Collection count
     * @throws Exception
     */
    public function collection(string $collection): Collection
    {
        if (empty($collection)) {
            \Yonna\Throwable\Exception::database('collection error');
        }
        $this->setting['collection'] = $collection;
        return (new Collection($this->setting));
    }

}