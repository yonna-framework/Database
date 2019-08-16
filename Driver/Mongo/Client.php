<?php
/**
 * Mongo DB Client
 */

namespace Yonna\Database\Driver\Mongo;

use MongoDB\Driver\Manager;
use MongoDB\Driver\Session;

class Client
{

    private $manager = null;

    private $session = null;

    /**
     * @return Manager | null
     */
    public function getManager()
    {
        return $this->manager;
    }

    /**
     * @param Manager | null $manager
     */
    public function setManager($manager): void
    {
        $this->manager = $manager;
    }

    /**
     * @return Session | null
     */
    public function getSession()
    {
        return $this->session;
    }

    /**
     * @param Session | null $session
     */
    public function setSession($session): void
    {
        $this->session = $session;
    }


}
