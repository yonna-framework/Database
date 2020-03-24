<?php
/**
 * Mdo DB Client
 */

namespace Yonna\Database\Driver\Mdo;

use MongoDB\Driver\Manager;
use MongoDB\Driver\Session;

class Client
{

    private $manager = null;

    private $session = null;

    private $replica = false;

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

    /**
     * @return bool
     */
    public function isReplica(): bool
    {
        return $this->replica;
    }

    /**
     * @param bool $replica
     */
    public function setReplica(bool $replica): void
    {
        $this->replica = $replica;
    }

}
