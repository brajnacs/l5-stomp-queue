<?php

namespace Mayconbordin\L5StompQueue;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Mayconbordin\L5StompQueue\Jobs\StompJob;
use Stomp\StatefulStomp as Stomp;
use Stomp\Transport\Frame;
use Stomp\Transport\Message;

/**
 * Class StompQueue
 * @package Mayconbordin\L5StompQueue
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
class StompQueue extends Queue implements QueueContract
{
    const SYSTEM_ACTIVEMQ = "activemq";

    /**
     * The Stomp instance.
     *
     * @var Stomp
     */
    protected $stomp;

    /**
     * The name of the default queue.
     *
     * @var string
     */
    protected $default;

    /**
     * The system name.
     *
     * @var string
     */
    protected $system;

    /**
     * The Stomp credentials for connection.
     *
     * @var array
     */
    protected $credentials;

    /**
     * The Job Handler to execute
     *
     * @var string
     */
    protected $jobClass;

    /**
     * Create a new ActiveMQ queue instance.
     *
     * @param Stomp $stomp
     * @param string $default
     * @param string $jobClass
     * @param string|null $system
     * @param array $credentials [username=string, password=string]
     */
    public function __construct(Stomp $stomp, $default,  string $jobClass,  $system = null, array $credentials = [])
    {
        $this->stomp = $stomp;
        $this->default = $default;
        $this->jobClass = $jobClass;
        $this->system = $system;
        $this->credentials = $credentials;
    }

    /**
     * Push a new job onto the queue.
     *
     * @param string $job
     * @param mixed $data
     * @param string $queue
     * @return mixed
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param string $payload
     * @param string $queue
     * @param array $options
     * @return bool
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $message = new Message($payload);
        return $this->getStomp()->send($this->getQueue($queue), $message);
    }

    /**
     * Push a new job onto the queue after a delay.
     * @param $delay
     * @param $job
     * @param string $data
     * @param null $queue
     * @throws \Exception
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        throw new \Exception("Not implemented");
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param string $queue
     * @return StompJob|void
     */
    public function pop($queue = null)
    {
        if (!$this->isAlreadySubscribed($queue)) {
            $this->getStomp()->subscribe($this->getQueue($queue));
        }
        $job = $this->getStomp()->read();
        if (!is_null($job) && ($job instanceof Frame)) {
            return new StompJob($this->container, $this, $job, $this->connectionName, $queue, $this->jobClass);
        }
    }

    /**
     * Determines if a queue is subscribed
     * todo: check exact queue name
     * @param $queue
     * @return bool
     */
    public function isAlreadySubscribed($queue): bool
    {
        $subscriptions = $this->getStomp()->getSubscriptions();
        return $subscriptions->count() > 0;
    }

    /**
     * Delete a message from the Stomp queue.
     *
     * @param string $queue
     * @param string|Frame $message
     * @return void
     */
    public function deleteMessage($queue, Frame $message)
    {
        $this->getStomp()->ack($message);
    }

    /**
     * Get the queue or return the default.
     *
     * @param string|null $queue
     * @return string
     */
    public function getQueue($queue)
    {
        return $queue ?: $this->default;
    }

    /**
     * @return Stomp
     */
    public function getStomp()
    {
        /*
        if (!$this->stomp->isConnected()) {
            $this->stomp->connect(Arr::get($this->credentials, 'username', ''), Arr::get($this->credentials, 'password', ''));
        }
        */
        return $this->stomp;
    }

    /**
     * @param int $delay
     * @return array
     */
    protected function makeDelayHeader($delay)
    {
        $delay = $this->getSeconds($delay);

        if ($this->system == self::SYSTEM_ACTIVEMQ) {
            return ['AMQ_SCHEDULED_DELAY' => $delay * 1000];
        } else {
            return [];
        }
    }

    public function size($queue = null)
    {
        throw new \Exception("Not Implemented");
    }
}
