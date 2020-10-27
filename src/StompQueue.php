<?php

namespace Mayconbordin\L5StompQueue;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Facades\Log;
use Mayconbordin\L5StompQueue\Jobs\StompJob;
use Stomp\Exception\ConnectionException;
use Stomp\Network\Observer\Exception\HeartbeatException;
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

    protected $credentials;
    private $stompConfig;

    public function __construct(Stomp $stomp, $default, array $stompConfig, $system = null, array $credentials = [])
    {
        $this->stomp = $stomp;
        $this->default = $default;
        $this->stompConfig = $stompConfig;
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
        return $this->pushRaw($this->createPayload($job, $data), $this->getQueue($queue));
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
        $allQueues = array_keys($this->stompConfig['queues'] ?? []);

        $allQueues[] = $this->getQueue();
        foreach ($allQueues as $queueItem) {
            if (!$this->isAlreadySubscribed($queueItem)) {
                $this->getStomp()->subscribe($this->getQueue($queueItem), null, "client");
            }
        }

        $job = null;

        try {
            $job = $this->getStomp()->read();
        } catch (ConnectionException $connectionException) {
            Log::info("Connection broken: " . $connectionException->getMessage());
            Log::info("exiting");
            exit(1);

        } catch (HeartbeatException $heartbeatException) {
            Log::info("Heartbeat exception: " . $heartbeatException->getMessage());
            Log::info("exiting");
            exit(1);
        }

        if (!is_null($job) && ($job instanceof Frame)) {
            return new StompJob($this->container, $this, $job, $this->connectionName, $queue, $this->stompConfig);
        }
    }

    /**
     * Determines if a queue is subscribed
     * @param $queue
     * @return bool
     */
    public function isAlreadySubscribed($queue): bool
    {
        $subscriptions = $this->getStomp()->getSubscriptions();

        foreach ($subscriptions as $subscription) {
            if ($subscription->getDestination() === $queue) {
                return true;
            }
        }
        return false;
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
//        $this->getStomp()->ack($message);
    }

    /**
     * Get the queue or return the default.
     *
     * @param string|null $queue
     * @return string
     */
    public function getQueue($queue = null)
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
