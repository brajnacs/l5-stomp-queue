<?php

namespace Mayconbordin\L5StompQueue\Jobs;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Queue\Jobs\JobName;
use Illuminate\Support\Arr;
use Mayconbordin\L5StompQueue\StompQueue;
use Stomp\Transport\Frame;

/**
 * Class StompJob
 * @package Mayconbordin\L5StompQueue\Jobs
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
class StompJob extends Job implements JobContract
{
    /**
     * The Stomp instance.
     *
     * @var StompQueue
     */
    protected $stomp;

    /**
     * The Stomp message instance.
     *
     * @var Frame
     */
    protected $job;

    /**
     * Application specific config
     * @var array
     */
    protected $stompConfig;

    /**
     * Create a new job instance.
     *
     * @param Container $container
     * @param StompQueue $stomp
     * @param Frame $job
     * @param string $connectionName
     * @param string $queue
     * @param array $stompConfig
     */
    public function __construct(Container $container, StompQueue $stomp, Frame $job, string $connectionName, string $queue, array $stompConfig)
    {
        $this->container = $container;
        $this->stomp = $stomp;
        $this->job = $job;
        $this->connectionName = $connectionName;
        $this->queue = $queue;
        $this->stompConfig = $stompConfig;
    }

    /**
     * Fire the job.
     *
     * @return void
     */
    public function fire()
    {
        $payload = $this->payload();

        if ($this->isLaravelJob($payload)) {
            [$class, $method] = JobName::parse($payload['job']);
            ($this->instance = $this->resolve($class))->{$method}($this, $payload['data']);
        } else {
            $destination = $this->job->getHeaders()['destination'];
            $jobClass = $this->resolveJob($this->stompConfig, $destination);
            $body = json_decode($this->getRawBody(), true) ?? $this->getRawBody();

            $jobInstance = new $jobClass(
                $destination,
                $body
            );

            $jobInstance->handle();
        }

        $this->ack();
    }

    public function isLaravelJob($payload)
    {
        try {
            [$class, $method] = JobName::parse($payload['job']);
            return class_exists($class);
        } catch (\Exception $e) {
            // do nothing
        }

        return false;
    }

    public function resolveJob($config, $destination)
    {
        $queues = Arr::get($config, 'queues', []);
        return $queues[$destination] ?? $this->getDefaultJob($config);
    }

    private function getDefaultJob($config)
    {
        return $config['default-job'] ?? DefaultJob::class;
    }

    /**
     * Delete the job from the queue.
     *
     * @return void
     */
    public function delete()
    {
        parent::delete();

        $this->stomp->deleteMessage($this->getQueue(), $this->job);
    }

    /**
     * Release the job back into the queue.
     *
     * @param int $delay
     * @return void
     */
    public function release($delay = 0)
    {
        $this->stomp->getStomp()->nack($this->job);
        parent::release($delay);
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
     */
    public function attempts()
    {
        return Arr::get(json_decode($this->job->body, true), 'attempts', 1);
    }

    /**
     * Get the name of the queued job class.
     *
     * @return string
     */
    public function getName()
    {
        if ($this->isLaravelJob($this->payload())) {
            return Arr::get(json_decode($this->job->body, true), 'job');
        }

        $destination = $this->job->getHeaders()['destination'];
        return $this->resolveJob($this->stompConfig, $destination);
    }

    /**
     * Get the name of the queue the job belongs to.
     *
     * @return string
     */
    public function getQueue()
    {
        return $this->queue;
    }

    /**
     * Get the raw body string for the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        return $this->job->body;
    }

    public function getJobId()
    {
        return $this->job->getHeaders()['message-id'];
    }

    /**
     * Process an exception that caused the job to fail.
     *
     * @param \Throwable|null $e
     * @return void
     */
    protected function failed($e)
    {
        $this->stomp->getStomp()->nack($this->job);
        $payload = $this->getRawBody();

        $destination = $this->job->getHeaders()['destination'];
        $jobClass = $this->resolveJob($this->stompConfig, $destination);

        if (method_exists($this->instance = $jobClass, 'failed')) {
            $this->instance->failed($payload, $e);
        }
    }

    public function ack()
    {
        $this->stomp->getStomp()->ack($this->job);
    }
}
