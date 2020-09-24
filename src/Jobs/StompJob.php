<?php

namespace Mayconbordin\L5StompQueue\Jobs;

use Stomp\Transport\Frame;
use Mayconbordin\L5StompQueue\StompQueue;
use Illuminate\Support\Arr;
use Illuminate\Container\Container;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Contracts\Queue\Job as JobContract;

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
     * Create a new job instance.
     *
     * @param Container $container
     * @param StompQueue $stomp
     * @param Frame $job
     * @param string $jobClass
     */
    public function __construct(Container $container, StompQueue $stomp, Frame $job, string $connectionName, string $queue, $jobClass)
    {
        $this->job = $job;
        $this->stomp = $stomp;
        $this->container = $container;
        $this->queue = $queue;
        $this->connectionName = $connectionName;
        $this->jobClass = $jobClass;
    }

    /**
     * Fire the job.
     *
     * @return void
     */
    public function fire()
    {
        $destination = $this->job->getHeaders()['destination'];
        $body = json_decode($this->getRawBody(), true);

        $jobInstance = new $this->jobClass(
            $destination,
            $body
        );

        $jobInstance->handle();
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
     * @param  int $delay
     * @return void
     */
    public function release($delay = 0)
    {
        parent::release($delay);
        $this->recreateJob($delay);
    }

    /**
     * Release a pushed job back onto the queue.
     *
     * @param  int  $delay
     * @return void
     */
    protected function recreateJob($delay)
    {
        $payload = json_decode($this->job->body, true);
        Arr::set($payload, 'attempts', Arr::get($payload, 'attempts', 1) + 1);

        $this->stomp->recreate(json_encode($payload), $this->getQueue(), $delay);
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
        return Arr::get(json_decode($this->job->body, true), 'job');
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
     * @param  \Throwable|null  $e
     * @return void
     */
    protected function failed($e)
    {
        $payload = $this->getRawBody();

        if (method_exists($this->instance = $this->jobClass, 'failed')) {
            $this->instance->failed($payload, $e);
        }
    }
}
