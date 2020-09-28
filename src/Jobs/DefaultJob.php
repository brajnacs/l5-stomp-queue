<?php

namespace Mayconbordin\L5StompQueue\Jobs;

use Illuminate\Contracts\Queue\ShouldQueue;
use Mayconbordin\L5StompQueue\Exceptions\NoJobMapped;

class DefaultJob implements ShouldQueue
{

    public $queue;
    public $message;

    public function __construct(string $queue, array $message)
    {
        $this->queue = $queue;
        $this->message = $message;
    }

    public function handle()
    {
        throw new NoJobMapped($this->queue);
    }

}
