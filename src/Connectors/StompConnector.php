<?php

namespace Mayconbordin\L5StompQueue\Connectors;

use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Support\Arr;
use Mayconbordin\L5StompQueue\StompQueue;
use Stomp\Client;
use Stomp\StatefulStomp;

/**
 * Class StompConnector
 * @package Mayconbordin\L5StompQueue\Connectors
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 * @author Nicholas Finzer <nfinzer@gmail.com>
 */
class StompConnector implements ConnectorInterface
{
    /**
     * Establish a queue connection.
     *
     * @param array $config
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config)
    {
        $stomp = new StatefulStomp(new Client($config['broker_url']));
        //$stomp->sync         = Arr::get($config, 'sync', false);
        //$stomp->prefetchSize = Arr::get($config, 'prefetchSize', 1);
        //$stomp->clientId     = Arr::get($config, 'clientId', null);

        return new StompQueue(
            $stomp,
            $config['queue'],
            $config['stomp-config'],
            Arr::get($config, 'system', null),
            [
                'username' => Arr::get($config, 'username', ''),
                'password' => Arr::get($config, 'password', '')
            ]
        );
    }
}
