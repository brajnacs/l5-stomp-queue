<?php

namespace Mayconbordin\L5StompQueue;

use Stomp\Client;
use Stomp\StatefulStomp as Stomp;
use Illuminate\Support\Arr;
use Illuminate\Support\ServiceProvider;
use Mayconbordin\L5StompQueue\Broadcasters\StompBroadcaster;
use Mayconbordin\L5StompQueue\Connectors\StompConnector;

/**
 * Class StompServiceProvider
 * @package Mayconbordin\L5StompQueue
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
class StompServiceProvider extends ServiceProvider
{
    /**
     * Indicates if loading of the provider is deferred.
     *
     * @var bool
     */
    protected $defer = false;

    /**
     * Add the connector to the queue drivers.
     *
     * @return void
     */
    public function boot()
    {
        $this->registerStompConnector($this->app['queue']);
        $this->registerStompBroadcaster($this->app->make('Illuminate\Broadcasting\BroadcastManager'));
    }

    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {

    }

    /**
     * Register the Stomp queue connector.
     *
     * @param \Illuminate\Queue\QueueManager $manager
     *
     * @return void
     */
    protected function registerStompConnector($manager)
    {
        $manager->addConnector('stomp', function() {
            return new StompConnector();
        });
    }

    /**
     * Register the Stomp queue broadcaster.
     *
     * @param \Illuminate\Broadcasting\BroadcastManager $manager
     */
    protected function registerStompBroadcaster($manager)
    {
        $manager->extend('stomp', function ($app, $config) {
            $stompClient = new Client($config['broker_url']);
            $username = Arr::get($config, 'username', null);
            $password = Arr::get($config, 'password', null);
            $stompClient->setLogin($username, $password);
            $stomp = new Stomp($stompClient);
            return new StompBroadcaster($stomp, compact('username', 'password'));
        });
    }

    /**
     * Get the services provided by the provider.
     *
     * @return array
     */
    public function provides()
    {
        return [];
    }
}
