# schedule_task
This is schedule task system written in Python, fulfilling the request of my boss.

The main objective of this system is to provide a flexible platform to manage, deploy, schedule, run script based task in a distributed way. This system will replace the way of deploying script task that everyone can upload a script and add a crontab to run his script. With this system fully functional, it will help the developers to develop, deploy, manage, execute scripts among all machines on a centrol console. 

The first version is a simple client side program. Main will read in conf files and set up rabbit-mq based listeners and controller. All queues share a same vhost operating in fanout mode, which means the messages are broadcasted to all queues. 

Each task would set up its own queue, and filter incoming messages or execute the message as configured. If a task is to be executed, a subprocess is spawned as executor, which then do the job, without spoiling the main process or listener thread.

The controller will simply check on controller message and rebuild task listeners as told. Further function includes redeployment of scripts, etc.

The rabbit-mq package here is written by me, serving as common rabbit-mq package among the company. It should be helpful to most use-cases.

There will be a counterpart working as manage center, to manage, distrubute, monitor tasks. With the help of rabbit mq, it can be done to simply watch each queue's status and get feedback on the execution result. This is gonna be done in the future as my boss now need me to work on an rpc system of our own... So now this client is just deployed on several machines and receives message from another python sending script. Not much of the managing utilitis has been operational.
