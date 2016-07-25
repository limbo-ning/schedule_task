#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import ScheduleMQ,sys


mq = ScheduleMQ.ScheduleMQHandler()

def send(scheduleType, scheduleId):
        msg = {

        }
        properties = {
                'type' : scheduleType,
                'subType' : scheduleId
        }
        mq.send_schedule_mq(msg,properties)

if __name__ == '__main__':
        if len(sys.argv) < 3:
                print   '''     usage: python SendSchedule.py scheduleType scheduleId
                                PROPERTIES_TYPE_SCHEDULE_INDEX          =       20#计算指数
                                PROPERTIES_TYPE_SCHEDULE_RULE           =       21#计算规则
                                PROPERTIES_TYPE_SCHEDULE_DUMP           =       22#定时dump
                                PROPERTIES_TYPE_SCHEDULE_EVENT          =       23#触发事件

                                PROPERTIES_TYPE_SCHEDULE_CONTROL        =       25

                                PROPERTIES_SUB_TYPE_SCHEDULE_ALL        =       -1#全部脚本

                        '''
        else:
                send(int(sys.argv[1]), int(sys.argv[2]))