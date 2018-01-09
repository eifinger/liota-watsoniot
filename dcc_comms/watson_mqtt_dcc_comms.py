# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------#
#  Copyright © 2015-2016 VMware, Inc. All Rights Reserved.                    #
#                                                                             #
#  Licensed under the BSD 2-Clause License (the “License”); you may not use   #
#  this file except in compliance with the License.                           #
#                                                                             #
#  The BSD 2-Clause License                                                   #
#                                                                             #
#  Redistribution and use in source and binary forms, with or without         #
#  modification, are permitted provided that the following conditions are met:#
#                                                                             #
#  - Redistributions of source code must retain the above copyright notice,   #
#      this list of conditions and the following disclaimer.                  #
#                                                                             #
#  - Redistributions in binary form must reproduce the above copyright        #
#      notice, this list of conditions and the following disclaimer in the    #
#      documentation and/or other materials provided with the distribution.   #
#                                                                             #
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"#
#  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE  #
#  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE #
#  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE  #
#  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR        #
#  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF       #
#  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS   #
#  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN    #
#  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)    #
#  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF     #
#  THE POSSIBILITY OF SUCH DAMAGE.                                            #
# ----------------------------------------------------------------------------#

import logging
import Queue

from liota.dcc_comms.dcc_comms import DCCComms
import ibmiotf.device
#import ibmiotf.device
from liota.lib.utilities.utility import systemUUID

log = logging.getLogger(__name__)


class WatsonMqttDccComms(DCCComms):
    """
    DccComms for MQTT Transport
    """

    def __init__(self, organization, deviceType, deviceId, authKey, authToken):

        """
        """

        self.userdata = Queue.Queue()

        self.organization = organization
        self.deviceType = deviceType
        self.deviceId = deviceId
        self.authKey = authKey
        self.authToken = authToken
        self._connect()

    def _connect(self):
        """
        Initializes Mqtt Transport and connects to MQTT broker.
        :return:
        """
        deviceOptions = {"org": self.organization, "type": self.deviceType, "id": self.deviceId, "auth-method": "token",
                         "auth-token": self.authToken}
        print deviceOptions
        self.client = ibmiotf.device.Client(deviceOptions)
        print "Connecting now"
        print deviceOptions
        #options = {"org": self.organization, "id": "mytest", "auth-method": "apikey", "auth-key": self.authKey,
        #           "auth-token": self.authToken}
        #self.client = ibmiotf.application.Client(options)

        self.client.connect()


    def _disconnect(self):
        """
        Disconnects from MQTT broker.
        :return:
        """
        self.client.disconnect()

    def receive(self, msg_attr):
        """
        Subscribes to a topic with specified QoS and callback.
        Set call back to receive_message method if no callback method is passed by user.

        :param msg_attr: MqttMessagingAttributes Object
        :return:
        """
        callback = msg_attr.sub_callback if msg_attr and msg_attr.sub_callback else self.receive_message
        self.client.subscribe(msg_attr.sub_topic, msg_attr.sub_qos, callback)

    def receive_message(self, client, userdata, msg):
        """
           Receives message during MQTT subscription and put it in the queue.
           This queue can be used to get message in DCC but remember to dequeue

           :param msg_attr: MqttMessagingAttributes Object, userdata as queue
           :return:
           """
        userdata.put(str(msg.payload))

    def send(self, message, msg_attr):
        """
        Publishes message to MQTT broker.
        If mess_attr is None, then self.mess_attr will be used.

        :param message: Message to be published
        :param msg_attr: MqttMessagingAttributes Object
        :return:
        """
        self.client.publishEvent(msg_attr.pub_topic, "json", message)
