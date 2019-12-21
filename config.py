CONFIG = {'dev': '/dev/usb-ir-lesekopf0',
          'mqtt': {'enabled': True,
                   'host': 'localhost',
                   'port': 1883,
                   'keepalive': 60,
                   'auth': {'enabled': True, 'username': 'mqtt_user', 'password': 'mqtt_pwd' },
                   'topic': 'powermeter/reading',
                   'retain': True,
                   'qos': 0 } 
         } 
