from django.contrib import admin
from .models import Subscriber, User, SubscriberSMS, Client

admin.site.register([Subscriber, User, Client, SubscriberSMS])
