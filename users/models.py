from django.db import models


class Subscriber(models.Model):
    create_date = models.DateTimeField(auto_now_add=True)
    email = models.EmailField(unique=True)
    gdpr_consent = models.BooleanField(default=False)

    def __str__(self):
        return self.email


class SubscriberSMS(models.Model):
    create_date = models.DateTimeField(auto_now_add=True)
    phone = models.CharField(unique=True, max_length=20)
    gdpr_consent = models.BooleanField(default=False)

    def __str__(self):
        return self.phone


class Client(models.Model):
    create_date = models.DateTimeField(auto_now_add=True)
    email = models.EmailField(unique=True)
    phone = models.CharField(unique=True, max_length=20)

    def __str__(self):
        return self.email


class User(models.Model):
    create_date = models.DateTimeField(auto_now_add=True)
    email = models.EmailField(unique=True)
    phone = models.CharField(unique=True, max_length=20)
    gdpr_consent = models.BooleanField(default=False)

    def __str__(self):
        return self.email
