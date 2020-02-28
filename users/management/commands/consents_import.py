import csv
import tempfile
from time import time
from django.apps import apps
from django.core.management.base import BaseCommand
from django.db import models
from django.db.models import Case, When, Value, F

from users.models import User, Subscriber, SubscriberSMS, Client


class Command(BaseCommand):
    help = 'Users import'

    subscribers_matched = \
        "select users_subscriber.email, " \
        "users_subscriber.gdpr_consent, " \
        "users_subscriber.id, " \
        "users_user.id as user_id " \
        "from users_subscriber " \
        "inner join users_user " \
        "on users_subscriber.email = users_user.email;"

    subscribers_sms_matched = \
        "select users_subscribersms.phone, " \
        "users_subscribersms.gdpr_consent, " \
        "users_subscribersms.id, " \
        "users_user.id as user_id " \
        "from users_subscribersms " \
        "inner join users_user " \
        "on users_subscribersms.phone = users_user.phone;"

    def handle(self, *args, **options):
        # conditions = {
        #     'subscriber': {
        #         'model': 'subscriber',
        #         'col1': 'email',
        #         'col2': 'phone'
        #     },
        #     'subscribersms': {
        #         'model': 'subscribersms',
        #         'col1': 'phone',
        #         'col2': 'email'
        #     }
        # }
        # for k, v in conditions.items():
        self.update_users_consents()

    def update_users_consents(self):
        subscribers = Subscriber.objects.raw(self.subscribers_matched)
        subscribers_users_ids = [subscriber.user_id for subscriber in
                                 subscribers]

        subscribers_sms = SubscriberSMS.objects.raw(
            self.subscribers_sms_matched)
        subscribers_sms_users_ids = [s.user_id for s in subscribers_sms]

        #in sub and sms
        matched_users_ids = list(set(subscribers_users_ids).intersection(
            subscribers_sms_users_ids))

        #update consents based on subscribers
        self._update_consents(subscribers, subscribers_users_ids, matched_users_ids)

        #update consents based on subscribers sms
        self._update_consents(subscribers, subscribers_sms_users_ids,
                              matched_users_ids)

        # update consents based on subscribers or subscribers sms
        # ___users = User.objects.filter(pk__in=matched_users). \
        #     annotate(consent=Case(*[
        #     When(id=s.user_id, create_date__lte=s.create_date, then=Value(
        #         s.gdpr_consent))
        #     for s in subscribers_sms], default=F('gdpr_consent'),
        #         output_field=models.BooleanField(),
        # ))

    def _update_consents(self, subscribers, users_ids, matched_users_ids):
        users = User.objects.filter(pk__in=users_ids).exclude(
            pk__in=matched_users_ids).annotate(consent=Case(*[
            When(id=s.user_id, create_date__lte=s.create_date,
                 then=Value(s.gdpr_consent)) for s in subscribers],
                                  default=F('gdpr_consent'),
                                  output_field=models.BooleanField()))
        users.update(gdpr_consent=F('consent'))
