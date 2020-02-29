from django.apps import apps
from django.core.management.base import BaseCommand
from django.db import models
from django.db.models import Case, When, Value, F

from users.models import User, Subscriber, SubscriberSMS, Client


class Command(BaseCommand):
    help = 'Users consents update'

    users_subscribers_raw_sql = \
        "select users_{model}.{col}, " \
        "users_{model}.gdpr_consent, " \
        "users_{model}.id, " \
        "users_user.id as user_id " \
        "from users_{model} " \
        "inner join users_user " \
        "on users_{model}.{col} = users_user.{col};"

    def handle(self, *args, **options):
        self.update_users_consents()
        self.stdout.write(
            self.style.SUCCESS('Finish'))


    def update_users_consents(self):
        subscribers, subscribers_users_ids = \
            self.get_subscribers('subscriber', 'email')

        subscribers_sms, subscribers_sms_users_ids = \
            self.get_subscribers('subscribersms', 'phone')

        matched_users_ids = list(set(subscribers_users_ids).intersection(
            subscribers_sms_users_ids))

        #subscriber
        self._update_consents(subscribers, subscribers_users_ids,
                              matched_users_ids)
        # #subscriber sms
        self._update_consents(subscribers, subscribers_sms_users_ids,
                              matched_users_ids)

        #matched users
        self._update_matched_users_consents(subscribers, subscribers_sms,
                                            matched_users_ids)

    def get_subscribers(self, model, col):
        _model = apps.get_model('users', model)
        subscribers = _model.objects.raw(
            self.users_subscribers_raw_sql.format(model=model, col=col))
        subscribers_users_ids = [subscriber.user_id for subscriber in
                                 subscribers]
        return subscribers, subscribers_users_ids

    @staticmethod
    def _update_consents(subscribers, users_ids, matched_users_ids):
        users = User.objects.filter(pk__in=users_ids).exclude(
            pk__in=matched_users_ids).annotate(consent=Case(*[
            When(id=s.user_id, create_date__lte=s.create_date,
                 then=Value(s.gdpr_consent)) for s in subscribers
        ], default=F('gdpr_consent'), output_field=models.BooleanField()))
        users.update(gdpr_consent=F('consent'))

    @staticmethod
    def _update_matched_users_consents(subscribers, subscribers_sms,
                                       matched_users_ids):
        subscribers = sorted([s for s in subscribers if s.user_id in
                              matched_users_ids], key=lambda k:k.user_id)
        subscribers_sms = sorted([sms for sms in subscribers_sms if
                                  sms.user_id in matched_users_ids],
                                 key=lambda k:k.user_id)
        users = User.objects.filter(pk__in=matched_users_ids).order_by('id')
        zipped = zip(subscribers, subscribers_sms, users)
        max_dates = [max(obj, key=lambda k:k.create_date) for obj in zipped]

        users = User.objects.filter(pk__in=matched_users_ids).\
            annotate(consent=Case(*[
            When(id=getattr(obj, 'user_id', getattr(obj, 'id')),
                 then=Value(obj.gdpr_consent)) for obj in max_dates
        ], default=F('gdpr_consent'), output_field=models.BooleanField()))
        users.update(gdpr_consent=F('consent'))
