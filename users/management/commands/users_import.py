import csv
import tempfile
from django.apps import apps
from django.core.management.base import BaseCommand
from django.db.models import Subquery, Q, OuterRef, Exists, Count

from users.models import User, Subscriber, SubscriberSMS, Client


class Command(BaseCommand):
    help = 'Users import'

    def handle(self, *args, **options):
        self.create_users_based_on_clients()
        self.create_users_based_on_clients_sms()
        self.get_subscribers_conflicts()
        self.get_subscribers_conflicts_sms()
        self.create_users_incomplete()
        self.stdout.write(self.style.SUCCESS('Finish'))

    def create_users_based_on_clients(self):
        users = User.objects.values('email')
        clients = Client.objects.annotate(gdpr_consent=Subquery(
            Subscriber.objects.filter(
                email=OuterRef('email')).exclude(email__in=users).values(
                'gdpr_consent')), usr=~Exists(
            User.objects.filter(~Q(email=OuterRef('email')),
                                phone=OuterRef('phone'))),
            phone_count=Count('phone')).filter(
            gdpr_consent__isnull=False, usr=True)
        self.create_users(clients, 'email')

    def create_users_based_on_clients_sms(self):
        users = User.objects.values('phone')
        clients = Client.objects.annotate(gdpr_consent=Subquery(
            SubscriberSMS.objects.filter(
                phone=OuterRef('phone')).exclude(phone__in=users).values(
                'gdpr_consent')), usr=~Exists(
            User.objects.filter(~Q(phone=OuterRef('phone')),
                                email=OuterRef('email'))),
            phone_count=Count('phone')).filter(
            gdpr_consent__isnull=False, usr=True)

        self.create_users(clients, 'phone')

    def create_users(self, clients, col_name):
        users_bulk = [
            User(email=c.email, phone=c.phone, gdpr_consent=c.gdpr_consent)
            for c in clients if c.phone_count == 1
        ]
        User.objects.bulk_create(users_bulk)
        self.get_conflicts_csv([c for c in clients if c.phone_count > 1],
                               'client_conflicts', col_name)

    def get_subscribers_conflicts(self):
        users = User.objects.values('email')
        clients = Client.objects.annotate(sub_id=Subquery(
            Subscriber.objects.filter(
                email=OuterRef('email')).exclude(email__in=users).values(
                'id')), usr=Exists(
            User.objects.filter(~Q(email=OuterRef('email')),
                                phone=OuterRef('phone')))).filter(
            sub_id__isnull=False, usr=True)

        subscribers = Subscriber.objects.filter(email__in=clients.values_list(
            'email', flat=True))
        self.get_conflicts_csv(subscribers, '{}_conflicts'.format(
            'subscriber'), 'email')

    def get_subscribers_conflicts_sms(self):
        users = User.objects.values('phone')
        clients = Client.objects.annotate(sub_id=Subquery(
            SubscriberSMS.objects.filter(
                phone=OuterRef('phone')).exclude(phone__in=users).values(
                'id')), usr=Exists(
            User.objects.filter(~Q(phone=OuterRef('phone')),
                                email=OuterRef('email')))).filter(
            sub_id__isnull=False, usr=True)

        subscribers_sms = SubscriberSMS.objects.filter(
            phone__in=clients.values_list(
            'phone', flat=True))
        self.get_conflicts_csv(subscribers_sms, '{}_conflicts'.format(
            'subscriber_sms'), 'phone')

    def create_users_incomplete(self):
        users = User.objects.values('email')
        clients = Client.objects.annotate(
            sub_id=Subquery(Subscriber.objects.filter(
                email=OuterRef('email')).exclude(
                email__in=users).values('id'))).filter(
            sub_id__isnull=True)
        subscribers = Subscriber.objects.filter(email__in=clients.values_list(
            'email', flat=True))

        users_bulk = [
            User(email=getattr(s, 'email', None),
                 gdpr_consent=s.gdpr_consent)for s in subscribers
        ]
        User.objects.bulk_create(users_bulk)

    def create_users_incomplete_sms(self):
        users = User.objects.values('phone')
        clients = Client.objects.annotate(
            sub_id=Subquery(SubscriberSMS.objects.filter(
                phone=OuterRef('phone')).exclude(
                phone__in=users).values('id'))).filter(
            sub_id__isnull=True)

        subscribers_sms = SubscriberSMS.objects.filter(
            phone__in=clients.values_list(
            'phone', flat=True))

        users_bulk = [
            User(phone=getattr(s, 'phone', None),
                 gdpr_consent=s.gdpr_consent)for s in subscribers_sms
        ]
        User.objects.bulk_create(users_bulk)

    @staticmethod
    def get_conflicts_csv(rows, name, col_name):
        filename = tempfile.NamedTemporaryFile(suffix='.csv',
                                               prefix=name,
                                               delete=False)
        with open(filename.name, 'w', encoding='utf8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['ID', col_name])
            writer.writerows(([row.id, getattr(row, col_name)] for row in rows))
