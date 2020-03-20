import csv
import tempfile
from django.apps import apps
from django.core.management.base import BaseCommand
from users.models import User, Subscriber, SubscriberSMS, Client


class Command(BaseCommand):
    help = 'Users import'

    users_based_on_clients_raw_sql = \
        "select users_client.phone, " \
        "users_client.id, users_client.email, " \
        "users_{model}.gdpr_consent as gdpr_consent, " \
        "count(*) over (partition by users_client.phone) as " \
        "number_of " \
        "from users_client " \
        "" \
        "inner join users_{model} " \
        "on users_client.{col1} = users_{model}.{col1} " \
        "" \
        "left join users_user " \
        "on users_{model}.{col1} = users_user.{col1} " \
        "" \
        "join users_client uc " \
        "on users_client.id = uc.id " \
        "" \
        "where not exists(" \
        "select 1 from users_user " \
        "where users_client.{col2} = users_user.{col2} " \
        "and users_client.{col1} != users_user.{col1}) " \
        "and users_user.{col1} is null order by users_{model}.id"

    subscribers_confilcts_raw_sql = \
        "select " \
        "users_{model}.{col1}, " \
        "users_{model}.id," \
        "users_{model}.gdpr_consent " \
        "from users_client " \
        "" \
        "inner join users_{model} " \
        "on users_client.{col1} = users_{model}.{col1} " \
        "" \
        "left join users_user " \
        "on users_{model}.{col1} = users_user.{col1} " \
        "" \
        "where exists(" \
        "select 1 from users_user " \
        "where users_client.{col2} = users_user.{col2} " \
        "and users_client.{col1} != users_user.{col1}) " \
        "and users_user.{col1} is null order by users_{model}.id"

    users_incomplete_raw_sql = \
        "select " \
        "users_{model}.{col1}, " \
        "users_{model}.id, " \
        "users_{model}.gdpr_consent " \
        "from users_client " \
        "right join users_{model} " \
        "on users_client.{col1} = users_{model}.{col1} " \
        "" \
        "left join users_user " \
        "on users_{model}.{col1} = users_user.{col1} " \
        "" \
        "where users_client.{col1} is null and users_user.{col1} is null"

    def handle(self, *args, **options):
        conditions = {
            'subscriber': {
                'model': 'subscriber',
                'col1': 'email',
                'col2': 'phone'
            },
            'subscribersms': {
                'model': 'subscribersms',
                'col1': 'phone',
                'col2': 'email'
            }
        }
        for k, v in conditions.items():
            self.create_users_based_on_clients(v)
            self.get_subscribers_conflicts(v)
            self.create_users_incomplete(v)
        self.stdout.write(self.style.SUCCESS('Finish'))

    def create_users_based_on_clients(self, conditions):
        clients = Client.objects.raw(self.users_based_on_clients_raw_sql.
                                     format(**conditions))
        users_bulk = [
            User(email=c.email, phone=c.phone, gdpr_consent=c.gdpr_consent)
            for c in clients if c.number_of == 1
        ]
        User.objects.bulk_create(users_bulk)
        self.get_conflicts_csv([c for c in clients if c.number_of > 1],
                               'client_conflicts', conditions['col1'])

    def get_subscribers_conflicts(self, conditions):
        model = apps.get_model('users', conditions['model'])
        subscribers = model.objects.raw(self.subscribers_confilcts_raw_sql.
            format(**conditions))
        self.get_conflicts_csv(subscribers, '{}_conflicts'.format(
            conditions['model']), conditions['col1'])

    def create_users_incomplete(self, conditions):
        model = apps.get_model('users', conditions['model'])
        subscribers = model.objects.raw(self.users_incomplete_raw_sql.format(
            **conditions))
        users_bulk = [
            User(email=getattr(s, 'email', None),
                 phone=getattr(s, 'phone', None),
                 gdpr_consent=s.gdpr_consent)for s in subscribers
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
