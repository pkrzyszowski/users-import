import csv
import tempfile

from django.core.management.base import BaseCommand
from users.models import User, Subscriber, SubscriberSMS, Client

class Command(BaseCommand):
    help = 'Users import'

    clients_raw_sl = "select users_client.phone, " \
                     "users_client.id, users_client.email, " \
                     "users_subscriber.gdpr_consent as gdpr_consent, " \
                     "count(*) over (partition by users_client.phone) as " \
                     "number_of " \
                     "from users_client " \
                     "inner join users_subscriber " \
                     "on users_client.email = users_subscriber.email " \
                     "" \
                     "left join users_user " \
                     "on users_subscriber.email = users_user.email " \
                     "" \
                     "join users_client uc " \
                     "on users_client.id = uc.id " \
                     "where not exists(select 1 from users_user " \
                     "where users_client.phone = users_user.phone " \
                     "and users_client.email != users_user.email) " \
                     "and users_user.email is null"

    subscribers_raw_sql = "select " \
                          "users_subscriber.email, " \
                          "users_subscriber.id," \
                          "users_subscriber.gdpr_consent " \
                          "from users_client " \
                          "" \
                          "inner join users_subscriber " \
                          "on users_client.email = users_subscriber.email " \
                          "" \
                          "left join users_user on users_subscriber.email = users_user.email " \
                          "" \
                          "where exists(" \
                          "select 1 from users_user where users_client.phone = users_user.phone " \
                          "and users_client.email != users_user.email) " \
                          "and users_user.email is null order by users_subscriber.id"

    empty_phone_raw_sql = "select " \
                          "users_subscriber.email, " \
                          "users_subscriber.id, " \
                          "users_subscriber.gdpr_consent " \
                          "from users_client " \
                          "right join users_subscriber " \
                          "on users_client.email = users_subscriber.email " \
                          "" \
                          "left join users_user " \
                          "on users_subscriber.email = users_user.email " \
                          "" \
                          "where users_client.email is null and users_user.email is null"

    def handle(self, *args, **options):
        self.users_from_clients()
        self.subscribers_conflicts_csv()
        self.users_empty_phone()

    def users_from_clients(self):
        clients = Client.objects.raw(self.clients_raw_sl)
        bulk = [User(email=c.email, phone=c.phone, gdpr_consent=c.gdpr_consent)
                for c in clients if c.number_of == 1]
        User.objects.bulk_create(bulk)

    def subscribers_conflicts_csv(self):
        subscribers = Subscriber.objects.raw(self.subscribers_raw_sql)
        filename = tempfile.NamedTemporaryFile(suffix='.csv',
                                               prefix='subscriber_conflicts',
                                               delete=False)
        with open(filename.name, 'w', encoding='utf8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['ID', 'Email'])
            writer.writerows([[row.id, row.email] for row in subscribers])

    def users_empty_phone(self):
        subscribers = Subscriber.objects.raw(self.empty_phone_raw_sql)
        bulk = [User(email=s.email, gdpr_consent=s.gdpr_consent)
                for s in subscribers]
        User.objects.bulk_create(bulk)
