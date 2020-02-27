from django.core.management.base import BaseCommand
from users.models import User, Subscriber, SubscriberSMS, Client

class Command(BaseCommand):
    help = 'Users import'

    def handle(self, *args, **options):
        clients_raw_sql = \
            "select " \
            "distinct on (users_client.phone) " \
            "users_client.id, " \
            "users_client.email, " \
            "users_subscriber.gdpr_consent as gdpr_consent " \
            "from users_client " \
            "" \
            "inner join users_subscriber " \
            "on users_client.email = users_subscriber.email " \
            "" \
            "left join users_user " \
            "on users_subscriber.email = users_user.email " \
            "" \
            "where not exists(" \
            "select 1 from users_user where users_client.phone = " \
            "users_user.phone and users_client.email != users_user.email) " \
            "and users_user.email is null;"

        subscribers_raw_sql = \
            "select " \
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
            "and users_user.email is null;"

        empty_phone_raw_sql = \
            "select " \
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
