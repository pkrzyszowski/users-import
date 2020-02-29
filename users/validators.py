from django.core.validators import RegexValidator


class PhoneNumberRegexValidator(RegexValidator):
    message = "Phone number must be entered in the format: '+999999999'. Up to 15 digits allowed."
    regex = r'^\+?1?\d{9,15}$'
