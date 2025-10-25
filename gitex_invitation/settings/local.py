from .base import *
import os

DEBUG = True
ALLOWED_HOSTS = ['*']

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': config('DATABASE_NAME', default='gitex_local'),
        'USER': config('DATABASE_USER', default='postgres'),
        'PASSWORD': config('DATABASE_PASSWORD', default='postgres'),
        'HOST': config('DATABASE_HOST', default='127.0.0.1'),
        'PORT': config('DATABASE_PORT', default='5432'),
    }
}

REDIS_URL = "redis://127.0.0.1:6379/0"
 
#Celery configurations
CELERY_BROKER_URL = REDIS_URL
CELERY_RESULT_BACKEND = REDIS_URL
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'


CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": REDIS_URL,
        "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
    }
}


CELERY_QUEUES = {
    "veuz3_queue": {
        "exchange": "veuz3_queue",
        "routing_key": "veuz3_queue",
    },
}

CELERY_TASK_DEFAULT_QUEUE = "veuz3_queue"

# CELERYD_HIJACK_ROOT_LOGGER = False
# worker_hijack_root_logger = False


# LOGGING = {
#     'version': 1,
#     'disable_existing_loggers': False,

#     'formatters': {
#         'verbose': {
#             'format': '[{asctime}] {levelname} {name}: {message}',
#             'style': '{',
#         },
#     },

#     'handlers': {
#         'console': {
#             'class': 'logging.StreamHandler',
#             'formatter': 'verbose',
#         },
#         'file': {
#             'class': 'logging.FileHandler',
#             'filename': BASE_DIR / 'logs' / 'django.log',
#             'formatter': 'verbose',
#         },
#         'celery_file': {
#             'class': 'logging.FileHandler',
#             'filename': BASE_DIR / 'logs' / 'celery.log',
#             'formatter': 'verbose',
#         },
#         'send_bulk_invite_file': {  
#             'class': 'logging.FileHandler',
#             'filename': BASE_DIR / 'logs' / 'send_bulk_invite.log',
#             'formatter': 'verbose',
#         },
#     },

#     'loggers': {
#         'django': {
#             'handlers': ['console', 'file'],
#             'level': 'INFO',
#             'propagate': True,
#         },
#         'celery': {
#             'handlers': ['console', 'celery_file'],
#             'level': 'INFO',
#             'propagate': False,
#         },
#         'send_bulk_invite': {   # ✅ Now logs only to its own file
#             'handlers': ['console', 'send_bulk_invite_file'],
#             'level': 'INFO',
#             'propagate': False,
#         },
#     },
# }


