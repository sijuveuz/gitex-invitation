from .base import *
import os

DEBUG = True
# ALLOWED_HOSTS = ['178.18.253.63']
ALLOWED_HOSTS = ['178.18.253.63', '178.18.253.63:8083', 'localhost', '127.0.0.1']
CSRF_TRUSTED_ORIGINS = ['http://178.18.253.63:8083', 'http://178.18.253.63']

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': config('DATABASE_NAME'),
        'USER': config('DATABASE_USER'),
        'PASSWORD': config('DATABASE_PASSWORD'),
        'HOST': config('DATABASE_HOST'),
        'PORT': config('DATABASE_PORT'),
    }
}



STATIC_URL = '/static/'
MEDIA_URL = '/media/'

REDIS_URL = "redis://127.0.0.1:6379/0"
 
#Celery configurations
CELERY_BROKER_URL = REDIS_URL
CELERY_RESULT_BACKEND = REDIS_URL
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'

CELERY_QUEUES = {
    "veuz3_queue": {
        "exchange": "veuz3_queue",
        "routing_key": "veuz3_queue",
    },
}

CELERY_TASK_DEFAULT_QUEUE = "veuz3_queue"


CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": REDIS_URL,
        "OPTIONS": {"CLIENT_CLASS": "django_redis.client.DefaultClient"},
    }
}


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,

    'formatters': {
        'verbose': {
            'format': '[{asctime}] {levelname} {name}: {message}',
            'style': '{',
        },
    },

    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': BASE_DIR / 'logs' / 'django.log',
            'formatter': 'verbose',
        },
        'celery_file': {
            'class': 'logging.FileHandler',
            'filename': BASE_DIR / 'logs' / 'celery.log',
            'formatter': 'verbose',
        },
        'send_bulk_invite_file': {  
            'class': 'logging.FileHandler',
            'filename': BASE_DIR / 'logs' / 'send_bulk_invite.log',
            'formatter': 'verbose',
        },
    },

    'loggers': {
        'django': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': True,
        },
        'celery': {
            'handlers': ['console', 'celery_file'],
            'level': 'INFO',
            'propagate': False,
        },
        'send_bulk_invite': {   
            'handlers': ['console', 'send_bulk_invite_file'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}
