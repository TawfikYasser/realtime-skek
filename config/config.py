config = {
    "openai": {
        "api_key": "sk-proj-j94LmhbDmmJ4rggseyUCT3BlbkFJgvKXB8PWIpLLwr6cr82Q"
    }
    ,
    "kafka": {
        "sasl.username": "HLX3O5IGPU6XWGPE",
        "sasl.password": "Xw+va/F1eLi68VabRL4FowphlFomfktSa5J+xGznSYuCgQs+BYgqQsA7YOWPDSZN",
        "bootstrap.servers": "pkc-w77k7w.centralus.azure.confluent.cloud:9092",
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'session.timeout.ms': 50000
    },
    "schema_registry": {
        "url": "SCHEMA_REGISTRY_URL",
        "basic.auth.user.info": "SR_API_KEY:SR_API_SECRET"
    }
}