FEATURE_FLAGS = {
    # Pozwala na używanie Jinja w zapytaniach SQL (przydatne!)
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Standardowe ustawienie dla SQLAlchemy
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Timeouty zostawiamy – zapytania przez Trino do Hive/Mongo 
# wciąż mogą potrzebować czasu na przetworzenie dużych zbiorów.
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

# Opcjonalnie: Jeśli masz problem z logowaniem, dodaj:
# SECRET_KEY = 'supersetsecretkey'