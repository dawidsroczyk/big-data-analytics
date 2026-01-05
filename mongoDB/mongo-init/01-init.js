db = db.getSiblingDB("air_quality");

// kolekcje
db.createCollection("gold_history");
db.createCollection("gold_latest");

// TTL na historii: trzyma ostatnie 48h
// UWAGA: pole event_ts_mongo musi być typu Date
db.gold_history.createIndex(
    { event_ts_mongo: 1 },
    { expireAfterSeconds: 172800 }
);

// pomocnicze indeksy pod query
db.gold_history.createIndex({ key: 1, event_ts_sec: -1 });
db.gold_latest.createIndex({ key: 1 }, { unique: true });

// opcjonalnie: pod geolokację (jeśli kiedyś zrobicie GeoJSON)
// db.gold_latest.createIndex({ location: "2dsphere" });
