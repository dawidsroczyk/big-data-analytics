# SUPERSET — Setup

When you initialize superset at first time you have to at fist build it:
```bash
docker compose build --no-cache
```

Then you can start container:
```bash
docker compose up
```

When you create container at first time then you don't have any user here and database register so you have to create them by fiollowing above commands:
```bash
docker exec -it superset superset fab create-admin   --username admin   --firstname Admin   --lastname User   --email admin@local   --password admin
docker exec -it superset superset db upgrade
docker exec -it superset superset init
```

Now, we creadet user: admin, with login: admin, and password to log in to superset: admin.

To connect to superset database to our database (Hive and MongoDB) go to ../trinto directory and run also :
```bash
docker compose up -d
```

Now, you are ready to build dashbords and analyse the data. 

