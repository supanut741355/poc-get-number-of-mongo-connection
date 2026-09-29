# start app
docker compose up -d --build app

# get connection 
curl localhost:300X/connections

# add load
curl "localhost:300X/load?n=100"

# observe
docker compose logs -f app

# destroy
docker compose down -v

curl -X POST localhost:300X/items -H 'Content-Type: application/json' -d '{"name":"apple","qty":3}'



